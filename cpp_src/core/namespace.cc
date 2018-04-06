#include "core/namespace.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include "core/index/index.h"
#include "core/nsdescriber/nsdescriber.h"
#include "core/nsselecter/nsselecter.h"
#include "itemimpl.h"
#include "storage/storagefactory.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/slice.h"
#include "tools/stringstools.h"
#include "tools/timetools.h"

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::make_shared;
using std::move;
using std::shared_ptr;
using std::stoi;
using std::thread;
using std::to_string;
using std::transform;

#define kStorageItemPrefix "I"
#define kStorageIndexesPrefix "indexes"
#define kStorageTagsPrefix "tags"
#define kStorageMetaPrefix "meta"

#define kStorageMagic 0x1234FEDC
#define kStorageVersion 0x6

namespace reindexer {

const int64_t kStorageSerialInitial = 1;

// private implementation and NOT THREADSAFE of copy CTOR
// use 'Namespace::Clone(Namespace& ns)'
Namespace::Namespace(const Namespace &src)
	: indexesNames_(src.indexesNames_),
	  items_(src.items_),
	  free_(src.free_),
	  name_(src.name_),
	  payloadType_(src.payloadType_),
	  tagsMatcher_(src.tagsMatcher_),
	  storage_(src.storage_),
	  updates_(src.updates_),
	  unflushedCount_(0),
	  sortOrdersBuilt_(false),
	  pkFields_(src.pkFields_),
	  meta_(src.meta_),
	  dbpath_(src.dbpath_),
	  queryCache_(src.queryCache_) {
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));
	logPrintf(LogTrace, "Namespace::Namespace (clone %s)", name_.c_str());
}

Namespace::Namespace(const string &name)
	: name_(name),
	  payloadType_(name),
	  tagsMatcher_(payloadType_),
	  unflushedCount_(0),
	  sortOrdersBuilt_(false),
	  queryCache_(make_shared<QueryCache>()) {
	logPrintf(LogTrace, "Namespace::Namespace (%s)", name_.c_str());
	items_.reserve(10000);

	// Add index and payload field for tuple of non indexed fields
	addIndex("-tuple", "", IndexStrStore, IndexOpts());
}

Namespace::~Namespace() {
	WLock wlock(mtx_);
	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items", name_.c_str(), items_.size());
}

void Namespace::recreateCompositeIndexes(int startIdx) {
	for (int i = startIdx; i < static_cast<int>(indexes_.size()); ++i) {
		std::unique_ptr<reindexer::Index> &index(indexes_[i]);
		if (isComposite(index->Type())) {
			index.reset(Index::New(index->Type(), index->Name(), index->Opts(), payloadType_, index->Fields()));
			for (IdType rowId = 0; rowId < static_cast<int>(items_.size()); ++rowId) {
				if (!items_[rowId].IsFree()) {
					indexes_[i]->Upsert(KeyRef(items_[rowId]), rowId);
				}
			}
		}
	}
}

void Namespace::updateItems(PayloadType oldPlType, const FieldsSet &changedFields, int deltaFields) {
	assert(oldPlType->NumFields() + deltaFields == payloadType_->NumFields());

	int compositeStartIdx = deltaFields >= 0 ? payloadType_.NumFields() : oldPlType.NumFields();

	if (deltaFields < 0) {
		recreateCompositeIndexes(compositeStartIdx);
	}

	for (auto &idx : indexes_) {
		idx->UpdatePayloadType(payloadType_);
	}

	KeyRefs krefs, skrefs;
	ItemImpl newItem(payloadType_, tagsMatcher_);
	newItem.Unsafe(true);
	for (size_t rowId = 0; rowId < items_.size(); rowId++) {
		if (items_[rowId].IsFree()) {
			continue;
		}
		PayloadValue &plCurr = items_[rowId];
		Payload oldValue(oldPlType, plCurr);
		ItemImpl oldItem(oldPlType, plCurr, tagsMatcher_);
		oldItem.Unsafe(true);
		newItem.FromCJSON(&oldItem);

		PayloadValue plNew = oldValue.CopyTo(payloadType_, deltaFields >= 0);
		Payload newValue(payloadType_, plNew);

		for (int fieldIdx = compositeStartIdx; fieldIdx < int(indexes_.size()); ++fieldIdx) {
			indexes_[fieldIdx]->Delete(KeyRef(plCurr), rowId);
		}

		for (auto fieldIdx : changedFields) {
			auto &index = *indexes_[fieldIdx];

			if ((fieldIdx == 0) || deltaFields <= 0) {
				oldValue.Get(fieldIdx, krefs);
				for (auto key : krefs) index.Delete(key, rowId);
				if (krefs.empty()) index.Delete(KeyRef(), rowId);
			}

			if ((fieldIdx == 0) || deltaFields >= 0) {
				if (fieldIdx == 0) {
					newItem.GetPayload().Get(fieldIdx, krefs);
				} else {
					newValue.Get(fieldIdx, krefs);
				}
				skrefs.resize(0);
				for (auto key : krefs) skrefs.push_back(index.Upsert(key, rowId));

				newValue.Set(fieldIdx, skrefs);
				if (skrefs.empty()) index.Upsert(KeyRef(), rowId);
			}
		}

		for (int fieldIdx = compositeStartIdx; fieldIdx < int(indexes_.size()); ++fieldIdx) {
			indexes_[fieldIdx]->Upsert(KeyRef(plNew), rowId);
		}

		plCurr = std::move(plNew);
	}
	markUpdated();
}

void Namespace::AddIndex(const string &index, const string &jsonPath, IndexType type, IndexOpts opts) {
	WLock wlock(mtx_);
	if (addIndex(index, jsonPath, type, opts)) {
		saveIndexesToStorage();
	}
}

bool Namespace::DropIndex(const string &index) {
	WLock wlock(mtx_);
	if (dropIndex(index)) {
		saveIndexesToStorage();
		return true;
	}
	return false;
}

bool Namespace::dropIndex(const string &index) {
	auto itIdxName = indexesNames_.find(index);
	if (itIdxName == indexesNames_.end()) {
		const char *errMsg = "Cannot remove index %s: doesn't exist";
		logPrintf(LogError, errMsg, index.c_str());
		throw Error(errParams, errMsg, index.c_str());
	}

	int fieldIdx = itIdxName->second;
	// Check, that index to remove is not part of composite index
	for (int compositeIdx = payloadType_.NumFields(); compositeIdx < int(indexes_.size()); ++compositeIdx) {
		if (indexes_[compositeIdx]->Fields().contains(fieldIdx))
			throw Error(LogError, "Cannot remove index %: it's a part of a composite index %s", index.c_str(),
						indexes_[compositeIdx]->Name().c_str());
	}
	for (auto &namePair : indexesNames_) {
		if (namePair.second >= fieldIdx) {
			namePair.second--;
		}
	}

	const unique_ptr<Index> &indexToRemove = indexes_[fieldIdx];
	if (indexToRemove->Opts().IsPK()) {
		if (indexToRemove->KeyType() == KeyValueComposite) {
			auto itCompositeIdxState = compositeIndexesPkState_.find(indexToRemove->Name());
			if (itCompositeIdxState == compositeIndexesPkState_.end()) {
				throw Error(LogError, "Composite index % is not PK", indexToRemove->Name().c_str());
			}
			bool eachSubIndexPk = itCompositeIdxState->second;
			if (eachSubIndexPk) {
				for (int idx : indexToRemove->Fields()) {
					auto opts = indexes_[idx]->Opts();
					indexes_[idx]->SetOpts(opts.PK(true));
				}
			}
		} else {
			pkFields_.erase(fieldIdx);
		}
	}

	FieldsSet newPKFields;
	for (auto idx : pkFields_) {
		newPKFields.push_back(idx < fieldIdx ? idx : idx - 1);
	}
	pkFields_ = std::move(newPKFields);

	if (!isComposite(indexToRemove->Type())) {
		PayloadType oldPlType = payloadType_;
		payloadType_.Drop(index);
		tagsMatcher_.updatePayloadType(payloadType_);
		FieldsSet changedFields{0, fieldIdx};
		updateItems(oldPlType, changedFields, -1);
	}

	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
	return true;
}

bool Namespace::addIndex(const string &index, const string &jsonPath, IndexType type, IndexOpts opts) {
	if (isComposite(type)) {
		// special handling of composite indexes;
		return AddCompositeIndex(index, type, opts);
	}

	auto idxNameIt = indexesNames_.find(index);
	int idxNo = payloadType_->NumFields();
	int fieldByJsonPath = payloadType_->FieldByJsonPath(jsonPath);

	// New index case. Just add
	if (idxNameIt == indexesNames_.end()) {
		// Check PK conflict
		if (opts.IsPK() && fieldByJsonPath != -1) {
			throw Error(errConflict, "Can't add PK Index '%s.%s', already exists another index with same json path", name_.c_str(),
						index.c_str());
		}

		insertIndex(Index::New(type, index, opts, PayloadType(), FieldsSet()), idxNo, index);

		PayloadType oldPlType = payloadType_;

		payloadType_.Add(PayloadFieldType(indexes_[idxNo]->KeyType(), index, jsonPath, opts.IsArray()));
		tagsMatcher_.updatePayloadType(payloadType_);
		FieldsSet changedFields{0, idxNo};
		updateItems(oldPlType, changedFields, 1);

		// Notify caller what indexes was changed
		return true;
	}

	idxNo = idxNameIt->second;
	// Existing index

	// payloadType must be in sync with indexes
	assert(idxNo < payloadType_->NumFields());

	// Check conflicts.
	if (indexes_[idxNameIt->second]->Type() != type) {
		throw Error(errConflict, "Index '%s.%s' already exists with different type", name_.c_str(), index.c_str());
	}

	if (pkFields_.contains(idxNo) != bool(opts.IsPK())) {
		throw Error(errConflict, "Index '%s.%s' already exists with different PK attribute", name_.c_str(), index.c_str());
	}

	// append different indexes
	if (fieldByJsonPath != idxNo && jsonPath.length()) {
		if (!opts.IsAppendable()) {
			throw Error(errConflict, "Index '%s.%s' already exists with different json path '%s' ", name_.c_str(), index.c_str(),
						jsonPath.c_str());
		}
		auto opts1 = indexes_[idxNo]->Opts();
		indexes_[idxNo]->SetOpts(opts1.Array(true));

		PayloadType oldPlType = payloadType_;
		payloadType_.Add(PayloadFieldType(indexes_[idxNo]->KeyType(), index, jsonPath, opts.IsArray()));
		tagsMatcher_.updatePayloadType(payloadType_);
		FieldsSet changedFields{0, idxNo};
		updateItems(oldPlType, changedFields, 0);
		return true;
	}

	return false;
}

bool Namespace::AddCompositeIndex(const string &index, IndexType type, IndexOpts opts) {
	string idxContent = index;
	string realName = index;
	auto pos = idxContent.find_first_of("=");
	if (pos != string::npos) {
		realName = idxContent.substr(pos + 1);
		idxContent = idxContent.substr(0, pos);
	}

	FieldsSet fields;

	bool eachSubIdxPK = true;
	vector<string> subIdxs;
	for (auto subIdx : split(idxContent, "+", true, subIdxs)) {
		auto idxNameIt = indexesNames_.find(subIdx);
		if (idxNameIt == indexesNames_.end()) {
			throw Error(errParams, "Subindex '%s' not found for composite index '%s'", subIdx.c_str(), index.c_str());
		}

		if (indexes_[idxNameIt->second]->Opts().IsArray() && (type == IndexCompositeBTree || type == IndexCompositeHash)) {
			throw Error(errParams, "Can't add array subindex '%s' to composite index '%s'", subIdx.c_str(), index.c_str());
		}

		fields.push_back(idxNameIt->second);
		if (!indexes_[idxNameIt->second]->Opts().IsPK()) {
			eachSubIdxPK = false;
		}
	}

	bool result = false;

	auto idxNameIt = indexesNames_.find(realName);
	if (idxNameIt == indexesNames_.end()) {
		if (eachSubIdxPK) {
			logPrintf(LogInfo, "Using composite index instead of single %s", index.c_str());
			for (auto i : fields) {
				auto opts = indexes_[i]->Opts();
				indexes_[i]->SetOpts(opts.PK(false));
			}
		}

		bool isPk(opts.IsPK() || eachSubIdxPK);
		opts.PK(isPk);
		if (isPk) {
			compositeIndexesPkState_.emplace(realName, eachSubIdxPK);
		}

		int idxPos = indexes_.size();
		insertIndex(Index::New(type, index, opts, payloadType_, fields), idxPos, realName);

		for (IdType rowId = 0; rowId < int(items_.size()); rowId++) {
			if (!items_[rowId].IsFree()) {
				indexes_[idxPos]->Upsert(KeyRef(items_[rowId]), rowId);
			}
		}

		result = true;
	} else {
		// Existing index
		if (indexes_[idxNameIt->second]->Type() != type) {
			throw Error(errConflict, "Index '%s' already exists with different type", index.c_str());
		}

		if (indexes_[idxNameIt->second]->Fields() != fields) {
			throw Error(errConflict, "Index '%s' already exists with different fields", index.c_str());
		}
	}

	return result;
}

void Namespace::insertIndex(Index *newIndex, int idxNo, const string &realName) {
	if (newIndex->Opts().IsPK() && newIndex->Opts().IsArray()) {
		throw Error(errParams, "Can't add index '%s' in namespace '%s'. PK field can't be array", newIndex->Name().c_str(), name_.c_str());
	}

	indexes_.insert(indexes_.begin() + idxNo, unique_ptr<Index>(newIndex));

	for (auto &n : indexesNames_) {
		if (n.second >= idxNo) {
			n.second++;
		}
	}

	indexesNames_.insert({realName, idxNo});

	if (newIndex->Opts().IsPK()) {
		if (newIndex->KeyType() == KeyValueComposite) {
			for (auto i : newIndex->Fields()) pkFields_.push_back(i);
		} else {
			pkFields_.push_back(idxNo);
		}
	}
}

int Namespace::getIndexByName(const string &index) const {
	auto idxIt = indexesNames_.find(index);

	if (idxIt == indexesNames_.end()) throw Error(errParams, "Index '%s' not found in '%s'", index.c_str(), name_.c_str());

	return idxIt->second;
}
void Namespace::ConfigureIndex(const string &index, const string &config) { indexes_[getIndexByName(index)]->Configure(config); }

void Namespace::Insert(Item &item, bool store) { upsertInternal(item, store, INSERT_MODE); }

void Namespace::Update(Item &item, bool store) { upsertInternal(item, store, UPDATE_MODE); }

void Namespace::Upsert(Item &item, bool store) { upsertInternal(item, store, INSERT_MODE | UPDATE_MODE); }

void Namespace::Delete(Item &item) {
	ItemImpl *ritem = item.impl_;
	string jsonSliceBuf;

	WLock lock(mtx_);

	updateTagsMatcherFromItem(ritem, jsonSliceBuf);

	auto itItem = findByPK(ritem);
	IdType id = itItem.first;

	if (!itItem.second) {
		return;
	}

	item.setID(id, items_[id].GetVersion());
	_delete(id);
}

void Namespace::_delete(IdType id) {
	assert(items_.exists(id));

	Payload pl(payloadType_, items_[id]);

	if (storage_) {
		auto pk = pl.GetPK(pkFields_);
		updates_->Remove(Slice(string(kStorageItemPrefix) + pk));
		++unflushedCount_;
	}

	// erase last item
	KeyRefs skrefs;
	int field;
	// erase from composite indexes
	for (field = pl.NumFields(); field < int(indexes_.size()); ++field) indexes_[field]->Delete(KeyRef(items_[id]), id);

	for (field = 0; field < pl.NumFields(); ++field) {
		auto &index = *indexes_[field];
		pl.Get(field, skrefs);
		// Delete value from index
		for (auto key : skrefs) index.Delete(key, id);
		// If no krefs delete empty value from index
		if (!skrefs.size()) index.Delete(KeyRef(), id);
	}

	// free PayloadValue
	items_[id].Free();
	markUpdated();
	free_.emplace(id);
}

void Namespace::Delete(const Query &q, QueryResults &result) {
	WLock lock(mtx_);
	NsSelecter selecter(this);
	SelectCtx ctx(q, nullptr);
	selecter(result, ctx);

	auto tmStart = high_resolution_clock::now();
	for (auto r : result) _delete(r.id);

	if (q.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", result.size(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void Namespace::upsert(ItemImpl *ritem, IdType id, bool doUpdate) {
	// Upsert fields to indexes
	assert(items_.exists(id));
	auto &plData = items_[id];

	// Inplace payload
	Payload pl(payloadType_, plData);
	Payload plNew = ritem->GetPayload();
	if (doUpdate) {
		plData.AllocOrClone(pl.RealSize());
	}
	markUpdated();

	KeyRefs krefs, skrefs;

	// Delete from composite indexes first
	int field = 0;
	for (field = plNew.NumFields(); field < int(indexes_.size()); ++field)
		if (doUpdate) indexes_[field]->Delete(KeyRef(plData), id);

	// Upsert fields to regular indexes
	for (field = 0; field < plNew.NumFields(); ++field) {
		auto &index = *indexes_[field];

		plNew.Get(field, skrefs);

		if (index.Opts().GetCollateMode() == CollateUTF8)
			for (auto &key : skrefs) key.EnsureUTF8();

		// Check for update
		if (doUpdate) {
			pl.Get(field, krefs);
			for (auto key : krefs) index.Delete(key, id);
			if (!krefs.size()) index.Delete(KeyRef(), id);
		}
		// Put value to index
		krefs.resize(0);
		for (auto key : skrefs) krefs.push_back(index.Upsert(key, id));

		// Put value to payload
		pl.Set(field, krefs);
		// If no krefs upsert empty value to index
		if (!skrefs.size()) index.Upsert(KeyRef(), id);
	}
	// Upsert to composite indexes
	for (; field < int(indexes_.size()); ++field) indexes_[field]->Upsert(KeyRef(plData), id);
}

void Namespace::updateTagsMatcherFromItem(ItemImpl *ritem, string &jsonSliceBuf) {
	if (ritem->tagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name_.c_str(), ritem->tagsMatcher().dump().c_str());
	}

	if (ritem->Type().get() != payloadType_.get() || !tagsMatcher_.try_merge(ritem->tagsMatcher())) {
		jsonSliceBuf = ritem->GetJSON().ToString();
		logPrintf(LogInfo, "Conflict TagsMatcher of namespace '%s' on modify: item:\n%s\ntm is\nnew tm is\n %s\n", name_.c_str(),
				  jsonSliceBuf.c_str(), tagsMatcher_.dump().c_str(), ritem->tagsMatcher().dump().c_str());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Unsafe(true);

		auto err = tmpItem.FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) throw err;

		*ritem = std::move(tmpItem);

		if (!tagsMatcher_.try_merge(ritem->tagsMatcher())) throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
}

void Namespace::upsertInternal(Item &item, bool store, uint8_t mode) {
	// Item to upsert
	ItemImpl *itemImpl = item.impl_;
	string jsonSlice;

	WLock lock(mtx_);

	updateTagsMatcherFromItem(itemImpl, jsonSlice);

	auto realItem = findByPK(itemImpl);
	IdType id = realItem.first;
	bool exists = realItem.second;
	auto newValue = itemImpl->GetPayload();

	switch (mode) {
		case INSERT_MODE:
			if (!exists) {
				// Check if payload not created yet, create it
				id = createItem(newValue.RealSize());
			} else {
				item.setID(-1, -1);
				return;
			}
			break;

		case UPDATE_MODE:
			if (!exists) {
				item.setID(-1, -1);
				return;
			}
			break;

		case (INSERT_MODE | UPDATE_MODE):
			if (!exists) id = createItem(newValue.RealSize());
			break;

		default:
			throw Error(errLogic, "Unknown write mode");
	}

	setFieldsBasedOnPrecepts(itemImpl);

	upsert(itemImpl, id, exists);
	item.setID(id, items_[id].GetVersion());

	if (storage_ && store) {
		char pk[512];
		auto prefLen = strlen(kStorageItemPrefix);
		memcpy(pk, kStorageItemPrefix, prefLen);
		newValue.GetPK(pk + prefLen, sizeof(pk) - prefLen, pkFields_);
		Slice b = itemImpl->GetCJSON();
		updates_->Put(Slice(pk), b);
		++unflushedCount_;
	}
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> Namespace::findByPK(ItemImpl *ritem) {
	h_vector<IdSetRef, 4> ids;
	h_vector<IdSetRef::iterator, 4> idsIt;

	if (!pkFields_.size()) {
		throw Error(errLogic, "Trying to modify namespace '%s', but it's have no PK indexes", name_.c_str());
	}

	Payload pl = ritem->GetPayload();
	// It's faster equalent of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	for (int field = 0; field < int(indexes_.size()); ++field)
		if (indexes_[field]->Opts().IsPK()) {
			KeyRefs krefs;
			if (field < pl.NumFields())
				pl.Get(field, krefs);
			else
				krefs.push_back(KeyRef(*pl.Value()));
			assertf(krefs.size() == 1, "Pkey field must contain 1 key, but there '%d' in '%s.%s'", int(krefs.size()), name_.c_str(),
					payloadType_->Field(field).Name().c_str());
			ids.push_back(indexes_[field]->Find(krefs[0]));
		}
	// Sort idsets. Intersect works faster on sets sorted by size
	std::sort(ids.begin(), ids.end(), [](const IdSetRef &k1, const IdSetRef &k2) { return k1.size() < k2.size(); });
	// Fast intersect found idsets
	if (ids.size() && ids[0].size()) {
		// Setup iterators to begin
		for (size_t i = 0; i < ids.size(); i++) idsIt.push_back(ids[i].begin());
		// Main loop by 1st result
		for (auto cur = ids[0].begin(); cur != ids[0].end(); cur++) {
			// Check if id exists in all other results
			unsigned j;
			for (j = 1; j < ids.size(); j++) {
				idsIt[j] = std::lower_bound(idsIt[j], ids[j].end(), *cur);
				if (idsIt[j] == ids[j].end()) {
					return {-1, false};
				} else if (*idsIt[j] != *cur)
					break;
			}
			if (j == ids.size()) {
				assert(items_.exists(*cur));
				return {*cur, true};
			}
		}
	}
	return {-1, false};
}

void Namespace::commit(const NSCommitContext &ctx, SelectLockUpgrader *lockUpgrader) {
	bool needCommit = (!sortOrdersBuilt_ && (ctx.phases() & CommitContext::MakeSortOrders));

	if (ctx.indexes())
		for (auto idxNo : *ctx.indexes()) needCommit = needCommit || !preparedIndexes_.contains(idxNo) || !commitedIndexes_.contains(idxNo);

	if (!needCommit) {
		return;
	}

	if (lockUpgrader) lockUpgrader->Upgrade();

	// Commit changes
	if ((ctx.phases() & CommitContext::MakeIdsets) && !commitedIndexes_.containsAll(indexes_.size())) {
		int field = payloadType_->NumFields();
		bool was = false;
		do {
			field %= indexes_.size();
			if (!ctx.indexes() || ctx.indexes()->contains(field) || (ctx.phases() & CommitContext::MakeSortOrders)) {
				if (!commitedIndexes_.contains(field)) {
					indexes_[field]->Commit(ctx);
					commitedIndexes_.push_back(field);
					was = true;
				}
			}
		} while (++field != payloadType_->NumFields());
		if (was) logPrintf(LogTrace, "Namespace::Commit ('%s'),%d items", name_.c_str(), items_.size());
		//	items_.shrink_to_fit();
	}

	if (!sortOrdersBuilt_ && (ctx.phases() & CommitContext::MakeSortOrders)) {
		// Update sort orders and sort_id for each index

		int i = 1;
		for (auto &idxIt : indexes_) {
			if (idxIt->IsOrdered()) {
				NSUpdateSortedContext sortCtx(*this, i++);
				idxIt->MakeSortOrders(sortCtx);
				// Build in multiple threads
				int maxIndexWorkers = std::thread::hardware_concurrency();
				unique_ptr<thread[]> thrs(new thread[maxIndexWorkers]);
				auto indexes = &this->indexes_;

				for (int i = 0; i < maxIndexWorkers; i++) {
					thrs[i] = std::thread(
						[&](int i) {
							for (int j = i; j < int(indexes->size()); j += maxIndexWorkers) indexes->at(j)->UpdateSortedIds(sortCtx);
						},
						i);
				}
				for (int i = 0; i < maxIndexWorkers; i++) thrs[i].join();
			}
		}
		sortOrdersBuilt_ = true;
	}

	if (ctx.indexes()) {
		NSCommitContext ctx1(*this, CommitContext::PrepareForSelect, ctx.indexes());
		for (auto idxNo : *ctx.indexes())
			if (!preparedIndexes_.contains(idxNo)) {
				assert(idxNo < indexes_.size());
				indexes_[idxNo]->Commit(ctx1);
				preparedIndexes_.push_back(idxNo);
			}
	}
}

void Namespace::markUpdated() {
	sortOrdersBuilt_ = false;
	preparedIndexes_.clear();
	commitedIndexes_.clear();
	invalidateQueryCache();
}

void Namespace::Select(QueryResults &result, SelectCtx &params) {
	NsSelecter selecter(this);
	selecter(result, params);
}

void Namespace::Describe(QueryResults &result) {
	NsDescriber describer(this);
	describer(result);
}

NamespaceDef Namespace::GetDefinition() {
	RLock rlock(mtx_);
	auto pt = this->payloadType_;

	NamespaceDef nsDef(name_, StorageOpts().Enabled(!dbpath_.empty()));

	for (size_t idx = 1; idx < indexes_.size(); idx++) {
		IndexDef indexDef;
		indexDef.FromType(indexes_[idx]->Type());
		indexDef.name = indexes_[idx]->Name();
		indexDef.opts = indexes_[idx]->Opts();

		if (int(idx) < pt->NumFields()) {
			for (auto &p : pt->Field(idx).JsonPaths()) indexDef.jsonPath += p + ",";
			if (!indexDef.jsonPath.empty()) indexDef.jsonPath.pop_back();
		}

		nsDef.AddIndex(indexDef);
	}
	return nsDef;
}

bool Namespace::loadIndexesFromStorage() {
	// Check is indexes structure already prepared.
	assert(indexes_.size() == 1);
	assert(items_.size() == 0);

	string def;
	auto status = storage_->Read(StorageOpts().FillCache(), Slice(kStorageIndexesPrefix), def);
	if (status.ok() && def.size()) {
		Serializer ser(def.data(), def.size());

		uint32_t dbMagic = ser.GetUInt32();
		if (dbMagic != kStorageMagic) {
			logPrintf(LogError, "Storage magic mismatch. want %08X, got %08X", kStorageMagic, dbMagic);
			return false;
		}

		uint32_t dbVer = ser.GetUInt32();
		if (dbVer != kStorageVersion) {
			logPrintf(LogError, "Storage version mismatch. want %08X, got %08X", kStorageVersion, dbVer);
			return false;
		}

		int cnt = ser.GetVarUint();
		while (cnt--) {
			string name = ser.GetVString().ToString();
			int count = ser.GetVarUint();
			vector<string> jsonPaths(std::max(count, 1), "");
			for (int i = 0; i < count; i++) {
				jsonPaths[i] = ser.GetVString().ToString();
			}

			IndexType type = IndexType(ser.GetVarUint());
			IndexOpts opts;
			opts.Array(ser.GetVarUint());
			opts.PK(ser.GetVarUint());
			opts.Dense(ser.GetVarUint());
			opts.Appendable(ser.GetVarUint());
			opts.SetCollateMode(static_cast<CollateMode>(ser.GetVarUint()));
			for (auto &jsonPath : jsonPaths) {
				addIndex(name, jsonPath, type, opts);
			}
		}
	}

	logPrintf(LogTrace, "Loaded index structure of namespace '%s'\n%s", name_.c_str(), payloadType_->ToString().c_str());

	def.clear();
	status = storage_->Read(StorageOpts().FillCache(), Slice(kStorageTagsPrefix), def);
	if (status.ok() && def.size()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logPrintf(LogTrace, "Loaded tags of namespace %s:\n%s", name_.c_str(), tagsMatcher_.dump().c_str());
	}

	return true;
}

void Namespace::saveIndexesToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveIndexesToStorage (%s)", name_.c_str());

	WrSerializer ser;
	ser.PutUInt32(kStorageMagic);
	ser.PutUInt32(kStorageVersion);

	ser.PutVarUint(indexes_.size() - 1);
	for (int f = 1; f < int(indexes_.size()); f++) {
		ser.PutVString(indexes_[f]->Name());

		if (f < payloadType_->NumFields()) {
			ser.PutVarUint(payloadType_->Field(f).JsonPaths().size());
			for (auto &jsonPath : payloadType_->Field(f).JsonPaths()) ser.PutVString(jsonPath);
		} else {
			ser.PutVarUint(0);
		}

		ser.PutVarUint(indexes_[f]->Type());
		ser.PutVarUint(indexes_[f]->Opts().IsArray());
		ser.PutVarUint(indexes_[f]->Opts().IsPK());
		ser.PutVarUint(indexes_[f]->Opts().IsDense());
		ser.PutVarUint(indexes_[f]->Opts().IsAppendable());
		ser.PutVarUint(indexes_[f]->Opts().GetCollateMode());
	}

	storage_->Write(StorageOpts().FillCache(), Slice(kStorageIndexesPrefix), Slice(reinterpret_cast<const char *>(ser.Buf()), ser.Len()));
}

void Namespace::EnableStorage(const string &path, StorageOpts opts) {
	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", name_.c_str(), path.c_str());
	}

	string dbpath = JoinPath(path, name_);
	datastorage::StorageType storageType = datastorage::StorageType::LevelDB;

	WLock lock(mtx_);
	storage_.reset(datastorage::StorageFactory::create(storageType));

	bool success = false;
	while (!success) {
		Error status = storage_->Open(dbpath, opts);
		if (!status.ok()) {
			if (!opts.IsDropOnFileFormatError()) {
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s' - %s", name_.c_str(), path.c_str(),
							status.what().c_str());
			}
		} else {
			success = loadIndexesFromStorage();
			if (!success && !opts.IsDropOnFileFormatError()) {
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s': format error", name_.c_str(), dbpath.c_str());
			}
		}
		if (!success && opts.IsDropOnFileFormatError()) {
			opts.DropOnFileFormatError(false);
			storage_->Destroy(path);
		}
	}

	updates_.reset(storage_->GetUpdatesCollection());
	dbpath_ = dbpath;
}

void Namespace::LoadFromStorage() {
	WLock lock(mtx_);

	StorageOpts opts;
	opts.FillCache(false);
	size_t ldcount = 0;

	logPrintf(LogTrace, "Loading items to '%s' from storage", name_.c_str());
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	ItemImpl item(payloadType_, tagsMatcher_);
	item.Unsafe(true);
	for (dbIter->Seek(kStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), Slice(kStorageItemPrefix "\xFF")) < 0; dbIter->Next()) {
		Slice dataSlice = dbIter->Value();
		if (dataSlice.size() > 0) {
			auto err = item.FromCJSON(dataSlice);
			if (!err.ok()) {
				logPrintf(LogError, "Error load item to '%s' from storage: '%s'", name_.c_str(), err.what().c_str());
				throw err;
			}

			IdType id = items_.size();
			items_.emplace_back(PayloadValue(item.GetPayload().RealSize()));
			upsert(&item, id, false);

			ldcount += dataSlice.size();
		}
	}
	logPrintf(LogInfo, "[%s] Done loading storage. %d items loaded, total size=%dM", name_.c_str(), int(items_.size()),
			  int(ldcount / (1024 * 1024)));
}

void Namespace::FlushStorage() {
	WLock wlock(mtx_);

	if (storage_) {
		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			tagsMatcher_.serialize(ser);
			updates_->Put(Slice(kStorageTagsPrefix), Slice(reinterpret_cast<const char *>(ser.Buf()), ser.Len()));
			unflushedCount_++;
			tagsMatcher_.clearUpdated();
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_.c_str(), tagsMatcher_.dump().c_str());
		}

		if (unflushedCount_) {
			Error status = storage_->Write(StorageOpts().FillCache(), *(updates_.get()));
			if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage:", name_.c_str(), status.what().c_str());
			updates_->Clear();
			unflushedCount_ = 0;
		}
	}
}

void Namespace::DeleteStorage() {
	WLock lck(mtx_);
	if (!dbpath_.empty()) {
		storage_->Destroy(dbpath_.c_str());
		storage_.reset();
	}
}

Item Namespace::NewItem() {
	RLock lock(mtx_);
	return Item(new ItemImpl(payloadType_, tagsMatcher_));
}

// Get meta data from storage by key
string Namespace::GetMeta(const string &key) {
	RLock lock(mtx_);
	return getMeta(key);
}

string Namespace::getMeta(const string &key) {
	auto it = meta_.find(key);
	if (it != meta_.end()) {
		return it->second;
	}

	if (storage_) {
		string data;
		Error status = storage_->Read(StorageOpts().FillCache(), Slice(kStorageMetaPrefix + key), data);
		if (status.ok()) {
			return data;
		}
	}

	return "";
}

// Put meta data to storage by key
void Namespace::PutMeta(const string &key, const Slice &data) {
	WLock lock(mtx_);
	putMeta(key, data);
}

// Put meta data to storage by key
void Namespace::putMeta(const string &key, const Slice &data) {
	meta_[key] = data.ToString();

	if (storage_) {
		storage_->Write(StorageOpts().FillCache(), Slice(kStorageMetaPrefix + key), Slice(data.data(), data.size()));
	}
}

vector<string> Namespace::EnumMeta() {
	vector<string> ret;

	RLock lck(mtx_);
	for (auto &m : meta_) {
		ret.push_back(m.first);
	}

	StorageOpts opts;
	opts.FillCache(false);
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	size_t prefixLen = strlen(kStorageMetaPrefix);

	for (dbIter->Seek(Slice(kStorageMetaPrefix));
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), Slice(kStorageMetaPrefix "\xFF")) < 0; dbIter->Next()) {
		Slice keySlice = dbIter->Key();
		if (keySlice.size() > prefixLen) {
			auto key = keySlice.ToString().substr(prefixLen);
			if (meta_.find(key) == meta_.end()) {
				ret.push_back(key);
			}
		}
	}
	return ret;
}

Namespace *Namespace::Clone(Namespace::Ptr ns) {
	RLock lock(ns->mtx_);
	return new Namespace(*ns);
}

int Namespace::getSortedIdxCount() const {
	int cnt = 0;
	for (auto &it : indexes_)
		if (it->IsOrdered()) cnt++;
	return cnt;
}

IdType Namespace::createItem(size_t realSize) {
	IdType id = 0;
	if (free_.size()) {
		id = *free_.begin();
		free_.erase(free_.begin());
		assert(id < IdType(items_.size()));
		assert(items_[id].IsFree());
		items_[id] = PayloadValue(realSize);
	} else {
		id = items_.size();
		items_.emplace_back(PayloadValue(realSize));
	}
	return id;
}

void Namespace::invalidateQueryCache() {
	if (!queryCache_->Empty()) {
		logPrintf(LogTrace, "[*] invalidate query cache. namespace: %s\n", name_.c_str());
		queryCache_.reset(new QueryCache);
	}
}

void Namespace::setFieldsBasedOnPrecepts(ItemImpl *ritem) {
	for (auto &precept : ritem->GetPrecepts()) {
		SelectFuncParser sqlFunc;
		SelectFuncStruct sqlFuncStruct = sqlFunc.Parse(precept);

		KeyRefs krs;
		KeyValue field = ritem->GetPayload().Get(sqlFuncStruct.field, krs)[0];

		KeyValue value(sqlFuncStruct.value);

		if (sqlFuncStruct.isFunction) {
			if (sqlFuncStruct.funcName == "now") {
				string mode = "sec";
				if (sqlFuncStruct.funcArgs.size() && !sqlFuncStruct.funcArgs.front().empty()) {
					mode = sqlFuncStruct.funcArgs.front();
				}
				value = KeyValue(getTimeNow(mode));
			} else if (sqlFuncStruct.funcName == "serial") {
				value = KeyValue(funcGetSerial(sqlFuncStruct));
			} else {
				throw Error(errParams, "Unknown function %s", sqlFuncStruct.field.c_str());
			}
		}

		value.convert(field.Type());
		KeyRefs refs{value};

		ritem->GetPayload().Set(sqlFuncStruct.field, refs, false);
	}
}

int64_t Namespace::funcGetSerial(SelectFuncStruct sqlFuncStruct) {
	int64_t counter = kStorageSerialInitial;

	string ser = getMeta("_SERIAL_" + sqlFuncStruct.field);
	if (ser != "") {
		counter = stoi(ser) + 1;
	}

	string s = to_string(counter);
	putMeta("_SERIAL_" + sqlFuncStruct.field, Slice(s));

	return counter;
}

}  // namespace reindexer
