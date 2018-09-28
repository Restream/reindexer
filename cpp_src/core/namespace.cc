#include "core/namespace.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include "core/cjson/jsonencoder.h"
#include "core/index/index.h"
#include "core/nsselecter/nsselecter.h"
#include "itemimpl.h"
#include "storage/storagefactory.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"
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
#define kStorageCachePrefix "cache"

#define kStorageMagic 0x1234FEDC
#define kStorageVersion 0x7

namespace reindexer {

const int64_t kStorageSerialInitial = 1;

Namespace::IndexesStorage::IndexesStorage(const Namespace &ns) : Base(), ns_(ns) {}

// private implementation and NOT THREADSAFE of copy CTOR
// use 'Namespace::Clone(Namespace& ns)'
Namespace::Namespace(const Namespace &src)
	: indexes_(*this),
	  indexesNames_(src.indexesNames_),
	  items_(src.items_),
	  free_(src.free_),
	  name_(src.name_),
	  payloadType_(src.payloadType_),
	  tagsMatcher_(src.tagsMatcher_),
	  storage_(src.storage_),
	  updates_(src.updates_),
	  unflushedCount_(0),
	  sortOrdersBuilt_(false),
	  sortedQueriesCount_(0),
	  pkFields_(src.pkFields_),
	  meta_(src.meta_),
	  dbpath_(src.dbpath_),
	  queryCache_(src.queryCache_),
	  joinCache_(src.joinCache_),
	  cacheMode_(src.cacheMode_),
	  enablePerfCounters_(src.enablePerfCounters_.load()),
	  queriesLogLevel_(src.queriesLogLevel_) {
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));
	logPrintf(LogTrace, "Namespace::Namespace (clone %s)", name_.c_str());
}

Namespace::Namespace(const string &name, CacheMode cacheMode)
	: indexes_(*this),
	  name_(name),
	  payloadType_(name),
	  tagsMatcher_(payloadType_),
	  unflushedCount_(0),
	  sortOrdersBuilt_(false),
	  sortedQueriesCount_(0),
	  queryCache_(make_shared<QueryCache>()),
	  joinCache_(make_shared<JoinCache>()),
	  cacheMode_(cacheMode),
	  needPutCacheMode_(true),
	  enablePerfCounters_(false),
	  queriesLogLevel_(LogNone) {
	logPrintf(LogTrace, "Namespace::Namespace (%s)", name_.c_str());
	items_.reserve(10000);

	// Add index and payload field for tuple of non indexed fields
	IndexDef tupleIndexDef("-tuple", "", IndexStrStore, IndexOpts());
	addIndex(tupleIndexDef);
}

Namespace::~Namespace() {
	WLock wlock(mtx_);
	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items", name_.c_str(), int(items_.size()));
}

void Namespace::recreateCompositeIndexes(int startIdx, int endIdx) {
	for (int i = startIdx; i < endIdx; ++i) {
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
	logPrintf(LogTrace, "Namespace::updateItems(%s) delta=%d", name_.c_str(), deltaFields);

	assert(oldPlType->NumFields() + deltaFields == payloadType_->NumFields());

	int compositeStartIdx = 0;
	if (deltaFields >= 0) {
		compositeStartIdx = indexes_.firstCompositePos();
	} else {
		compositeStartIdx = indexes_.firstCompositePos(oldPlType, sparseIndexesCount_);
	}
	int compositeEndIdx = indexes_.totalSize();
	recreateCompositeIndexes(compositeStartIdx, compositeEndIdx);

	for (auto &idx : indexes_) {
		idx->UpdatePayloadType(payloadType_);
	}

	KeyRefs krefs, skrefs;
	ItemImpl newItem(payloadType_, tagsMatcher_);
	newItem.Unsafe(true);
	int errCount = 0;
	Error lastErr = errOK;
	for (size_t rowId = 0; rowId < items_.size(); rowId++) {
		if (items_[rowId].IsFree()) {
			continue;
		}
		PayloadValue &plCurr = items_[rowId];
		Payload oldValue(oldPlType, plCurr);
		ItemImpl oldItem(oldPlType, plCurr, tagsMatcher_);
		oldItem.Unsafe(true);
		auto err = newItem.FromCJSON(&oldItem);
		if (!err.ok()) {
			logPrintf(LogTrace, "Can't apply indexes: %s", err.what().c_str());
			errCount++;
			lastErr = err;
		}

		PayloadValue plNew = oldValue.CopyTo(payloadType_, deltaFields >= 0);
		Payload newValue(payloadType_, plNew);

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			indexes_[fieldIdx]->Delete(KeyRef(plCurr), rowId);
		}

		for (auto fieldIdx : changedFields) {
			auto &index = *indexes_[fieldIdx];
			if ((fieldIdx == 0) || deltaFields <= 0) {
				oldValue.Get(fieldIdx, skrefs);
				for (auto key : skrefs) index.Delete(key, rowId);
				if (skrefs.empty()) index.Delete(KeyRef(), rowId);
			}

			if ((fieldIdx == 0) || deltaFields >= 0) {
				newItem.GetPayload().Get(fieldIdx, skrefs);
				krefs.resize(0);
				for (auto key : skrefs) krefs.push_back(index.Upsert(key, rowId));

				newValue.Set(fieldIdx, krefs);
				if (krefs.empty()) index.Upsert(KeyRef(), rowId);
			}
		}

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			indexes_[fieldIdx]->Upsert(KeyRef(plNew), rowId);
		}

		plCurr = std::move(plNew);
	}
	markUpdated();
	if (errCount != 0) {
		logPrintf(LogError, "Can't update indexes of %d items in namespace %s: %s", errCount, name_.c_str(), lastErr.what().c_str());
	}
}

void Namespace::AddIndex(const IndexDef &indexDef) {
	WLock wlock(mtx_);

	addIndex(indexDef);
	saveIndexesToStorage();
}

void Namespace::UpdateIndex(const IndexDef &indexDef) {
	WLock wlock(mtx_);

	updateIndex(indexDef);
	saveIndexesToStorage();
}
static string stripIndexName(string name) {
	auto pos = name.find_first_of("=");
	if (pos != string::npos) {
		name = name.substr(pos + 1);
	}
	return name;
}

void Namespace::DropIndex(const string &index) {
	WLock wlock(mtx_);

	dropIndex(stripIndexName(index));
	saveIndexesToStorage();
}

void Namespace::dropIndex(const string &index) {
	auto itIdxName = indexesNames_.find(index);
	if (itIdxName == indexesNames_.end()) {
		const char *errMsg = "Cannot remove index %s: doesn't exist";
		logPrintf(LogError, errMsg, index.c_str());
		throw Error(errParams, errMsg, index.c_str());
	}

	int fieldIdx = itIdxName->second;
	if (indexes_[fieldIdx]->Opts().IsSparse()) --sparseIndexesCount_;

	// Check, that index to remove is not a part of composite index
	for (int i = indexes_.firstCompositePos(); i < indexes_.totalSize(); ++i) {
		if (indexes_[i]->Fields().contains(fieldIdx))
			throw Error(LogError, "Cannot remove index %s : it's a part of a composite index %s", index.c_str(),
						indexes_[i]->Name().c_str());
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
				throw Error(LogError, "Composite index %s is not PK", indexToRemove->Name().c_str());
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

	if (!isComposite(indexToRemove->Type()) && !indexToRemove->Opts().IsSparse()) {
		PayloadType oldPlType = payloadType_;
		payloadType_.Drop(index);
		tagsMatcher_.updatePayloadType(payloadType_);
		FieldsSet changedFields{0, fieldIdx};
		updateItems(oldPlType, changedFields, -1);
	}

	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
}

void Namespace::configureIndex(const string &name, const string &config) {
	int indexPos = getIndexByName(name);

	IndexOpts opts = indexes_[indexPos]->Opts();
	opts.config = config;
	indexes_[indexPos]->SetOpts(opts);

	saveIndexesToStorage();
}

void Namespace::addIndex(const IndexDef &indexDef) {
	if (isComposite(indexDef.Type())) {
		return AddCompositeIndex(indexDef);
	}

	string indexName = indexDef.name_;
	IndexType type = indexDef.Type();
	IndexOpts opts = indexDef.opts_;
	JsonPaths jsonPaths = indexDef.jsonPaths_;

	auto idxNameIt = indexesNames_.find(indexName);
	int idxNo = payloadType_->NumFields();

	if (idxNameIt != indexesNames_.end()) {
		IndexDef oldIndexDef = getIndexDefinition(indexName);
		if (oldIndexDef == indexDef) {
			return;
		} else {
			throw Error(errConflict, "Index '%s.%s' already exists", name_.c_str(), indexName.c_str());
		}
	}

	for (const string &jsonPath : jsonPaths) {
		int fieldByJsonPath = payloadType_->FieldByJsonPath(jsonPath);
		if (opts.IsPK() && fieldByJsonPath != -1) {
			throw Error(errConflict, "Can't add PK Index '%s.%s', already exists another index with same json path", name_.c_str(),
						indexName.c_str());
		}
	}

	auto newIndex = unique_ptr<Index>(Index::New(type, indexName, opts, PayloadType(), FieldsSet()));
	if (opts.IsSparse()) {
		FieldsSet fields;

		for (const string &jsonPath : jsonPaths) {
			bool updated = false;
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPath, updated);
			assert(tagsPath.size() > 0);

			fields.push_back(jsonPath);
			fields.push_back(tagsPath);
		}

		newIndex->SetFields(fields);
		newIndex->UpdatePayloadType(payloadType_);
		++sparseIndexesCount_;
		insertIndex(newIndex.release(), idxNo, indexName);
	} else {
		PayloadType oldPlType = payloadType_;

		for (const string &jsonPath : jsonPaths) {
			payloadType_.Add(PayloadFieldType(newIndex->KeyType(), indexName, jsonPath, opts.IsArray()));
		}
		tagsMatcher_.updatePayloadType(payloadType_);

		FieldsSet changedFields{0, idxNo};
		insertIndex(newIndex.release(), idxNo, indexName);
		updateItems(oldPlType, changedFields, 1);
	}
}

void Namespace::updateIndex(const IndexDef &indexDef) {
	string indexName = stripIndexName(indexDef.name_);

	IndexDef foundIndex = getIndexDefinition(indexName);

	IndexDef indexDefTst = indexDef;
	indexDefTst.opts_.config = "";
	IndexDef curIndexDefTst = foundIndex;
	curIndexDefTst.opts_.config = "";
	if (indexDefTst == curIndexDefTst) {
		if (indexDef.opts_.config != foundIndex.opts_.config) {
			configureIndex(foundIndex.name_, indexDef.opts_.config);
		}

		return;
	}

	dropIndex(indexName);
	addIndex(indexDef);
}

IndexDef Namespace::getIndexDefinition(const string &indexName) {
	NamespaceDef nsDef = getDefinition();

	auto indexes = nsDef.indexes;
	auto indexDefIt =
		std::find_if(indexes.begin(), indexes.end(), [&](const IndexDef &idxDef) { return stripIndexName(idxDef.name_) == indexName; });
	if (indexDefIt == indexes.end()) {
		throw Error(errParams, "Index '%s' not found in '%s'", indexName.c_str(), name_.c_str());
	}

	return *indexDefIt;
}

void Namespace::AddCompositeIndex(const IndexDef &indexDef) {
	string indexName = indexDef.name_;
	IndexType type = indexDef.Type();
	IndexOpts opts = indexDef.opts_;

	string idxContent = indexName;
	string realName = indexName;
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
			bool updated = false;
			TagsPath tagsPath = tagsMatcher_.path2tag(subIdx, updated);
			if (tagsPath.empty()) {
				throw Error(errParams, "Subindex '%s' for composite index '%s' does not exist", subIdx.c_str(), indexName.c_str());
			}
			fields.push_back(tagsPath);
			fields.push_back(subIdx);
			eachSubIdxPK = false;
		} else {
			if (indexes_[idxNameIt->second]->Opts().IsArray() && (type == IndexCompositeBTree || type == IndexCompositeHash)) {
				throw Error(errParams, "Can't add array subindex '%s' to composite index '%s'", subIdx.c_str(), indexName.c_str());
			}
			fields.push_back(idxNameIt->second);
			if (!indexes_[idxNameIt->second]->Opts().IsPK()) eachSubIdxPK = false;
		}
	}

	assert(fields.getJsonPathsLength() == fields.getTagsPathsLength());

	auto idxNameIt = indexesNames_.find(realName);
	if (idxNameIt == indexesNames_.end()) {
		if (eachSubIdxPK) {
			logPrintf(LogInfo, "Using composite index instead of single %s", indexName.c_str());
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
		insertIndex(Index::New(type, indexName, opts, payloadType_, fields), idxPos, realName);

		for (IdType rowId = 0; rowId < int(items_.size()); rowId++) {
			if (!items_[rowId].IsFree()) {
				indexes_[idxPos]->Upsert(KeyRef(items_[rowId]), rowId);
			}
		}
	} else {
		// Existing index
		if (indexes_[idxNameIt->second]->Type() != type) {
			throw Error(errConflict, "Index '%s' already exists with different type", indexName.c_str());
		}

		if (indexes_[idxNameIt->second]->Fields() != fields) {
			throw Error(errConflict, "Index '%s' already exists with different fields", indexName.c_str());
		}
	}
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

bool Namespace::getIndexByName(const string &name, int &index) const {
	auto it = indexesNames_.find(name);
	if (it == indexesNames_.end()) return false;
	index = it->second;
	return true;
}

void Namespace::ConfigureIndex(const string &index, const string &config) {
	WLock lock(mtx_);

	configureIndex(index, config);
}

void Namespace::Insert(Item &item, bool store) { upsertInternal(item, store, INSERT_MODE); }

void Namespace::Update(Item &item, bool store) { upsertInternal(item, store, UPDATE_MODE); }

void Namespace::Upsert(Item &item, bool store) { upsertInternal(item, store, INSERT_MODE | UPDATE_MODE); }

void Namespace::Delete(Item &item) {
	ItemImpl *ritem = item.impl_;
	string jsonSliceBuf;

	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	WLock lock(mtx_);
	calc.LockHit();

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
		updates_->Remove(string_view(string(kStorageItemPrefix) + pk));
		++unflushedCount_;
	}

	// erase last item
	KeyRefs skrefs;
	int field;

	// erase from composite indexes
	for (field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Delete(KeyRef(items_[id]), id);
	}

	for (field = 0; field < indexes_.firstCompositePos(); ++field) {
		Index &index = *indexes_[field];
		if (index.Opts().IsSparse()) {
			assert(index.Fields().getTagsPathsLength() > 0);
			pl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else {
			pl.Get(field, skrefs);
		}
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
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	WLock lock(mtx_);
	calc.LockHit();

	NsSelecter selecter(this);
	SelectCtx ctx(q, nullptr);
	selecter(result, ctx);

	auto tmStart = high_resolution_clock::now();
	for (auto r : result.Items()) _delete(r.id);

	if (q.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", int(result.Count()),
				  int(duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count()));
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
	if (doUpdate) {
		for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
			indexes_[field]->Delete(KeyRef(plData), id);
		}
	}

	// Upserting fields to dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assert(indexes_.firstCompositePos() != 0);
	const int borderIdx = indexes_.totalSize() > 1 ? 1 : 0;
	int field = borderIdx;
	do {
		field %= indexes_.firstCompositePos();
		Index &index = *indexes_[field];
		bool isIndexSparse = index.Opts().IsSparse();
		assert(!isIndexSparse || (isIndexSparse && index.Fields().getTagsPathsLength() > 0));

		if (isIndexSparse) {
			assert(index.Fields().getTagsPathsLength() > 0);
			plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else {
			plNew.Get(field, skrefs);
		}

		if (index.Opts().GetCollateMode() == CollateUTF8)
			for (auto &key : skrefs) key.EnsureUTF8();

		// Check for update
		if (doUpdate) {
			if (isIndexSparse) {
				pl.GetByJsonPath(index.Fields().getTagsPath(0), krefs, index.KeyType());
			} else {
				pl.Get(field, krefs);
			}
			for (auto key : krefs) index.Delete(key, id);
			if (!krefs.size()) index.Delete(KeyRef(), id);
		}
		// Put value to index
		krefs.resize(0);
		for (auto key : skrefs) krefs.push_back(index.Upsert(key, id));

		// Put value to payload
		if (!isIndexSparse) pl.Set(field, krefs);
		// If no krefs upsert empty value to index
		if (!skrefs.size()) index.Upsert(KeyRef(), id);
	} while (++field != borderIdx);

	// Upsert to composite indexes
	for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Upsert(KeyRef(plData), id);
	}
}

void Namespace::updateTagsMatcherFromItem(ItemImpl *ritem, string &jsonSliceBuf) {
	if (ritem->tagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name_.c_str(), ritem->tagsMatcher().dump().c_str());
	}

	if (ritem->Type().get() != payloadType_.get() || !tagsMatcher_.try_merge(ritem->tagsMatcher())) {
		jsonSliceBuf = ritem->GetJSON().ToString();
		logPrintf(LogInfo, "Conflict TagsMatcher of namespace '%s' on modify: item:\n%s\ntm is\n%s\nnew tm is\n %s\n", name_.c_str(),
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

	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	WLock lock(mtx_);
	calc.LockHit();

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
		string_view b = itemImpl->GetCJSON();
		updates_->Put(string_view(pk), b);
		++unflushedCount_;
	}
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> Namespace::findByPK(ItemImpl *ritem) {
	h_vector<IdSetRef, 4> ids;
	h_vector<IdSetRef::iterator, 4> idsIt;

	if (!pkFields_.size()) {
		throw Error(errLogic, "Trying to modify namespace '%s', but it doesn't contain any PK indexes", name_.c_str());
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
		assert(indexes_.firstCompositePos() != 0);
		int field = indexes_.firstCompositePos();
		bool was = false;
		do {
			field %= indexes_.totalSize();
			if (!ctx.indexes() || ctx.indexes()->contains(field) || (ctx.phases() & CommitContext::MakeSortOrders)) {
				if (!commitedIndexes_.contains(field)) {
					{
						PerfStatCalculatorST calc(indexes_[field]->GetCommitPerfCounter(), enablePerfCounters_);
						calc.LockHit();
						was = indexes_[field]->Commit(ctx);
					}
					if (was) commitedIndexes_.push_back(field);
				}
			}
		} while (++field != indexes_.firstCompositePos());
		if (was) logPrintf(LogTrace, "Namespace::Commit ('%s'),%d items", name_.c_str(), int(items_.size()));
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
			if ((idxNo != IndexValueType::SetByJsonPath) && !preparedIndexes_.contains(idxNo)) {
				assert(static_cast<size_t>(idxNo) < indexes_.size());
				indexes_[idxNo]->Commit(ctx1);
				preparedIndexes_.push_back(idxNo);
			}
	}
}

void Namespace::markUpdated() {
	sortOrdersBuilt_ = false;
	sortedQueriesCount_ = 0;
	preparedIndexes_.clear();
	commitedIndexes_.clear();
	invalidateQueryCache();
	invalidateJoinCache();
}

void Namespace::Select(QueryResults &result, SelectCtx &params) {
	NsSelecter selecter(this);
	selecter(result, params);
}

NamespaceDef Namespace::getDefinition() {
	auto pt = this->payloadType_;
	NamespaceDef nsDef(name_, StorageOpts().Enabled(!dbpath_.empty()));

	for (int i = 1; i < int(indexes_.size()); i++) {
		IndexDef indexDef;
		const Index &index = *indexes_[i];
		indexDef.name_ = index.Name();
		indexDef.opts_ = index.Opts();
		indexDef.FromType(index.Type());

		if (index.Opts().IsSparse()) {
			assert(index.Fields().getJsonPathsLength() > 0);
			for (size_t i = 0; i < index.Fields().getJsonPathsLength(); ++i) {
				indexDef.jsonPaths_.push_back(index.Fields().getJsonPath(i));
			}
		} else if (i < payloadType_.NumFields()) {
			indexDef.jsonPaths_.Set(payloadType_->Field(i).JsonPaths());
		}

		nsDef.AddIndex(indexDef);
	}

	return nsDef;
}

NamespaceDef Namespace::GetDefinition() {
	RLock rlock(mtx_);
	return getDefinition();
}

NamespaceMemStat Namespace::GetMemStat() {
	RLock lck(mtx_);

	NamespaceMemStat ret;
	ret.name = name_;
	ret.joinCache = joinCache_->GetMemStat();
	ret.queryCache = queryCache_->GetMemStat();

	ret.itemsCount = items_.size() - free_.size();
	for (auto &item : items_) {
		if (!item.IsFree()) ret.dataSize += item.GetCapacity() + sizeof(PayloadValue::dataHeader);
	}

	ret.emptyItemsCount = free_.size();

	ret.Total.dataSize = ret.dataSize + items_.capacity() * sizeof(PayloadValue);
	ret.Total.cacheSize = ret.joinCache.totalSize + ret.queryCache.totalSize;

	for (auto &idx : indexes_) {
		auto istat = idx->GetMemStat();
		ret.Total.indexesSize += istat.idsetPlainSize + istat.idsetBTreeSize + istat.sortOrdersSize + istat.fulltextSize + istat.columnSize;
		ret.Total.dataSize += istat.dataSize;
		ret.Total.cacheSize += istat.idsetCache.totalSize;
		ret.indexes.push_back(istat);
	}

	char *endp;
	ret.updatedUnixNano = strtoull(getMeta("updated").c_str(), &endp, 10);
	ret.storageOK = storage_ != nullptr;
	ret.storagePath = dbpath_;
	return ret;
}

NamespacePerfStat Namespace::GetPerfStat() {
	NamespacePerfStat ret;
	ret.name = name_;
	ret.selects = selectPerfCounter_.Get<PerfStat>();
	ret.updates = updatePerfCounter_.Get<PerfStat>();
	for (unsigned i = 1; i < indexes_.size(); i++) {
		ret.indexes.emplace_back(indexes_[i]->GetIndexPerfStat());
	}
	return ret;
}

bool Namespace::loadIndexesFromStorage() {
	// Check if indexes structures are ready.
	assert(indexes_.size() == 1);
	assert(items_.size() == 0);

	string def;
	Error status = storage_->Read(StorageOpts().FillCache(), string_view(kStorageTagsPrefix), def);
	if (!status.ok() && status.code() != errNotFound) {
		throw Error(errNotValid, "Error load namespace from storage '%s': %s", name_.c_str(), status.what().c_str());
	}

	if (def.size()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logPrintf(LogTrace, "Loaded tags of namespace %s:\n%s", name_.c_str(), tagsMatcher_.dump().c_str());
	}

	def.clear();
	status = storage_->Read(StorageOpts().FillCache(), string_view(kStorageIndexesPrefix), def);

	if (!status.ok() && status.code() != errNotFound) {
		throw Error(errNotValid, "Error load namespace from storage '%s': %s", name_.c_str(), status.what().c_str());
	}

	if (def.size()) {
		Serializer ser(def.data(), def.size());
		const uint32_t dbMagic = ser.GetUInt32();
		if (dbMagic != kStorageMagic) {
			logPrintf(LogError, "Storage magic mismatch. want %08X, got %08X", kStorageMagic, dbMagic);
			return false;
		}

		const uint32_t dbVer = ser.GetUInt32();
		int count = ser.GetVarUint();
		while (count--) {
			IndexDef indexDef;
			if (dbVer == 6) {
				indexDef.name_ = ser.GetVString().ToString();

				int jsonPathsCount = ser.GetVarUint();
				while (jsonPathsCount > 0) {
					indexDef.jsonPaths_.emplace_back(ser.GetVString().ToString());
					jsonPathsCount--;
				}

				indexDef.FromType(IndexType(ser.GetVarUint()));

				indexDef.opts_.Array(ser.GetVarUint());
				indexDef.opts_.PK(ser.GetVarUint());
				indexDef.opts_.Dense(ser.GetVarUint());
				indexDef.opts_.Sparse(false);
				ser.GetVarUint();  // skip Appendable
				indexDef.opts_.SetCollateMode(static_cast<CollateMode>(ser.GetVarUint()));
				if (indexDef.opts_.GetCollateMode() == CollateCustom)
					indexDef.opts_.collateOpts_ = CollateOpts(ser.GetVString().ToString());
			} else {
				if (dbVer != kStorageVersion) {
					logPrintf(LogError, "Storage version mismatch. want %08X, got %08X", kStorageVersion, dbVer);
					return false;
				}
				string indexData = ser.GetVString().ToString();
				Error err = indexDef.FromJSON(const_cast<char *>(indexData.c_str()));
				if (!err.ok()) throw err;
			}

			if (indexDef.jsonPaths_.empty()) indexDef.jsonPaths_.emplace_back(string());

			addIndex(indexDef);
		}
	}

	logPrintf(LogTrace, "Loaded index structure of namespace '%s'\n%s", name_.c_str(), payloadType_->ToString().c_str());

	return true;
}

void Namespace::saveIndexesToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveIndexesToStorage (%s)", name_.c_str());

	WrSerializer ser;
	ser.PutUInt32(kStorageMagic);
	ser.PutUInt32(kStorageVersion);

	ser.PutVarUint(indexes_.size() - 1);
	NamespaceDef nsDef = getDefinition();
	for (const IndexDef &indexDef : nsDef.indexes) {
		WrSerializer wrser;
		indexDef.GetJSON(wrser);
		ser.PutVString(wrser.Slice());
	}

	storage_->Write(StorageOpts().FillCache(), string_view(kStorageIndexesPrefix),
					string_view(reinterpret_cast<const char *>(ser.Buf()), ser.Len()));
}

void Namespace::EnableStorage(const string &path, StorageOpts opts) {
	string dbpath = fs::JoinPath(path, name_);
	datastorage::StorageType storageType = datastorage::StorageType::LevelDB;

	WLock lock(mtx_);
	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", name_.c_str(), path.c_str());
	}

	storage_.reset(datastorage::StorageFactory::create(storageType));

	bool success = false;
	while (!success) {
		Error status = storage_->Open(dbpath, opts);
		if (!status.ok()) {
			if (!opts.IsDropOnFileFormatError()) {
				storage_ = nullptr;
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s' - %s", name_.c_str(), path.c_str(),
							status.what().c_str());
			}
		} else {
			success = loadIndexesFromStorage();
			if (!success && !opts.IsDropOnFileFormatError()) {
				storage_ = nullptr;
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s': format error", name_.c_str(), dbpath.c_str());
			}
			getCachedMode();
		}
		if (!success && opts.IsDropOnFileFormatError()) {
			opts.DropOnFileFormatError(false);
			storage_->Destroy(path);
			storage_ = nullptr;
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
	getCachedMode();
	logPrintf(LogTrace, "Loading items to '%s' from storage", name_.c_str());
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	ItemImpl item(payloadType_, tagsMatcher_);
	item.Unsafe(true);
	int errCount = 0;
	Error lastErr = errOK;
	for (dbIter->Seek(kStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), string_view(kStorageItemPrefix "\xFF")) < 0; dbIter->Next()) {
		string_view dataSlice = dbIter->Value();
		if (dataSlice.size() > 0) {
			if (!pkFields_.size()) {
				throw Error(errLogic, "Can't load data storage of '%s' - there are no PK fields in ns", name_.c_str());
			}
			auto err = item.FromCJSON(dataSlice);
			if (!err.ok()) {
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", name_.c_str(), err.what().c_str());
				errCount++;
				lastErr = err;
				continue;
			}

			IdType id = items_.size();
			items_.emplace_back(PayloadValue(item.GetPayload().RealSize()));
			upsert(&item, id, false);

			ldcount += dataSlice.size();
		}
	}
	logPrintf(LogInfo, "[%s] Done loading storage. %d items loaded (%d errors %s), total size=%dM", name_.c_str(), int(items_.size()),
			  errCount, lastErr.what().c_str(), int(ldcount / (1024 * 1024)));
}

void Namespace::FlushStorage() {
	WLock wlock(mtx_);
	flushStorage();
}

void Namespace::flushStorage() {
	if (storage_) {
		putCachedMode();

		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			tagsMatcher_.serialize(ser);
			updates_->Put(string_view(kStorageTagsPrefix), string_view(reinterpret_cast<const char *>(ser.Buf()), ser.Len()));
			unflushedCount_++;
			tagsMatcher_.clearUpdated();
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_.c_str(), tagsMatcher_.dump().c_str());
		}

		if (unflushedCount_) {
			Error status = storage_->Write(StorageOpts().FillCache(), *(updates_.get()));
			if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage: %s", name_.c_str(), status.what().c_str());
			updates_->Clear();
			unflushedCount_ = 0;
		}
	}
}

void Namespace::DeleteStorage() {
	WLock lck(mtx_);
	if (storage_) {
		storage_->Destroy(dbpath_.c_str());
		dbpath_.clear();
		storage_.reset();
	}
}

void Namespace::CloseStorage() {
	WLock lck(mtx_);
	if (storage_) {
		flushStorage();
		dbpath_.clear();
		storage_.reset();
	}
}

Item Namespace::NewItem() {
	RLock lock(mtx_);
	return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields_));
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
		Error status = storage_->Read(StorageOpts().FillCache(), string_view(kStorageMetaPrefix + key), data);
		if (status.ok()) {
			return data;
		}
	}

	return "";
}

// Put meta data to storage by key
void Namespace::PutMeta(const string &key, const string_view &data) {
	WLock lock(mtx_);
	putMeta(key, data);
}

// Put meta data to storage by key
void Namespace::putMeta(const string &key, const string_view &data) {
	meta_[key] = data.ToString();

	if (storage_) {
		storage_->Write(StorageOpts().FillCache(), string_view(kStorageMetaPrefix + key), string_view(data.data(), data.size()));
	}
}

vector<string> Namespace::EnumMeta() {
	vector<string> ret;

	RLock lck(mtx_);
	for (auto &m : meta_) {
		ret.push_back(m.first);
	}
	if (!storage_) return ret;

	StorageOpts opts;
	opts.FillCache(false);
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	size_t prefixLen = strlen(kStorageMetaPrefix);

	for (dbIter->Seek(string_view(kStorageMetaPrefix));
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), string_view(kStorageMetaPrefix "\xFF")) < 0; dbIter->Next()) {
		string_view keySlice = dbIter->Key();
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
void Namespace::invalidateJoinCache() {
	if (!joinCache_->Empty()) {
		logPrintf(LogTrace, "[*] invalidate join cache. namespace: %s\n", name_.c_str());
		joinCache_.reset(new JoinCache);
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
	putMeta("_SERIAL_" + sqlFuncStruct.field, string_view(s));

	return counter;
}

void Namespace::FillResult(QueryResults &result, IdSet::Ptr ids, const h_vector<string, 4> &selectFilter) {
	result.addNSContext(payloadType_, tagsMatcher_, JsonPrintFilter(tagsMatcher_, selectFilter));
	for (auto &id : *ids) {
		result.Add({id, items_[id].GetVersion(), items_[id], 0, 0});
	}
}

void Namespace::GetFromJoinCache(JoinCacheRes &ctx) {
	RLock lock(cache_mtx_);

	if (cacheMode_ == CacheModeOff) return;
	auto it = joinCache_->Get(ctx.key);
	ctx.needPut = false;
	ctx.haveData = false;
	if (it.key) {
		if (!it.val.inited) {
			ctx.needPut = true;
		} else {
			ctx.haveData = true;
			ctx.it = std::move(it);
		}
	}
}

void Namespace::GetIndsideFromJoinCache(JoinCacheRes &ctx) {
	RLock lock(cache_mtx_);

	if (cacheMode_ != CacheModeAggressive) return;
	auto it = joinCache_->Get(ctx.key);
	ctx.needPut = false;
	ctx.haveData = false;
	if (it.key) {
		if (!it.val.inited) {
			ctx.needPut = true;
		} else {
			ctx.haveData = true;
			ctx.it = std::move(it);
		}
	}
}

void Namespace::PutToJoinCache(JoinCacheRes &res, SelectCtx::PreResult::Ptr preResult) {
	JoinCacheVal joinCacheVal;
	res.needPut = false;
	joinCacheVal.inited = true;
	joinCacheVal.preResult = preResult;
	joinCache_->Put(res.key, joinCacheVal);
}
void Namespace::PutToJoinCache(JoinCacheRes &res, JoinCacheVal &val) {
	val.inited = true;
	joinCache_->Put(res.key, val);
}
void Namespace::SetCacheMode(CacheMode cacheMode) {
	WLock lock(cache_mtx_);
	needPutCacheMode_ = true;
	cacheMode_ = cacheMode;
}
void Namespace::putCachedMode() {
	RLock lock(cache_mtx_);
	if (!needPutCacheMode_) return;
	needPutCacheMode_ = false;
	string data = to_string(cacheMode_);
	storage_->Write(StorageOpts().FillCache(), string_view(kStorageCachePrefix), string_view(data.data(), data.size()));
}
void Namespace::getCachedMode() {
	WLock lock(cache_mtx_);
	string data;
	Error status = storage_->Read(StorageOpts().FillCache(), string_view(kStorageCachePrefix), data);
	if (!status.ok()) {
		return;
	}

	cacheMode_ = static_cast<CacheMode>(atoi(data.c_str()));
}
}  // namespace reindexer
