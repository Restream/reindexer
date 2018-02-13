#include "core/namespace.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include "core/aggregator.h"
#include "core/sqlfunc/sqlfunc.h"
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
	  name(src.name),
	  payloadType_(src.payloadType_),
	  tagsMatcher_(src.tagsMatcher_),
	  storage_(src.storage_),
	  storageSnapshot_(src.storageSnapshot_),
	  updates_(src.updates_),
	  unflushedCount_(0),
	  sortOrdersBuilt_(false),
	  pkFields_(src.pkFields_),
	  meta_(src.meta_),
	  dbpath_(src.dbpath_),
	  queryCache(src.queryCache) {
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));
	logPrintf(LogTrace, "Namespace::Namespace (clone %s)", name.c_str());
}

Namespace::Namespace(const string &_name)
	: name(_name),
	  payloadType_(PayloadType::Ptr(new PayloadType(_name))),
	  tagsMatcher_(payloadType_),
	  unflushedCount_(0),
	  sortOrdersBuilt_(false),
	  queryCache(make_shared<QueryCache>()) {
	logPrintf(LogTrace, "Namespace::Namespace (%s)", name.c_str());
	items_.reserve(10000);

	// Add index and payload field for tuple of non indexed fields
	addIndex("-tuple", "", IndexStrStore, IndexOpts());
}

Namespace::~Namespace() {
	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items %s", name.c_str(), items_.size(), storageSnapshot_ ? "(snapshot)" : "");
	if (storage_ && storageSnapshot_) storage_->ReleaseSnapshot(storageSnapshot_);
}

bool Namespace::AddIndex(const string &index, const string &jsonPath, IndexType type, IndexOpts opts) {
	WLock wlock(mtx_);
	bool idxChanged = addIndex(index, jsonPath, type, opts);
	bool haveData = bool(items_.size());

	if (storage_ && idxChanged) saveIndexesToStorage();

	return idxChanged && haveData;
}

bool Namespace::addIndex(const string &index, const string &jsonPath, IndexType type, IndexOpts opts) {
	if (type == IndexCompositeBTree || type == IndexCompositeHash || type == IndexCompositeText || type == IndexCompositeNewText) {
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
			throw Error(errConflict, "Can't add PK Index '%s.%s', already exists another index with same json path", name.c_str(),
						index.c_str());
		}

		// Create new index object
		insertIndex(Index::New(type, index, opts), idxNo, index);

		// Add payload field corresponding to index
		payloadType_.clone()->Add(PayloadFieldType(indexes_[idxNo]->KeyType(), index, jsonPath, opts.IsArray()));
		tagsMatcher_.updatePayloadType(payloadType_);
		for (auto &idx : indexes_) idx->UpdatePayloadType(payloadType_);

		// Notify caller what indexes was changed
		return true;
	}

	idxNo = idxNameIt->second;
	// Existing index

	// payloadType must be in sync with indexes
	assert(idxNo < payloadType_->NumFields());

	// Check conflicts.
	if (indexes_[idxNameIt->second]->type != type) {
		throw Error(errConflict, "Index '%s.%s' already exists with different type", name.c_str(), index.c_str());
	}

	if (pkFields_.contains(idxNo) != bool(opts.IsPK())) {
		throw Error(errConflict, "Index '%s.%s' already exists with different PK attribute", name.c_str(), index.c_str());
	}

	// append different indexes
	if (fieldByJsonPath != idxNo && jsonPath.length()) {
		if (!opts.IsAppendable()) {
			throw Error(errConflict, "Index '%s.%s' already exists with different json path '%s' ", name.c_str(), index.c_str(),
						jsonPath.c_str());
		}

		indexes_[idxNo]->opts_.Array();
		payloadType_.clone()->Add(PayloadFieldType(indexes_[idxNo]->KeyType(), index, jsonPath, opts.IsArray()));
		tagsMatcher_.updatePayloadType(payloadType_);
		for (auto &idx : indexes_) idx->UpdatePayloadType(payloadType_);
		return true;
	}

	return false;
}

bool Namespace::AddCompositeIndex(const string &index, IndexType type, IndexOpts opts) {
	FieldsSet ff;
	vector<string> subIdxes;
	bool allPK = true;
	string idxContent = index, realName = index;
	auto pos = idxContent.find_first_of("=");
	if (pos != string::npos) {
		realName = idxContent.substr(pos + 1);
		idxContent = idxContent.substr(0, pos);
	}

	for (auto subIdx : split(idxContent, "+", true, subIdxes)) {
		auto idxNameIt = indexesNames_.find(subIdx);
		if (idxNameIt == indexesNames_.end())
			throw Error(errParams, "Subindex '%s' not found for composite index '%s'", subIdx.c_str(), index.c_str());
		ff.push_back(idxNameIt->second);
		if (!indexes_[idxNameIt->second]->opts_.IsPK()) allPK = false;
	}

	auto idxNameIt = indexesNames_.find(realName);
	if (idxNameIt == indexesNames_.end()) {
		if (allPK) {
			logPrintf(LogInfo, "Using composite index instead of single %s", index.c_str());
			for (auto i : ff) indexes_[i]->opts_.PK(false);
		}
		opts.PK(opts.IsPK() || allPK);
		int idxNo = indexes_.size();
		insertIndex(Index::NewComposite(type, index, opts, payloadType_, ff), idxNo, realName);
		return true;
	}

	// Existing index

	// Check conflicts.
	if (indexes_[idxNameIt->second]->type != type) {
		throw Error(errConflict, "Index '%s' already exists with different type", index.c_str());
	}

	if (indexes_[idxNameIt->second]->fields_ != ff) {
		throw Error(errConflict, "Index '%s' already exists with different fields", index.c_str());
	}
	return false;
}

void Namespace::insertIndex(Index *newIndex, int idxNo, const string &realName) {
	if (newIndex->opts_.IsPK() && newIndex->opts_.IsArray()) {
		throw Error(errParams, "Can't add index '%s' in namespace '%s'. PK field can't be array", newIndex->name.c_str(), name.c_str());
	}

	indexes_.insert(indexes_.begin() + idxNo, unique_ptr<Index>(newIndex));

	for (auto &n : indexesNames_) {
		if (n.second >= idxNo) {
			n.second++;
		}
	}

	indexesNames_.insert({realName, idxNo});

	if (newIndex->opts_.IsPK()) {
		if (newIndex->KeyType() == KeyValueComposite) {
			for (auto i : newIndex->fields_) pkFields_.push_back(i);
		} else {
			pkFields_.push_back(idxNo);
		}
	}
}

void Namespace::ConfigureIndex(const string &index, const string &config) { indexes_[getIndexByName(index)]->Configure(config); }

void Namespace::Insert(Item *item, bool store) { upsertInternal(item, store, INSERT_MODE); }

void Namespace::Update(Item *item, bool store) { upsertInternal(item, store, UPDATE_MODE); }

void Namespace::Upsert(Item *item, bool store) { upsertInternal(item, store, INSERT_MODE | UPDATE_MODE); }

void Namespace::Delete(Item *item) {
	ItemImpl *ritem = reinterpret_cast<ItemImpl *>(item);
	string jsonSliceBuf;

	WLock lock(mtx_);

	updateTagsMatcherFromItem(ritem, jsonSliceBuf);

	auto exists = findByPK(ritem);
	IdType id = exists.first;

	if (!exists.second) {
		return;
	}

	reinterpret_cast<ItemImpl *>(item)->SetID(id, items_[id].GetVersion());
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
	markUpdated(id);
	free_.emplace(id);
}

void Namespace::Delete(const Query &q, QueryResults &result) {
	WLock lock(mtx_);
	NsSelecter selecter(this);

	selecter(result, SelectCtx(q, nullptr));

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
	if (doUpdate) {
		plData.AllocOrClone(pl.RealSize());
	}
	markUpdated(id);

	KeyRefs krefs, skrefs;

	// Delete from composite indexes first
	int field = 0;
	for (field = ritem->NumFields(); field < int(indexes_.size()); ++field)
		if (doUpdate) indexes_[field]->Delete(KeyRef(plData), id);

	// Upsert fields to regular indexes
	for (field = 0; field < ritem->NumFields(); ++field) {
		auto &index = *indexes_[field];

		ritem->Get(field, skrefs);

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
	if (ritem->getTagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name.c_str(), ritem->getTagsMatcher().dump().c_str());
	}

	if (&ritem->Type() != payloadType_.get() || !tagsMatcher_.try_merge(ritem->getTagsMatcher())) {
		jsonSliceBuf = ritem->GetJSON().ToString();
		logPrintf(LogInfo, "Conflict TagsMatcher of namespace '%s' on modify: item:\n%s\ntm is\nnew tm is\n %s\n", name.c_str(),
				  jsonSliceBuf.c_str(), tagsMatcher_.dump().c_str(), ritem->getTagsMatcher().dump().c_str());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);

		auto err = tmpItem.FromJSON(jsonSliceBuf, nullptr);
		*ritem = move(tmpItem);

		if (!err.ok()) throw err;
		if (!tagsMatcher_.try_merge(ritem->getTagsMatcher())) throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
	}
}

void Namespace::upsertInternal(Item *item, bool store, uint8_t mode) {
	// Item to upsert
	ItemImpl *ritem = reinterpret_cast<ItemImpl *>(item);
	string jsonSlice;

	WLock lock(mtx_);

	if (storageSnapshot_) {
		throw Error(errLogic, "Could not insert item to readonly snapshot of '%s'", name.c_str());
	}

	updateTagsMatcherFromItem(ritem, jsonSlice);

	auto realItem = findByPK(ritem);
	IdType id = realItem.first;
	bool exists = realItem.second;

	switch (mode) {
		case INSERT_MODE:
			if (!exists) {
				// Check if payload not created yet, create it
				id = createItem(ritem->RealSize());
			} else {
				reinterpret_cast<ItemImpl *>(item)->SetID(-1, -1);
				return;
			}
			break;

		case UPDATE_MODE:
			if (!exists) {
				reinterpret_cast<ItemImpl *>(item)->SetID(-1, -1);
				return;
			}
			break;

		case (INSERT_MODE | UPDATE_MODE):
			if (!exists) id = createItem(ritem->RealSize());
			break;

		default:
			throw Error(errLogic, "Unknown write mode");
	}

	setFieldsBasedOnPrecepts(ritem);

	upsert(ritem, id, exists);
	reinterpret_cast<ItemImpl *>(item)->SetID(id, items_[id].GetVersion());

	if (storage_ && store) {
		char pk[512];
		auto prefLen = strlen(kStorageItemPrefix);
		memcpy(pk, kStorageItemPrefix, prefLen);
		ritem->GetPK(pk + prefLen, sizeof(pk) - prefLen, pkFields_);
		Slice b = ritem->GetCJSON();
		updates_->Put(Slice(pk), Slice(b.data(), b.size()));
		++unflushedCount_;
	}
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> Namespace::findByPK(ItemImpl *ritem) {
	h_vector<IdSetRef, 4> ids;
	h_vector<IdSetRef::iterator, 4> idsIt;

	// It's faster equalent of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	for (int field = 0; field < int(indexes_.size()); ++field)
		if (indexes_[field]->opts_.IsPK()) {
			KeyRefs krefs;
			if (field < ritem->NumFields())
				ritem->Get(field, krefs);
			else
				krefs.push_back(KeyRef(*ritem->Value()));
			assertf(krefs.size() == 1, "Pkey field must contain 1 key, but there '%d' in '%s.%s'", int(krefs.size()), name.c_str(),
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

void Namespace::commit(const NSCommitContext &ctx, std::function<void()> lockUpgrader) {
	bool needCommit = (!sortOrdersBuilt_ && (ctx.phases() & CommitContext::MakeSortOrders));

	if (ctx.indexes())
		for (auto idxNo : *ctx.indexes()) needCommit = needCommit || !preparedIndexes_.contains(idxNo) || !commitedIndexes_.contains(idxNo);

	if (!needCommit) {
		return;
	}

	if (lockUpgrader) lockUpgrader();

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
		if (was) logPrintf(LogTrace, "Namespace::Commit ('%s'),%d items", name.c_str(), items_.size());
		//	items_.shrink_to_fit();
	}

	if (!sortOrdersBuilt_ && (ctx.phases() & CommitContext::MakeSortOrders)) {
		// Update sort orders and sort_id for each index

		int i = 1;
		for (auto &idxIt : indexes_) {
			if (idxIt->IsOrdered()) {
				idxIt->sort_id = i++;
				NSUpdateSortedContext sortCtx(*this, idxIt->sort_id);
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

void Namespace::markUpdated(IdType /*id*/) {
	sortOrdersBuilt_ = false;
	preparedIndexes_.clear();
	commitedIndexes_.clear();
	invalidateQueryCache();
}

void Namespace::Select(QueryResults &result, const SelectCtx &params) {
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

	NamespaceDef nsDef(name, StorageOpts().Enabled(!dbpath_.empty()));

	for (size_t idx = 1; idx < indexes_.size(); idx++) {
		IndexDef indexDef;
		indexDef.FromType(indexes_[idx]->type);
		indexDef.name = indexes_[idx]->name;
		indexDef.opts = indexes_[idx]->opts_;

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

	logPrintf(LogInfo, "Loaded index structure of namespace '%s'\n%s", name.c_str(), payloadType_->ToString().c_str());

	def.clear();
	status = storage_->Read(StorageOpts().FillCache(), Slice(kStorageTagsPrefix), def);
	if (status.ok() && def.size()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logPrintf(LogTrace, "Loaded tags of namespace %s:\n%s", name.c_str(), tagsMatcher_.dump().c_str());
	}

	return true;
}

void Namespace::saveIndexesToStorage() {
	logPrintf(LogTrace, "Namespace::saveIndexesToStorage (%s)", name.c_str());

	WrSerializer ser;
	ser.PutUInt32(kStorageMagic);
	ser.PutUInt32(kStorageVersion);

	ser.PutVarUint(indexes_.size() - 1);
	for (int f = 1; f < int(indexes_.size()); f++) {
		ser.PutVString(indexes_[f]->name);

		if (f < payloadType_->NumFields()) {
			ser.PutVarUint(payloadType_->Field(f).JsonPaths().size());
			for (auto &jsonPath : payloadType_->Field(f).JsonPaths()) ser.PutVString(jsonPath);
		} else {
			ser.PutVarUint(0);
		}

		ser.PutVarUint(indexes_[f]->type);
		ser.PutVarUint(indexes_[f]->opts_.IsArray());
		ser.PutVarUint(indexes_[f]->opts_.IsPK());
		ser.PutVarUint(indexes_[f]->opts_.IsDense());
		ser.PutVarUint(indexes_[f]->opts_.IsAppendable());
		ser.PutVarUint(indexes_[f]->opts_.GetCollateMode());
	}

	storage_->Write(StorageOpts().FillCache(), Slice(kStorageIndexesPrefix), Slice(reinterpret_cast<const char *>(ser.Buf()), ser.Len()));
}

void Namespace::EnableStorage(const string &path, StorageOpts opts) {
	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", name.c_str(), path.c_str());
	}

	string dbpath = JoinPath(path, name);
	datastorage::StorageType storageType = datastorage::StorageType::LevelDB;

	WLock lock(mtx_);
	storage_.reset(datastorage::StorageFactory::create(storageType));

	bool success = false;
	while (!success) {
		Error status = storage_->Open(dbpath, opts);
		if (!status.ok()) {
			if (!opts.IsDropOnFileFormatError()) {
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s' - %s", name.c_str(), path.c_str(),
							status.what().c_str());
			}
		} else {
			success = loadIndexesFromStorage();
			if (!success && !opts.IsDropOnFileFormatError()) {
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s': format error", name.c_str(), dbpath.c_str());
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

	logPrintf(LogTrace, "Loading items to '%s' from cache", name.c_str());
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	ItemImpl item(payloadType_, tagsMatcher_);
	for (dbIter->Seek(kStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), Slice(kStorageItemPrefix "\xFF")) < 0; dbIter->Next()) {
		Slice dataSlice = dbIter->Value();
		if (dataSlice.size() > 0) {
			auto err = item.FromCJSON(Slice(dataSlice.data(), dataSlice.size()));
			if (!err.ok()) {
				logPrintf(LogError, "Error load item to '%s' from cache: '%s'", name.c_str(), err.what().c_str());
				throw err;
			}

			IdType id = items_.size();
			items_.emplace_back(PayloadValue(item.RealSize()));
			upsert(&item, id, false);

			ldcount += dataSlice.size();
		}
	}
	logPrintf(LogTrace, "[%s] Done loading cache. %d items loaded, total size=%dM", name.c_str(), int(items_.size()),
			  int(ldcount / (1024 * 1024)));
}

void Namespace::FlushStorage() {
	WLock wlock(mtx_);

	if (storageSnapshot_) {
		return;
	}

	if (storage_) {
		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			tagsMatcher_.serialize(ser);
			updates_->Put(Slice(kStorageTagsPrefix), Slice(reinterpret_cast<const char *>(ser.Buf()), ser.Len()));
			unflushedCount_++;
			tagsMatcher_.clearUpdated();
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name.c_str(), tagsMatcher_.dump().c_str());
		}

		if (unflushedCount_) {
			Error status = storage_->Write(StorageOpts().FillCache(), *(updates_.get()));
			if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage:", name.c_str(), status.what().c_str());
			updates_->Clear();
			unflushedCount_ = 0;
		}
	}
}

void Namespace::DeleteStorage() {
	if (!dbpath_.empty()) {
		storage_->Destroy(dbpath_.c_str());
		storage_.reset();
	}
}

void Namespace::NewItem(Item **item) {
	RLock lock(mtx_);
	*item = new ItemImpl(payloadType_, tagsMatcher_);
}

// Get meta data from storage by key
string Namespace::GetMeta(const string &key) {
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

// Lock reads to snapshot
void Namespace::LockSnapshot() {
	if (!storage_) return;
	if (storageSnapshot_) storage_->ReleaseSnapshot(storageSnapshot_);
	storageSnapshot_ = storage_->MakeSnapshot();
	assert(storageSnapshot_);
}

int Namespace::getIndexByName(const string &index) {
	auto idxIt = indexesNames_.find(index);

	if (idxIt == indexesNames_.end()) throw Error(errParams, "Index '%s' not found in '%s'", index.c_str(), name.c_str());

	return idxIt->second;
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
	if (!queryCache->Empty()) {
		logPrintf(LogTrace, "[*] invalidate query cache. namespace: %s\n", name.c_str());
		queryCache.reset(new QueryCache);
	}
}

void Namespace::setFieldsBasedOnPrecepts(ItemImpl *ritem) {
	for (auto &precept : ritem->GetPrecepts()) {
		SqlFunc sqlFunc;
		SqlFuncStruct sqlFuncStruct = sqlFunc.Parse(precept);

		KeyValue field = ritem->GetField(sqlFuncStruct.field);

		KeyRefs refs;
		KeyValue value(sqlFuncStruct.value);

		if (sqlFuncStruct.isFunction) {
			if (sqlFuncStruct.funcName == "now") {
				string mode = "sec";
				if (sqlFuncStruct.funcArgs.size()) {
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
		refs.push_back(value);

		Error err = ritem->SetField(sqlFuncStruct.field, refs);
		if (!err.ok()) {
			throw Error(errParams, "Namespace '%s' doesn't contain field '%s' for setting value '%s'", name.c_str(),
						sqlFuncStruct.field.c_str(), sqlFuncStruct.value.c_str());
		}
	}
}

int64_t Namespace::funcGetSerial(SqlFuncStruct sqlFuncStruct) {
	int64_t counter = kStorageSerialInitial;

	string ser = GetMeta("_SERIAL_" + sqlFuncStruct.field);
	if (ser != "") {
		counter = stoi(ser) + 1;
	}

	string s = to_string(counter);
	PutMeta("_SERIAL_" + sqlFuncStruct.field, Slice(s));

	return counter;
}

}  // namespace reindexer
