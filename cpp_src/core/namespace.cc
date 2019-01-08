#include "core/namespace.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include "cjson/jsonbuilder.h"
#include "core/index/index.h"
#include "core/nsselecter/nsselecter.h"
#include "itemimpl.h"
#include "replicator/updatesobserver.h"
#include "replicator/walselecter.h"
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
using std::defer_lock;

#define kStorageItemPrefix "I"
#define kStorageIndexesPrefix "indexes"
#define kStorageReplStatePrefix "repl"
#define kStorageTagsPrefix "tags"
#define kStorageMetaPrefix "meta"
#define kStorageCachePrefix "cache"
#define kTupleName "-tuple"

static const string kPKIndexName = "#pk";
static const string kLSNIndexName = "#lsn";

#define kStorageMagic 0x1234FEDC
#define kStorageVersion 0x8

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
	  meta_(src.meta_),
	  dbpath_(src.dbpath_),
	  queryCache_(src.queryCache_),
	  joinCache_(src.joinCache_),
	  enablePerfCounters_(src.enablePerfCounters_.load()),
	  config_(src.config_),
	  wal_(src.wal_),
	  repl_(src.repl_),
	  observers_(src.observers_),
	  storageLoaded_(src.storageLoaded_.load()),
	  lastUpdateTime_(src.lastUpdateTime_.load()) {
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));
	logPrintf(LogTrace, "Namespace::Namespace (clone %s)", name_);
}

Namespace::Namespace(const string &name, UpdatesObservers &observers)
	: indexes_(*this),
	  name_(name),
	  payloadType_(name),
	  tagsMatcher_(payloadType_),
	  unflushedCount_(0),
	  sortOrdersBuilt_(false),
	  queryCache_(make_shared<QueryCache>()),
	  joinCache_(make_shared<JoinCache>()),
	  enablePerfCounters_(false),
	  observers_(observers),
	  storageLoaded_(false),
	  cancelCommit_(false),
	  lastUpdateTime_(0) {
	logPrintf(LogTrace, "Namespace::Namespace (%s)", name_);
	items_.reserve(10000);

	// Add index and payload field for tuple of non indexed fields
	IndexDef tupleIndexDef(kTupleName, {}, IndexStrStore, IndexOpts());
	addIndex(tupleIndexDef);
	updateSelectTime();
}

Namespace::~Namespace() {
	WLock wlock(mtx_);
	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items", name_, items_.size());
}

void Namespace::onConfigUpdated(DBConfigProvider &configProvider) {
	NamespaceConfigData configData;
	ReplicationConfigData replicationConf;
	configProvider.GetNamespaceConfig(GetName(), configData);
	replicationConf = configProvider.GetReplicationConfig();

	enablePerfCounters_ = configProvider.GetProfilingConfig().perfStats;

	WLock lk(mtx_);
	config_ = configData;
	storageOpts_.LazyLoad(configData.lazyLoad);
	storageOpts_.noQueryIdleThresholdSec = configData.noQueryIdleThreshold;

	if (isSystem()) return;

	// clusterID is not set in replication state. Init it
	if (repl_.clusterID == -1) repl_.clusterID = replicationConf.clusterID;

	if (replicationConf.role == ReplicationSlave && repl_.clusterID != replicationConf.clusterID) {
		throw Error(errConflict, "ClusterID of ns %s mismatch in storage state %d in config %d", name_, repl_.clusterID,
					replicationConf.clusterID);
	}

	// try to turn on/off replication

	// CASE1: Replication state same in config and state
	if (repl_.slaveMode == bool(replicationConf.role == ReplicationSlave)) return;

	unflushedCount_++;

	// CASE2: Replication enabled in state, but disabled in config
	if (repl_.slaveMode && replicationConf.role != ReplicationSlave) {
		// switch slave -> master
		repl_.slaveMode = false;
		logPrintf(LogTrace, "Disable slave mode for namespace '%s'", name_);
		return;
	}

	// CASE3: Replication enabled in config, but disabled in state
	if (!repl_.slaveMode && replicationConf.role == ReplicationSlave) {
		// switch master -> slave
		if (storageOpts_.IsSlaveMode()) {
			repl_.slaveMode = true;
			logPrintf(LogTrace, "Enable slave mode for namespace '%s'", name_);
			repl_.incarnationCounter++;
		}
		return;
	}
}

void Namespace::recreateCompositeIndexes(int startIdx, int endIdx) {
	for (int i = startIdx; i < endIdx; ++i) {
		std::unique_ptr<reindexer::Index> &index(indexes_[i]);
		if (isComposite(index->Type())) {
			IndexDef indexDef;
			indexDef.name_ = index->Name();
			indexDef.opts_ = index->Opts();
			indexDef.FromType(index->Type());

			index.reset(Index::New(indexDef, payloadType_, index->Fields()));
			for (IdType rowId = 0; rowId < static_cast<int>(items_.size()); ++rowId) {
				if (!items_[rowId].IsFree()) {
					indexes_[i]->Upsert(Variant(items_[rowId]), rowId);
				}
			}
		}
	}
}

void Namespace::updateItems(PayloadType oldPlType, const FieldsSet &changedFields, int deltaFields) {
	logPrintf(LogTrace, "Namespace::updateItems(%s) delta=%d", name_, deltaFields);

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

	VariantArray krefs, skrefs;
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
			logPrintf(LogTrace, "Can't apply indexes: %s", err.what());
			errCount++;
			lastErr = err;
		}

		PayloadValue plNew = oldValue.CopyTo(payloadType_, deltaFields >= 0);
		Payload newValue(payloadType_, plNew);

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			indexes_[fieldIdx]->Delete(Variant(plCurr), rowId);
		}

		for (auto fieldIdx : changedFields) {
			auto &index = *indexes_[fieldIdx];
			if ((fieldIdx == 0) || deltaFields <= 0) {
				oldValue.Get(fieldIdx, skrefs);
				for (auto key : skrefs) index.Delete(key, rowId);
				if (skrefs.empty()) index.Delete(Variant(), rowId);
			}

			if ((fieldIdx == 0) || deltaFields >= 0) {
				newItem.GetPayload().Get(fieldIdx, skrefs);
				krefs.resize(0);
				for (auto key : skrefs) krefs.push_back(index.Upsert(key, rowId));

				newValue.Set(fieldIdx, krefs);
				if (krefs.empty()) index.Upsert(Variant(), rowId);
			}
		}

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			indexes_[fieldIdx]->Upsert(Variant(plNew), rowId);
		}

		plCurr = std::move(plNew);
	}
	markUpdated();
	if (errCount != 0) {
		logPrintf(LogError, "Can't update indexes of %d items in namespace %s: %s", errCount, name_, lastErr.what());
	}
}

void Namespace::addToWAL(const IndexDef &indexDef, WALRecType type) {
	WrSerializer ser;
	indexDef.GetJSON(ser);
	WALRecord wrec(type, ser.Slice());
	int64_t lsn = repl_.slaveMode ? -1 : wal_.Add(wrec);
	observers_.OnWALUpdate(lsn, name_, wrec);
}

void Namespace::AddIndex(const IndexDef &indexDef) {
	WLock wlock(mtx_);
	addIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexAdd);
}

void Namespace::UpdateIndex(const IndexDef &indexDef) {
	WLock wlock(mtx_);
	updateIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexUpdate);
}

void Namespace::DropIndex(const IndexDef &indexDef) {
	WLock wlock(mtx_);
	dropIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexDrop);
}

void Namespace::dropIndex(const IndexDef &index) {
	auto itIdxName = indexesNames_.find(index.name_);
	if (itIdxName == indexesNames_.end()) {
		const char *errMsg = "Cannot remove index %s: doesn't exist";
		logPrintf(LogError, errMsg, index.name_);
		throw Error(errParams, errMsg, index.name_);
	}

	int fieldIdx = itIdxName->second;
	if (indexes_[fieldIdx]->Opts().IsSparse()) --sparseIndexesCount_;

	// Check, that index to remove is not a part of composite index
	for (int i = indexes_.firstCompositePos(); i < indexes_.totalSize(); ++i) {
		if (indexes_[i]->Fields().contains(fieldIdx))
			throw Error(LogError, "Cannot remove index %s : it's a part of a composite index %s", index.name_, indexes_[i]->Name());
	}
	for (auto &namePair : indexesNames_) {
		if (namePair.second >= fieldIdx) {
			namePair.second--;
		}
	}

	const unique_ptr<Index> &indexToRemove = indexes_[fieldIdx];
	if (indexToRemove->Opts().IsPK()) {
		indexesNames_.erase(kPKIndexName);
	}

	// Update indexes fields refs
	for (int idx = 0; idx < payloadType_->NumFields(); idx++) {
		FieldsSet fields = indexes_[idx]->Fields(), newFields;
		int jsonPathIdx = 0;
		for (auto field : fields) {
			if (field == IndexValueType::SetByJsonPath) {
				newFields.push_back(fields.getJsonPath(jsonPathIdx));
				newFields.push_back(fields.getTagsPath(jsonPathIdx));
				jsonPathIdx++;
			} else {
				newFields.push_back(field < fieldIdx ? field : field - 1);
			}
		}
		indexes_[idx]->SetFields(std::move(newFields));
	}

	if (!isComposite(indexToRemove->Type()) && !indexToRemove->Opts().IsSparse()) {
		PayloadType oldPlType = payloadType_;
		payloadType_.Drop(index.name_);
		tagsMatcher_.updatePayloadType(payloadType_);
		FieldsSet changedFields{0, fieldIdx};
		updateItems(oldPlType, changedFields, -1);
	}

	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
	int sortedIdxCount = getSortedIdxCount();
	for (auto &idx : indexes_) idx->SetSortedIdxCount(sortedIdxCount);
}

void Namespace::addIndex(const IndexDef &indexDef) {
	string indexName = indexDef.name_;

	auto idxNameIt = indexesNames_.find(indexName);
	int idxNo = payloadType_->NumFields();
	IndexOpts opts = indexDef.opts_;
	JsonPaths jsonPaths = indexDef.jsonPaths_;
	auto currentPKIndex = indexesNames_.find(kPKIndexName);

	if (idxNameIt != indexesNames_.end()) {
		IndexDef newIndexDef = indexDef;
		IndexDef oldIndexDef = getIndexDefinition(indexName);
		// reset config
		oldIndexDef.opts_.config = "";
		newIndexDef.opts_.config = "";
		if (newIndexDef == oldIndexDef) {
			return;
		} else {
			throw Error(errConflict, "Index '%s.%s' already exists with different settings", name_, indexName);
		}
	}

	// New index case. Just add
	if (currentPKIndex != indexesNames_.end() && opts.IsPK()) {
		throw Error(errConflict, "Can't add PK index '%s.%s'. Already exists another PK index - '%s'", name_, indexName,
					indexes_[currentPKIndex->second]->Name());
	}
	if (opts.IsPK() && opts.IsArray()) {
		throw Error(errParams, "Can't add index '%s' in namespace '%s'. PK field can't be array", indexName, name_);
	}

	if (isComposite(indexDef.Type())) {
		addCompositeIndex(indexDef);
		return;
	}

	auto newIndex = unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet()));
	FieldsSet fields;
	if (opts.IsSparse()) {
		for (const string &jsonPath : jsonPaths) {
			bool updated = false;
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPath, updated);
			assert(tagsPath.size() > 0);

			fields.push_back(jsonPath);
			fields.push_back(tagsPath);
		}

		++sparseIndexesCount_;
		insertIndex(Index::New(indexDef, payloadType_, fields), idxNo, indexName);
	} else {
		PayloadType oldPlType = payloadType_;

		payloadType_.Add(PayloadFieldType(newIndex->KeyType(), indexName, jsonPaths, opts.IsArray()));
		tagsMatcher_.updatePayloadType(payloadType_);
		newIndex->SetFields(FieldsSet{idxNo});
		newIndex->UpdatePayloadType(payloadType_);

		FieldsSet changedFields{0, idxNo};
		insertIndex(newIndex.release(), idxNo, indexName);
		updateItems(oldPlType, changedFields, 1);
	}
	int sortedIdxCount = getSortedIdxCount();
	for (auto &idx : indexes_) idx->SetSortedIdxCount(sortedIdxCount);
}

void Namespace::updateIndex(const IndexDef &indexDef) {
	const string &indexName = indexDef.name_;

	IndexDef foundIndex = getIndexDefinition(indexName);

	if (indexDef.IsEqual(foundIndex, true)) {
		// Index has not been changed
		if (!indexDef.IsEqual(foundIndex, false)) {
			// Only index config changed
			// Just call SetOpts
			indexes_[getIndexByName(indexName)]->SetOpts(indexDef.opts_);
		}
		return;
	}

	dropIndex(indexDef);
	addIndex(indexDef);
}

IndexDef Namespace::getIndexDefinition(const string &indexName) {
	NamespaceDef nsDef = getDefinition();

	auto indexes = nsDef.indexes;
	auto indexDefIt = std::find_if(indexes.begin(), indexes.end(), [&](const IndexDef &idxDef) { return idxDef.name_ == indexName; });
	if (indexDefIt == indexes.end()) {
		throw Error(errParams, "Index '%s' not found in '%s'", indexName, name_);
	}

	return *indexDefIt;
}

void Namespace::addCompositeIndex(const IndexDef &indexDef) {
	string indexName = indexDef.name_;
	IndexType type = indexDef.Type();
	IndexOpts opts = indexDef.opts_;

	FieldsSet fields;

	for (auto &jsonPathOrSubIdx : indexDef.jsonPaths_) {
		auto idxNameIt = indexesNames_.find(jsonPathOrSubIdx);
		if (idxNameIt == indexesNames_.end()) {
			bool updated = false;
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPathOrSubIdx, updated);
			if (tagsPath.empty()) {
				throw Error(errParams, "Subindex '%s' for composite index '%s' does not exist", jsonPathOrSubIdx, indexName);
			}
			fields.push_back(tagsPath);
			fields.push_back(jsonPathOrSubIdx);
		} else if (indexes_[idxNameIt->second]->Opts().IsSparse() && !indexes_[idxNameIt->second]->Opts().IsArray()) {
			fields.push_back(jsonPathOrSubIdx);
			fields.push_back(indexes_[idxNameIt->second]->Fields().getTagsPath(0));
		} else {
			if (indexes_[idxNameIt->second]->Opts().IsArray() && (type == IndexCompositeBTree || type == IndexCompositeHash)) {
				throw Error(errParams, "Can't add array subindex '%s' to composite index '%s'", jsonPathOrSubIdx, indexName);
			}
			fields.push_back(idxNameIt->second);
		}
	}

	assert(fields.getJsonPathsLength() == fields.getTagsPathsLength());
	assert(indexesNames_.find(indexName) == indexesNames_.end());

	int idxPos = indexes_.size();
	insertIndex(Index::New(indexDef, payloadType_, fields), idxPos, indexName);

	for (IdType rowId = 0; rowId < int(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			indexes_[idxPos]->Upsert(Variant(items_[rowId]), rowId);
		}
	}
	int sortedIdxCount = getSortedIdxCount();
	for (auto &idx : indexes_) idx->SetSortedIdxCount(sortedIdxCount);
}

void Namespace::insertIndex(Index *newIndex, int idxNo, const string &realName) {
	indexes_.insert(indexes_.begin() + idxNo, unique_ptr<Index>(newIndex));

	for (auto &n : indexesNames_) {
		if (n.second >= idxNo) {
			n.second++;
		}
	}

	indexesNames_.insert({realName, idxNo});

	if (newIndex->Opts().IsPK()) {
		indexesNames_.insert({kPKIndexName, idxNo});
	}
}

int Namespace::getIndexByName(const string &index) const {
	auto idxIt = indexesNames_.find(index);

	if (idxIt == indexesNames_.end()) throw Error(errParams, "Index '%s' not found in '%s'", index, name_);

	return idxIt->second;
}

bool Namespace::getIndexByName(const string &name, int &index) const {
	auto it = indexesNames_.find(name);
	if (it == indexesNames_.end()) return false;
	index = it->second;
	return true;
}

void Namespace::Insert(Item &item, bool store) { modifyItem(item, store, ModeInsert); }

void Namespace::Update(Item &item, bool store) { modifyItem(item, store, ModeUpdate); }

void Namespace::Upsert(Item &item, bool store) { modifyItem(item, store, ModeUpsert); }

void Namespace::Delete(Item &item, bool noLock) {
	ItemImpl *ritem = item.impl_;
	string jsonSliceBuf;

	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	WLock lock(mtx_, defer_lock);

	if (!noLock) {
		cancelCommit_ = true;
		lock.lock();
		cancelCommit_ = false;
	}
	calc.LockHit();

	if (item.GetLSN() == -1) {
		if (repl_.slaveMode) throw Error(errLogic, "Can't modify slave ns '%s'", name_);
	} else {
		// Check if item is from replicator, then slaveMode must be on
		// assert(repl_.slaveMode);
	}

	updateTagsMatcherFromItem(ritem, jsonSliceBuf);

	auto itItem = findByPK(ritem);
	IdType id = itItem.first;

	if (!itItem.second) {
		return;
	}

	item.setID(id);

	WrSerializer ser;
	WALRecord wrec(WalItemModify, ritem->GetCJSON(), ritem->tagsMatcher().version(), ModeDelete);
	doDelete(id);
	int64_t lsn = item.GetLSN();

	if (repl_.slaveMode) {
		if (repl_.lastLsn >= lsn) {
			logPrintf(LogError, "[repl:%s] Namespace::Delete lsn = %ld lastLsn = %ld ", name_, lsn, repl_.lastLsn);
		}
	} else {
		lsn = wal_.Add(wrec);
		item.setLSN(lsn);
	}
	observers_.OnWALUpdate(lsn, name_, wrec);
}

void Namespace::doDelete(IdType id) {
	assert(items_.exists(id));

	Payload pl(payloadType_, items_[id]);

	WrSerializer pk;
	pk << kStorageItemPrefix;
	pl.SerializeFields(pk, pkFields());

	repl_.dataHash ^= pl.GetHash();
	if (!repl_.slaveMode) wal_.Set(WALRecord(), items_[id].GetLSN());

	if (storage_) {
		std::unique_lock<std::mutex> lock(storage_mtx_);
		updates_->Remove(pk.Slice());
		++unflushedCount_;
	}

	// erase last item
	VariantArray skrefs;
	int field;

	// erase from composite indexes
	for (field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Delete(Variant(items_[id]), id);
	}

	// Holder for tuple. It is required for sparse indexes will be valid
	VariantArray tupleHolder(pl.Get(0, skrefs));

	// Deleteing fields from dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assert(indexes_.firstCompositePos() != 0);
	const int borderIdx = indexes_.totalSize() > 1 ? 1 : 0;
	field = borderIdx;
	do {
		field %= indexes_.firstCompositePos();

		Index &index = *indexes_[field];
		if (index.Opts().IsSparse()) {
			assert(index.Fields().getTagsPathsLength() > 0);
			pl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else {
			pl.Get(field, skrefs, index.Opts().IsArray());
		}
		// Delete value from index
		for (auto key : skrefs) index.Delete(key, id);
		// If no krefs delete empty value from index
		if (!skrefs.size()) index.Delete(Variant(), id);
	} while (++field != borderIdx);

	// free PayloadValue
	items_[id].Free();
	markUpdated();
	free_.push_back(id);
	if (free_.size() == items_.size()) {
		free_.resize(0);
		items_.resize(0);
	}
}

void Namespace::Delete(const Query &q, QueryResults &result) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	WLock lock(mtx_);
	calc.LockHit();

	NsSelecter selecter(this);
	SelectCtx ctx(q);
	selecter(result, ctx);
	result.lockResults();

	auto tmStart = high_resolution_clock::now();
	for (auto &r : result.Items()) {
		doDelete(r.id);
	}

	WrSerializer ser;

	WALRecord wrec(WalUpdateQuery, q.GetSQL(ser).Slice());

	int64_t lsn = repl_.slaveMode ? -1 : wal_.Add(wrec);

	observers_.OnWALUpdate(lsn, name_, wrec);

	if (q.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

ReplicationState Namespace::GetReplState() {
	RLock lck(mtx_);
	return repl_;
}

void Namespace::SetSlaveLSN(int64_t slaveLsn) {
	assert(repl_.slaveMode);
	WLock lck(mtx_);
	repl_.lastLsn = slaveLsn;
	unflushedCount_++;
}

void Namespace::ApplyTransactionStep(TransactionStep &step) {
	if (step.status_ == ModeDelete) {
		Delete(step.item_, true);
	} else {
		modifyItem(step.item_, true, step.status_, true);
	}
}

void Namespace::StartTransaction() {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	cancelCommit_ = true;
	mtx_.lock();
	cancelCommit_ = false;
	calc.LockHit();
}
void Namespace::EndTransaction() { mtx_.unlock(); }

void Namespace::doUpsert(ItemImpl *ritem, IdType id, bool doUpdate) {
	// Upsert fields to indexes
	assert(items_.exists(id));
	auto &plData = items_[id];

	// Inplace payload
	Payload pl(payloadType_, plData);
	Payload plNew = ritem->GetPayload();
	if (doUpdate) {
		repl_.dataHash ^= pl.GetHash();
		plData.Clone(pl.RealSize());
	}

	// keep them in nsamespace, to prevent allocs
	// VariantArray krefs, skrefs;

	// Delete from composite indexes first
	if (doUpdate) {
		for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
			indexes_[field]->Delete(Variant(plData), id);
		}
	}

	plData.SetLSN(ritem->Value().GetLSN());

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
				pl.Get(field, krefs, index.Opts().IsArray());
			}
			for (auto key : krefs) index.Delete(key, id);
			if (!krefs.size()) index.Delete(Variant(), id);
		}
		// Put value to index
		krefs.resize(0);
		krefs.reserve(skrefs.size());
		for (auto key : skrefs) krefs.push_back(index.Upsert(key, id));

		// Put value to payload
		if (!isIndexSparse) pl.Set(field, krefs);
		// If no krefs doUpsert empty value to index
		if (!skrefs.size()) index.Upsert(Variant(), id);
	} while (++field != borderIdx);

	// Upsert to composite indexes
	for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Upsert(Variant(plData), id);
	}
	repl_.dataHash ^= pl.GetHash();
}

void Namespace::updateTagsMatcherFromItem(ItemImpl *ritem, string &jsonSliceBuf) {
	if (ritem->tagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name_, ritem->tagsMatcher().dump());
	}

	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		jsonSliceBuf = ritem->GetJSON().ToString();
		logPrintf(LogTrace, "Conflict TagsMatcher of namespace '%s' on modify: item:\n%s\ntm is\n%s\nnew tm is\n %s\n", name_, jsonSliceBuf,
				  tagsMatcher_.dump(), ritem->tagsMatcher().dump());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Value().SetLSN(ritem->Value().GetLSN());
		// tmpItem.Unsafe(true);
		*ritem = std::move(tmpItem);

		auto err = ritem->FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) throw err;

		if (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))
			throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
	if (ritem->tagsMatcher().isUpdated()) {
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
}

void Namespace::modifyItem(Item &item, bool store, int mode, bool noLock) {
	if (item.GetLSN() == -1) {
		if (repl_.slaveMode) throw Error(errLogic, "Can't modify slave ns '%s'", name_);
	} else {
		// Check if item is from replicator, then slaveMode must be on
		// assertf(repl_.slaveMode, "Can't apply item with lsn %ld to non slave ns '%s'", long(item.GetLSN()), name_.c_str());
	}

	// Item to doUpsert
	ItemImpl *itemImpl = item.impl_;
	string jsonSlice;
	WLock lock(mtx_, defer_lock);
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	if (!noLock) {
		cancelCommit_ = true;
		lock.lock();
		cancelCommit_ = false;
	}
	calc.LockHit();

	updateTagsMatcherFromItem(itemImpl, jsonSlice);
	auto newPl = itemImpl->GetPayload();

	auto realItem = findByPK(itemImpl);
	bool exists = realItem.second;

	if ((exists && mode == ModeInsert) || (!exists && mode == ModeUpdate)) {
		item.setID(-1);
		return;
	}

	IdType id = exists ? realItem.first : createItem(newPl.RealSize());
	setFieldsBasedOnPrecepts(itemImpl);

	int64_t lsn = item.GetLSN();
	if (repl_.slaveMode) {
		if (repl_.lastLsn >= lsn)
			logPrintf(LogError, "[repl:%s] Namespace::modifyItem lsn = %ld lastLsn = %ld ", name_, lsn, repl_.lastLsn);
	} else {
		lsn = wal_.Add(WALRecord(WalItemUpdate, id), exists ? items_[id].GetLSN() : -1);
	}

	bool emptyAfterReload = (items_.empty() && !storageLoaded_);
	if (!emptyAfterReload) {
		item.setLSN(lsn);
		item.setID(id);
		doUpsert(itemImpl, id, exists);
	}

	if (storage_ && store) {
		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			tagsMatcher_.serialize(ser);
			tagsMatcher_.clearUpdated();
			writeToStorage(string_view(kStorageTagsPrefix), ser.Slice());
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
		}

		WrSerializer pk, data;
		pk << kStorageItemPrefix;
		newPl.SerializeFields(pk, pkFields());
		data.PutUInt64(lsn);
		itemImpl->GetCJSON(data);
		writeToStorage(pk.Slice(), data.Slice());
		++unflushedCount_;
	}

	observers_.OnModifyItem(lsn, name_, item.impl_, mode);

	markUpdated();
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> Namespace::findByPK(ItemImpl *ritem) {
	auto pkIndexIt = indexesNames_.find(kPKIndexName);

	if (pkIndexIt == indexesNames_.end()) {
		throw Error(errLogic, "Trying to modify namespace '%s', but it doesn't contain PK index", name_);
	}
	Index *pkIndex = indexes_[pkIndexIt->second].get();

	Payload pl = ritem->GetPayload();
	// It is a faster alternative of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	VariantArray krefs;
	if (isComposite(pkIndex->Type())) {
		krefs.push_back(Variant(*pl.Value()));
	} else if (pkIndex->Opts().IsSparse()) {
		auto f = pkIndex->Fields();
		pl.GetByJsonPath(f.getTagsPath(0), krefs, pkIndex->KeyType());
	} else
		pl.Get(pkIndexIt->second, krefs);
	assertf(krefs.size() == 1, "Pkey field must contain 1 key, but there '%d' in '%s.%s'", int(krefs.size()), name_.c_str(),
			pkIndex->Name().c_str());

	IdSetRef ids = pkIndex->Find(krefs[0]);

	if (ids.size()) return {ids[0], true};
	return {-1, false};
}

void Namespace::commitIndexes() {
	// This is read lock only atomics based implementation of rebuild indexes
	// If sortOrdersBuilt_ is true, then indexes are completely built
	// In this case reset sortOrdersBuilt_ to false and/or any idset's and sort orders builds are allowed only protected by write lock
	if (sortOrdersBuilt_) return;
	int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	if (!lastUpdateTime_ || now - lastUpdateTime_ < 300) {
		return;
	}
	if (!indexes_.size()) {
		return;
	}

	RLock lck(mtx_);
	if (sortOrdersBuilt_ || cancelCommit_) return;

	logPrintf(LogTrace, "Namespace::commitIndexes(%s) enter", name_);
	assert(indexes_.firstCompositePos() != 0);
	int field = indexes_.firstCompositePos();
	do {
		field %= indexes_.totalSize();
		PerfStatCalculatorST calc(indexes_[field]->GetCommitPerfCounter(), enablePerfCounters_);
		calc.LockHit();
		indexes_[field]->Commit();
	} while (++field != indexes_.firstCompositePos() && !cancelCommit_);

	// Update sort orders and sort_id for each index

	int i = 1;
	for (auto &idxIt : indexes_) {
		if (idxIt->IsOrdered()) {
			NSUpdateSortedContext sortCtx(*this, i++);
			idxIt->MakeSortOrders(sortCtx);
			// Build in multiple threads
			int maxIndexWorkers = std::thread::hardware_concurrency();
			// if (maxIndexWorkers > 4) maxIndexWorkers = 4;
			unique_ptr<thread[]> thrs(new thread[maxIndexWorkers]);
			auto indexes = &this->indexes_;

			for (int i = 0; i < maxIndexWorkers; i++) {
				thrs[i] = std::thread(
					[&](int i) {
						for (int j = i; j < int(indexes->size()) && !cancelCommit_; j += maxIndexWorkers)
							indexes->at(j)->UpdateSortedIds(sortCtx);
					},
					i);
			}
			for (int i = 0; i < maxIndexWorkers; i++) thrs[i].join();
		}
		if (cancelCommit_) break;
	}
	sortOrdersBuilt_ = !cancelCommit_;
	if (!cancelCommit_) lastUpdateTime_ = 0;
	logPrintf(LogTrace, "Namespace::commitIndexes(%s) leave %s", name_, cancelCommit_ ? "(cancelled by concurent update)" : "");
}

void Namespace::markUpdated() {
	sortOrdersBuilt_ = false;
	queryCache_->Clear();
	joinCache_->Clear();
	lastUpdateTime_ = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void Namespace::updateSelectTime() {
	lastSelectTime_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int64_t Namespace::getLastSelectTime() const { return lastSelectTime_; }

void Namespace::Select(QueryResults &result, SelectCtx &params) {
	if (params.query.entries.size() == 1 && params.query.entries[0].index == kLSNIndexName) {
		WALSelecter selecter(this);
		selecter(result, params);
	} else {
		NsSelecter selecter(this);
		selecter(result, params);
	}
}

NamespaceDef Namespace::getDefinition() {
	auto pt = this->payloadType_;
	NamespaceDef nsDef(name_, StorageOpts().Enabled(!dbpath_.empty()));

	nsDef.indexes.reserve(indexes_.size());
	for (int i = 1; i < int(indexes_.size()); i++) {
		IndexDef indexDef;
		const Index &index = *indexes_[i];
		indexDef.name_ = index.Name();
		indexDef.opts_ = index.Opts();
		indexDef.FromType(index.Type());

		if (index.Opts().IsSparse() || i >= payloadType_.NumFields()) {
			int fIdx = 0;
			for (auto &f : index.Fields()) {
				if (f != IndexValueType::SetByJsonPath) {
					indexDef.jsonPaths_.push_back(indexes_[f]->Name());
				} else {
					indexDef.jsonPaths_.push_back(index.Fields().getJsonPath(fIdx++));
				}
			}
		} else {
			indexDef.jsonPaths_ = payloadType_->Field(i).JsonPaths();
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
	*(static_cast<ReplicationState *>(&ret.replication)) = repl_;
	ret.replication.walCount = wal_.size();
	ret.replication.walSize = wal_.heap_size();
	if (!repl_.slaveMode) ret.replication.lastLsn = wal_.LSNCounter() - 1;

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
	ret.storageLoaded = storageLoaded_.load();
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
		throw Error(errNotValid, "Error load namespace from storage '%s': %s", name_, status.what());
	}

	if (def.size()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logPrintf(LogTrace, "Loaded tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
	}

	def.clear();
	status = storage_->Read(StorageOpts().FillCache(), string_view(kStorageIndexesPrefix), def);

	if (!status.ok() && status.code() != errNotFound) {
		throw Error(errNotValid, "Error load namespace from storage '%s': %s", name_, status.what());
	}

	if (def.size()) {
		Serializer ser(def.data(), def.size());
		const uint32_t dbMagic = ser.GetUInt32();
		const uint32_t dbVer = ser.GetUInt32();
		if (dbMagic != kStorageMagic) {
			logPrintf(LogError, "Storage magic mismatch. want %08X, got %08X", kStorageMagic, dbMagic);
			return false;
		}
		if (dbVer != kStorageVersion) {
			logPrintf(LogError, "Storage version mismatch. want %08X, got %08X", kStorageVersion, dbVer);
			return false;
		}

		int count = ser.GetVarUint();
		while (count--) {
			IndexDef indexDef;
			string indexData = ser.GetVString().ToString();
			Error err = indexDef.FromJSON(const_cast<char *>(indexData.c_str()));
			if (!err.ok()) throw err;

			addIndex(indexDef);
		}
	}

	logPrintf(LogTrace, "Loaded index structure of namespace '%s'\n%s", name_, payloadType_->ToString());

	return true;
}

void Namespace::loadReplStateFromStorage() {
	string json;
	Error status = storage_->Read(StorageOpts().FillCache(), string_view(kStorageReplStatePrefix), json);
	if (!status.ok() && status.code() != errNotFound) {
		throw Error(errNotValid, "Error load replication state from storage '%s': %s", name_, status.what());
	}

	if (json.size()) {
		logPrintf(LogTrace, "Loading replication state of namespace %s: %s", name_, json);
		repl_.FromJSON(const_cast<char *>(json.c_str()));
	}
}

void Namespace::saveIndexesToStorage() {
	// clear ItemImpl pool on payload change
	pool_.clear();

	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveIndexesToStorage (%s)", name_);

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

	saveReplStateToStorage();
}

void Namespace::saveReplStateToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveReplStateToStorage (%s)", name_);

	WrSerializer ser;
	JsonBuilder builder(ser);
	repl_.GetJSON(builder);
	builder.End();

	storage_->Write(StorageOpts().FillCache(), string_view(kStorageReplStatePrefix),
					string_view(reinterpret_cast<const char *>(ser.Buf()), ser.Len()));
}

bool Namespace::needToLoadData() const { return (storage_ && (dbpath_.length() > 0)) ? !storageLoaded_.load() : false; }

void Namespace::EnableStorage(const string &path, StorageOpts opts) {
	string dbpath = fs::JoinPath(path, name_);
	datastorage::StorageType storageType = datastorage::StorageType::LevelDB;

	WLock lock(mtx_);
	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", name_, path);
	}

	bool success = false;
	while (!success) {
		storage_.reset(datastorage::StorageFactory::create(storageType));
		Error status = storage_->Open(dbpath, opts);
		if (!status.ok()) {
			if (!opts.IsDropOnFileFormatError()) {
				storage_ = nullptr;
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s' - %s", name_, path, status.what());
			}
		} else {
			success = loadIndexesFromStorage();
			if (!success && !opts.IsDropOnFileFormatError()) {
				storage_ = nullptr;
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s': format error", name_, dbpath);
			}
			loadReplStateFromStorage();
		}
		if (!success && opts.IsDropOnFileFormatError()) {
			logPrintf(LogWarning, "Dropping storage for namespace '%s' on path '%s' due to format error", name_, dbpath);
			opts.DropOnFileFormatError(false);
			storage_->Destroy(dbpath);
			storage_ = nullptr;
		}
	}

	storageOpts_ = opts;
	updates_.reset(storage_->GetUpdatesCollection());
	dbpath_ = dbpath;
}

StorageOpts Namespace::getStorageOpts() {
	shared_lock<shared_timed_mutex> lk(mtx_);
	return storageOpts_;
}

void Namespace::SetStorageOpts(StorageOpts opts) {
	shared_lock<shared_timed_mutex> lk(mtx_);
	if (opts.IsSlaveMode()) {
		storageOpts_.SlaveMode();
		repl_.slaveMode = true;
	}
}

void Namespace::LoadFromStorage() {
	WLock lock(mtx_);

	StorageOpts opts;
	opts.FillCache(false);
	size_t ldcount = 0;
	logPrintf(LogTrace, "Loading items to '%s' from storage", name_);
	unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	ItemImpl item(payloadType_, tagsMatcher_);
	item.Unsafe(true);
	int errCount = 0;
	int64_t maxLSN = -1;
	Error lastErr = errOK;

	uint64_t dataHash = repl_.dataHash;
	repl_.dataHash = 0;
	for (dbIter->Seek(kStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), string_view(kStorageItemPrefix "\xFF")) < 0; dbIter->Next()) {
		string_view dataSlice = dbIter->Value();
		if (dataSlice.size() > 0) {
			if (!pkFields().size()) {
				throw Error(errLogic, "Can't load data storage of '%s' - there are no PK fields in ns", name_);
			}
			if (dataSlice.size() < sizeof(int64_t)) {
				lastErr = Error(errParseBin, "Not enougth data in data slice");
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", name_, lastErr.what());
				errCount++;
				continue;
			}

			// Read LSN
			int64_t lsn = *reinterpret_cast<const int64_t *>(dataSlice.data());
			assert(lsn >= 0);
			maxLSN = std::max(maxLSN, lsn);
			dataSlice = dataSlice.substr(sizeof(lsn));

			auto err = item.FromCJSON(dataSlice);
			if (!err.ok()) {
				logPrintf(LogTrace, "Error load item to '%s' from storage: '%s'", name_, err.what());
				errCount++;
				lastErr = err;
				continue;
			}

			IdType id = items_.size();
			items_.emplace_back(PayloadValue(item.GetPayload().RealSize()));
			item.Value().SetLSN(lsn);
			doUpsert(&item, id, false);
			ldcount += dataSlice.size();
		}
	}
	if (!repl_.slaveMode) initWAL(maxLSN + 1);

	logPrintf(LogInfo, "[%s] Done loading storage. %d items loaded (%d errors %s), lsn #%ld%s, total size=%dM, dataHash=%ld", name_,
			  items_.size(), errCount, lastErr.what(), repl_.lastLsn, repl_.slaveMode ? " (slave)" : "", ldcount / (1024 * 1024),
			  repl_.dataHash);
	storageLoaded_ = true;
	if (dataHash != repl_.dataHash) {
		logPrintf(LogWarning, "[%s] Warning dataHash mismatch %lu != %lu", name_, dataHash, repl_.dataHash);
		unflushedCount_++;
	}

	markUpdated();
}

void Namespace::initWAL(int64_t maxLSN) {
	// Fill wall
	wal_.Init(maxLSN, storage_);

	// Fill existing records
	for (IdType rowId = 0; rowId < IdType(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			wal_.Set(WALRecord(WalItemUpdate, rowId), items_[rowId].GetLSN());
		}
	}
	repl_.lastLsn = wal_.LSNCounter() - 1;
}

void Namespace::BackgroundRoutine() {
	flushStorage();
	commitIndexes();
}

void Namespace::flushStorage() {
	RLock rlock(mtx_);
	if (storage_) {
		if (unflushedCount_) {
			unflushedCount_ = 0;
			std::unique_lock<std::mutex> lck(storage_mtx_);
			Error status = storage_->Write(StorageOpts().FillCache(), *(updates_.get()));
			if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage: %s", name_, status.what());
			updates_->Clear();
			saveReplStateToStorage();
		}
	}
}

void Namespace::DeleteStorage() {
	WLock lck(mtx_);
	if (storage_) {
		storage_->Destroy(dbpath_);
		dbpath_.clear();
		storage_.reset();
	}
}

void Namespace::CloseStorage() {
	flushStorage();
	WLock lck(mtx_);
	dbpath_.clear();
	storage_.reset();
}

bool Namespace::tryToReload() {
	uint16_t noQueryIdleThresholdSec = getStorageOpts().noQueryIdleThresholdSec;
	if (noQueryIdleThresholdSec > 0) {
		int64_t now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
		if ((now - getLastSelectTime()) > noQueryIdleThresholdSec) {
			unique_lock<shared_timed_mutex> lk(mtx_);
			items_.clear();
			for (auto it = indexesNames_.begin(); it != indexesNames_.end();) {
				payloadType_.Drop(it->first);
				it = indexesNames_.erase(it);
			}
			indexes_.clear();
			free_.clear();
			IndexDef tupleIndexDef(kTupleName, {}, IndexStrStore, IndexOpts());
			addIndex(tupleIndexDef);
			loadIndexesFromStorage();
			updateSelectTime();
			lk.unlock();

			logPrintf(LogInfo, "NS reloaded: %s", GetName());
			storageLoaded_ = false;
			return true;
		}
	}
	return true;
}

Item Namespace::NewItem() {
	WLock lock(mtx_);
	if (pool_.size()) {
		ItemImpl *impl = pool_.back().release();
		pool_.pop_back();
		impl->Clear(tagsMatcher_);
		return Item(impl);
	}
	return Item(new ItemImpl(payloadType_, tagsMatcher_, pkFields()));
}
void Namespace::ToPool(ItemImpl *item) {
	WLock lck(mtx_);
	item->Clear(tagsMatcher_);
	if (pool_.size() < 1024)
		pool_.push_back(std::unique_ptr<ItemImpl>(item));
	else
		delete item;
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

	WALRecord wrec(WalPutMeta, key, data);

	int64_t lsn = repl_.slaveMode ? -1 : wal_.Add(wrec);
	observers_.OnWALUpdate(lsn, name_, wrec);
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
		id = free_.back();
		free_.pop_back();
		assert(id < IdType(items_.size()));
		assert(items_[id].IsFree());
		items_[id] = PayloadValue(realSize);
	} else {
		id = items_.size();
		items_.emplace_back(PayloadValue(realSize));
	}
	return id;
}

void Namespace::setFieldsBasedOnPrecepts(ItemImpl *ritem) {
	for (auto &precept : ritem->GetPrecepts()) {
		SelectFuncParser sqlFunc;
		SelectFuncStruct sqlFuncStruct = sqlFunc.Parse(precept);

		VariantArray krs;
		Variant field = ritem->GetPayload().Get(sqlFuncStruct.field, krs)[0];

		Variant value(make_key_string(sqlFuncStruct.value));

		if (sqlFuncStruct.isFunction) {
			if (sqlFuncStruct.funcName == "now") {
				string mode = "sec";
				if (sqlFuncStruct.funcArgs.size() && !sqlFuncStruct.funcArgs.front().empty()) {
					mode = sqlFuncStruct.funcArgs.front();
				}
				value = Variant(getTimeNow(mode));
			} else if (sqlFuncStruct.funcName == "serial") {
				value = Variant(funcGetSerial(sqlFuncStruct));
			} else {
				throw Error(errParams, "Unknown function %s", sqlFuncStruct.field);
			}
		}

		value.convert(field.Type());
		VariantArray refs{value};

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
	result.addNSContext(payloadType_, tagsMatcher_, FieldsSet(tagsMatcher_, selectFilter));
	for (auto &id : *ids) {
		result.Add({id, items_[id], 0, 0});
	}
}

void Namespace::GetFromJoinCache(JoinCacheRes &ctx) {
	if (config_.cacheMode == CacheModeOff || !sortOrdersBuilt_) return;
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
	if (config_.cacheMode != CacheModeAggressive || !sortOrdersBuilt_) return;
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

const FieldsSet &Namespace::pkFields() {
	auto it = indexesNames_.find(kPKIndexName);
	if (it != indexesNames_.end()) {
		return indexes_[it->second]->Fields();
	}

	static FieldsSet ret;
	return ret;
}

}  // namespace reindexer
