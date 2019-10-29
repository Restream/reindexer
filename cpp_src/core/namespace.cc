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
#include "core/payload/payloadiface.h"
#include "core/query/expressionevaluator.h"
#include "core/rdxcontext.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/storage/storagefactory.h"
#include "itemimpl.h"
#include "replicator/updatesobserver.h"
#include "replicator/walselecter.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
#include "tools/timetools.h"

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::make_shared;
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
const int kWALStatementItemsThreshold = 5;

#define kStorageMagic 0x1234FEDC
#define kStorageVersion 0x8

namespace reindexer {

constexpr int64_t kStorageSerialInitial = 1;
constexpr uint8_t kSysRecordsBackupCount = 8;
constexpr uint8_t kSysRecordsFirstWriteCopies = 3;

Namespace::IndexesStorage::IndexesStorage(const Namespace &ns) : ns_(ns) {}

void Namespace::IndexesStorage::MoveBase(IndexesStorage &&src) { Base::operator=(move(src)); }

// private implementation and NOT THREADSAFE of copy CTOR
// use 'Namespace::Clone(Namespace& ns)'
Namespace::Namespace(const Namespace &src) : indexes_(*this), observers_(src.observers_), lastSelectTime_(0), cancelCommit_(false) {
	CopyContentsFrom(src);
}

Namespace::Namespace(const string &name, UpdatesObservers &observers)
	: indexes_(*this),
	  name_(name),
	  payloadType_(name),
	  tagsMatcher_(payloadType_),
	  unflushedCount_{0},
	  sortOrdersBuilt_(false),
	  queryCache_(make_shared<QueryCache>()),
	  joinCache_(make_shared<JoinCache>()),
	  enablePerfCounters_(false),
	  observers_(&observers),
	  storageLoaded_(false),
	  lastSelectTime_(0),
	  cancelCommit_(false),
	  lastUpdateTime_(0),
	  itemsCount_(0) {
	logPrintf(LogTrace, "Namespace::Namespace (%s)", name_);
	items_.reserve(10000);

	// Add index and payload field for tuple of non indexed fields
	IndexDef tupleIndexDef(kTupleName, {}, IndexStrStore, IndexOpts());
	addIndex(tupleIndexDef);
	updateSelectTime();
}

void Namespace::CopyContentsFrom(const Namespace &src) {
	indexesNames_ = src.indexesNames_;
	items_ = src.items_;
	free_ = src.free_;
	name_ = src.name_;
	payloadType_ = src.payloadType_;
	tagsMatcher_ = src.tagsMatcher_;
	storage_ = src.storage_;
	updates_ = src.updates_;
	unflushedCount_.store(src.unflushedCount_.load(std::memory_order_acquire), std::memory_order_release);  // 0
	sortOrdersBuilt_ = src.sortOrdersBuilt_.load();															// false
	meta_ = src.meta_;
	dbpath_ = src.dbpath_;
	queryCache_ = src.queryCache_;
	joinCache_ = src.joinCache_;

	enablePerfCounters_ = src.enablePerfCounters_.load();
	config_ = src.config_;
	wal_ = src.wal_;
	repl_ = src.repl_;
	storageLoaded_ = src.storageLoaded_.load();
	lastUpdateTime_.store(src.lastUpdateTime_.load(std::memory_order_acquire), std::memory_order_release);
	itemsCount_ = src.itemsCount_.load();
	sparseIndexesCount_ = src.sparseIndexesCount_;
	krefs = src.krefs;
	skrefs = src.skrefs;

	storageOpts_ = src.storageOpts_;
	for (auto &idxIt : src.indexes_) indexes_.push_back(unique_ptr<Index>(idxIt->Clone()));
	logPrintf(LogTrace, "Namespace::CopyContentsFrom (%s)", name_);
}

Namespace::~Namespace() {
	const RdxContext dummyCtx;
	flushStorage(dummyCtx);
	WLock wlock(mtx_, &dummyCtx);
	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items", name_, items_.size());
}

void Namespace::onConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx) {
	NamespaceConfigData configData;
	configProvider.GetNamespaceConfig(GetName(), configData);
	ReplicationConfigData replicationConf = configProvider.GetReplicationConfig();

	enablePerfCounters_ = configProvider.GetProfilingConfig().perfStats;

	WLock lk(mtx_, &ctx);
	config_ = configData;
	storageOpts_.LazyLoad(configData.lazyLoad);
	storageOpts_.noQueryIdleThresholdSec = configData.noQueryIdleThreshold;

	updateSortedIdxCount();

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

	if (repl_.slaveMode && replicationConf.role != ReplicationSlave) {
		// CASE2: Replication enabled in state, but disabled in config
		// switch slave -> master
		repl_.slaveMode = false;
		initWAL(repl_.lastLsn);
		logPrintf(LogInfo, "Disable slave mode for namespace '%s'", name_);
	} else if (!repl_.slaveMode && replicationConf.role == ReplicationSlave) {
		// CASE3: Replication enabled in config, but disabled in state
		// switch master -> slave
		if (storageOpts_.IsSlaveMode()) {
			repl_.slaveMode = true;
			logPrintf(LogInfo, "Enable slave mode for namespace '%s'", name_);
			repl_.incarnationCounter++;
		}
		if (storageOpts_.IsTemporary()) {
			repl_.temporary = true;
			logPrintf(LogInfo, "Marking namespace '%s' as temporary", name_);
			repl_.incarnationCounter++;
		}
	}

	unflushedCount_.fetch_add(1, std::memory_order_release);
	logPrintf(LogInfo, "Replication role changed '%s' %d", name_, replicationConf.role);
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

	VariantArray krefs, skrefsDel, skrefsUps;
	ItemImpl newItem(payloadType_, tagsMatcher_);
	newItem.Unsafe(true);
	int errCount = 0;
	Error lastErr = errOK;
	repl_.dataHash = 0;
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
		plNew.SetLSN(plCurr.GetLSN());
		Payload newValue(payloadType_, plNew);

		for (auto fieldIdx : changedFields) {
			auto &index = *indexes_[fieldIdx];
			if ((fieldIdx == 0) || deltaFields <= 0) {
				oldValue.Get(fieldIdx, skrefsDel, true);
				for (auto key : skrefsDel) index.Delete(key, rowId);
				if (skrefsDel.empty()) index.Delete(Variant(), rowId);
			}

			if ((fieldIdx == 0) || deltaFields >= 0) {
				newItem.GetPayload().Get(fieldIdx, skrefsUps);
				krefs.resize(0);
				for (auto key : skrefsUps) krefs.push_back(index.Upsert(key, rowId));

				newValue.Set(fieldIdx, krefs);
				if (krefs.empty()) index.Upsert(Variant(), rowId);
			}
		}

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			indexes_[fieldIdx]->Upsert(Variant(plNew), rowId);
		}

		plCurr = std::move(plNew);
		repl_.dataHash ^= Payload(payloadType_, plCurr).GetHash();
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
	observers_->OnWALUpdate(lsn, name_, wrec);
}

void Namespace::AddIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	WLock wlock(mtx_, &ctx);
	addIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexAdd);
}

void Namespace::UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	WLock wlock(mtx_, &ctx);
	updateIndex(indexDef);
	saveIndexesToStorage();
	addToWAL(indexDef, WalIndexUpdate);
}

void Namespace::DropIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	WLock wlock(mtx_, &ctx);
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
	for (const unique_ptr<Index> &idx : indexes_) {
		FieldsSet fields = idx->Fields(), newFields;
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
		idx->SetFields(std::move(newFields));
	}

	if (!isComposite(indexToRemove->Type()) && !indexToRemove->Opts().IsSparse()) {
		PayloadType oldPlType = payloadType_;
		payloadType_.Drop(index.name_);
		tagsMatcher_.UpdatePayloadType(payloadType_);
		FieldsSet changedFields{0, fieldIdx};
		updateItems(oldPlType, changedFields, -1);
	}

	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
	updateSortedIdxCount();
}

static void verifyConvertTypes(KeyValueType from, KeyValueType to, const PayloadType &payloadType, const FieldsSet &fields) {
	static const std::string defaultStringValue;
	Variant value;
	switch (from) {
		case KeyValueInt64:
			value = Variant(int64_t(0));
			break;
		case KeyValueDouble:
			value = Variant(0.0);
			break;
		case KeyValueString:
			value = Variant(defaultStringValue);
			break;
		case KeyValueBool:
			value = Variant(false);
			break;
		case KeyValueNull:
			break;
		case KeyValueInt:
			value = Variant(0);
			break;
		default:
			if (to != from) throw Error(errParams, "Cannot convert key value types");
	}
	value.convert(to, &payloadType, &fields);
}

void Namespace::verifyUpdateIndex(const IndexDef &indexDef) const {
	const auto idxNameIt = indexesNames_.find(indexDef.name_);
	const auto currentPKIt = indexesNames_.find(kPKIndexName);

	if (idxNameIt == indexesNames_.end()) {
		throw Error(errParams, "Cannot update index %s: doesn't exist", indexDef.name_);
	}
	const auto &oldIndex = indexes_[idxNameIt->second];
	if (indexDef.opts_.IsPK() && !oldIndex->Opts().IsPK() && currentPKIt != indexesNames_.end()) {
		throw Error(errConflict, "Can't add PK index '%s.%s'. Already exists another PK index - '%s'", name_, indexDef.name_,
					indexes_[currentPKIt->second]->Name());
	}
	if (indexDef.opts_.IsArray() != oldIndex->Opts().IsArray()) {
		throw Error(errParams, "Can't update index '%s' in namespace '%s'. Can't convert array index to not array and vice versa",
					indexDef.name_, name_);
	}
	if (indexDef.opts_.IsPK() && indexDef.opts_.IsArray()) {
		throw Error(errParams, "Can't update index '%s' in namespace '%s'. PK field can't be array", indexDef.name_, name_);
	}

	if (isComposite(indexDef.Type())) {
		verifyUpdateCompositeIndex(indexDef);
		return;
	}

	const auto newIndex = unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet()));
	if (indexDef.opts_.IsSparse()) {
		const auto newSparseIndex = std::unique_ptr<Index>(Index::New(indexDef, payloadType_, {}));
	} else {
		FieldsSet changedFields{idxNameIt->second};
		PayloadType newPlType = payloadType_;
		newPlType.Drop(indexDef.name_);
		newPlType.Add(PayloadFieldType(newIndex->KeyType(), indexDef.name_, indexDef.jsonPaths_, indexDef.opts_.IsArray()));
		verifyConvertTypes(oldIndex->KeyType(), newIndex->KeyType(), newPlType, changedFields);
	}
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
		tagsMatcher_.UpdatePayloadType(payloadType_);
		newIndex->SetFields(FieldsSet{idxNo});
		newIndex->UpdatePayloadType(payloadType_);

		FieldsSet changedFields{0, idxNo};
		insertIndex(newIndex.release(), idxNo, indexName);
		updateItems(oldPlType, changedFields, 1);
	}
	updateSortedIdxCount();
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

	verifyUpdateIndex(indexDef);
	dropIndex(indexDef);
	addIndex(indexDef);
}

IndexDef Namespace::getIndexDefinition(const string &indexName) const {
	NamespaceDef nsDef = getDefinition();

	auto indexes = nsDef.indexes;
	auto indexDefIt = std::find_if(indexes.begin(), indexes.end(), [&](const IndexDef &idxDef) { return idxDef.name_ == indexName; });
	if (indexDefIt == indexes.end()) {
		throw Error(errParams, "Index '%s' not found in '%s'", indexName, name_);
	}

	return *indexDefIt;
}

void Namespace::verifyUpdateCompositeIndex(const IndexDef &indexDef) const {
	IndexType type = indexDef.Type();

	for (auto &jsonPathOrSubIdx : indexDef.jsonPaths_) {
		auto idxNameIt = indexesNames_.find(jsonPathOrSubIdx);
		if (idxNameIt != indexesNames_.end() && !indexes_[idxNameIt->second]->Opts().IsSparse() &&
			indexes_[idxNameIt->second]->Opts().IsArray() && (type == IndexCompositeBTree || type == IndexCompositeHash)) {
			throw Error(errParams, "Can't add array subindex '%s' to composite index '%s'", jsonPathOrSubIdx, indexDef.name_);
		}
	}
	const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, payloadType_, {}));
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
	updateSortedIdxCount();
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

void Namespace::Insert(Item &item, const RdxContext &ctx, bool store) { modifyItem(item, ctx, store, ModeInsert); }

void Namespace::Update(Item &item, const RdxContext &ctx, bool store) { modifyItem(item, ctx, store, ModeUpdate); }

void Namespace::Update(const Query &query, QueryResults &result, const RdxContext &ctx, int64_t lsn, bool noLock) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	WLock lock(mtx_, defer_lock, &ctx);

	if (!noLock) {
		cancelCommit_ = true;
		lock.lock();
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(lsn);

	NsSelecter selecter(this);
	SelectCtx selCtx(query);
	selCtx.contextCollectingMode = true;
	selecter(result, selCtx, ctx);

	auto tmStart = high_resolution_clock::now();
	// If update statement is expression, and contains functions call, then we should use
	// row statement replication to preserve data consistense
	bool enableStatementRepl = true;
	for (auto &ue : query.updateFields_) {
		if (ue.isExpression) enableStatementRepl = false;
	}
	if (repl_.slaveMode && !enableStatementRepl) throw Error(errLogic, "Can't apply update query with expression to slave ns '%s'", name_);

	ThrowOnCancel(ctx);

	for (ItemRef &item : result.Items()) {
		updateFieldsFromQuery(item.id, query, true);
		item.value = items_[item.id];
	}
	result.getTagsMatcher(0) = tagsMatcher_;
	result.lockResults();

	WrSerializer ser;
	if (enableStatementRepl && !query.HasLimit() && !query.HasOffset() && (result.Count() >= kWALStatementItemsThreshold)) {
		// FAST PATH: statement based repliaction
		const_cast<Query &>(query).type_ = QueryUpdate;
		WALRecord wrec(WalUpdateQuery, query.GetSQL(ser).Slice());
		if (!repl_.slaveMode) lsn = wal_.Add(wrec);
		observers_->OnWALUpdate(lsn, name_, wrec);
	} else {
		// SLOW PATH: row based repliaction
		for (auto it : result) {
			int id = it.GetItemRef().id;
			lsn = wal_.Add(WALRecord(WalItemUpdate, id), items_[id].GetLSN());
			ser.Reset();
			it.GetCJSON(ser, false);
			observers_->OnWALUpdate(lsn, name_, WALRecord(WalItemModify, ser.Slice(), tagsMatcher_.version(), ModeUpdate));
		}
	}

	if (query.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Updated %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void Namespace::Upsert(Item &item, const RdxContext &ctx, bool store, bool noLock) { modifyItem(item, ctx, store, ModeUpsert, noLock); }

void Namespace::Delete(Item &item, const RdxContext &ctx, bool noLock) {
	ItemImpl *ritem = item.impl_;

	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	WLock lock(mtx_, defer_lock, &ctx);

	if (!noLock) {
		cancelCommit_ = true;
		lock.lock();
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(item.GetLSN());

	updateTagsMatcherFromItem(ritem);

	auto itItem = findByPK(ritem, ctx);
	IdType id = itItem.first;

	if (!itItem.second) {
		return;
	}

	item.setID(id);

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
	observers_->OnWALUpdate(lsn, name_, wrec);
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
		unflushedCount_.fetch_add(1, std::memory_order_release);
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

void Namespace::Delete(const Query &q, QueryResults &result, const RdxContext &ctx, int64_t lsn, bool noLock) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	WLock lock(mtx_, defer_lock, &ctx);
	if (!noLock) {
		cancelCommit_ = true;
		lock.lock();
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(lsn);

	NsSelecter selecter(this);
	SelectCtx selCtx(q);
	selCtx.contextCollectingMode = true;
	selecter(result, selCtx, ctx);
	result.lockResults();

	auto tmStart = high_resolution_clock::now();
	for (auto &r : result.Items()) {
		doDelete(r.id);
	}

	if (!q.HasLimit() && !q.HasOffset() && result.Count() >= kWALStatementItemsThreshold) {
		WrSerializer ser;

		const_cast<Query &>(q).type_ = QueryDelete;
		WALRecord wrec(WalUpdateQuery, q.GetSQL(ser).Slice());
		if (!repl_.slaveMode) lsn = wal_.Add(wrec);

		observers_->OnWALUpdate(lsn, name_, wrec);
	} else if (result.Count() > 0) {
		for (auto it : result) {
			WrSerializer cjson;
			it.GetCJSON(cjson, false);
			int id = it.GetItemRef().id;
			lsn = wal_.Add(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete), items_[id].GetLSN());
			observers_->OnWALUpdate(lsn, name_, WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete));
		}
	}
	if (q.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void Namespace::Truncate(const RdxContext &ctx, int64_t lsn, bool noLock) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	WLock lock(mtx_, defer_lock, &ctx);
	if (!noLock) {
		cancelCommit_ = true;
		lock.lock();
		cancelCommit_ = false;
	}
	calc.LockHit();

	checkApplySlaveUpdate(lsn);

	if (storage_) {
		for (PayloadValue &pv : items_) {
			if (pv.IsFree()) continue;
			Payload pl(payloadType_, pv);
			WrSerializer pk;
			pk << kStorageItemPrefix;
			pl.SerializeFields(pk, pkFields());
			std::unique_lock<std::mutex> lock(storage_mtx_);
			updates_->Remove(pk.Slice());
			unflushedCount_.fetch_add(1, std::memory_order_release);
		}
	}
	items_.clear();
	free_.clear();
	for (size_t i = 0; i < indexes_.size(); ++i) {
		const IndexOpts opts = indexes_[i]->Opts();
		unique_ptr<Index> newIdx{Index::New(getIndexDefinition(i), indexes_[i]->GetPayloadType(), indexes_[i]->Fields())};
		newIdx->SetOpts(opts);
		std::swap(indexes_[i], newIdx);
	}

	WrSerializer ser;
	WALRecord wrec(WalUpdateQuery, (ser << "TRUNCATE " << name_).Slice());
	if (!repl_.slaveMode) lsn = wal_.Add(wrec);
	observers_->OnWALUpdate(lsn, name_, wrec);
}

ReplicationState Namespace::GetReplState(const RdxContext &ctx) const {
	RLock lck(mtx_, &ctx);
	return getReplState();
}

ReplicationState Namespace::getReplState() const {
	ReplicationState ret = repl_;
	ret.dataCount = items_.size() - free_.size();
	if (!repl_.slaveMode) ret.lastLsn = wal_.LSNCounter() - 1;
	return ret;
}

void Namespace::SetSlaveLSN(int64_t slaveLsn, const RdxContext &ctx) {
	WLock lck(mtx_, &ctx);
	assert(repl_.slaveMode);
	/*if (slaveLsn > repl_.lastLsn)*/ repl_.lastLsn = slaveLsn;
	unflushedCount_.fetch_add(1, std::memory_order_release);
}

void Namespace::SetSlaveReplError(const Error &err, const RdxContext &ctx) {
	WLock lck(mtx_, &ctx);
	assert(repl_.slaveMode);
	repl_.replError = err;
	unflushedCount_.fetch_add(1, std::memory_order_release);
}

Transaction Namespace::NewTransaction(const RdxContext &ctx) {
	RLock lck(mtx_, &ctx);
	return Transaction(name_, payloadType_, tagsMatcher_, pkFields());
}

void Namespace::CommitTransaction(Transaction &tx, const RdxContext &ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	cancelCommit_ = true;  // -V519
	WLock lck(mtx_, &ctx);
	cancelCommit_ = false;  // -V519
	calc.LockHit();

	RdxActivityContext *const actCtx = ctx.Activity();
	for (auto &step : tx.GetSteps()) {
		if (step.query_) {
			QueryResults qr;
			if (step.query_->type_ == QueryDelete) {
				Delete(*step.query_, qr, actCtx, -1, true);
			} else {
				Update(*step.query_, qr, actCtx, -1, true);
			}
		} else if (step.status_ == ModeDelete) {
			Delete(step.item_, ctx, true);
		} else {
			modifyItem(step.item_, ctx, true, step.status_, true);
		}
	}
}

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

		// Delete from composite indexes first
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
			try {
				plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
			} catch (const Error &) {
				skrefs.resize(0);
			}
		} else {
			plNew.Get(field, skrefs);
		}

		if (index.Opts().GetCollateMode() == CollateUTF8)
			for (auto &key : skrefs) key.EnsureUTF8();

		// Check for update
		if (doUpdate) {
			if (isIndexSparse) {
				try {
					pl.GetByJsonPath(index.Fields().getTagsPath(0), krefs, index.KeyType());
				} catch (const Error &) {
					krefs.resize(0);
				}
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

		if (!isIndexSparse) {
			// Put value to payload
			pl.Set(field, krefs);
			// If no krefs doUpsert empty value to index
			if (!skrefs.size()) index.Upsert(Variant(), id);
		}
	} while (++field != borderIdx);

	// Upsert to composite indexes
	for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Upsert(Variant(plData), id);
	}
	repl_.dataHash ^= pl.GetHash();
	ritem->RealValue() = plData;
}

void Namespace::ReplaceTagsMatcher(const TagsMatcher &tm, const RdxContext &ctx) {
	assert(!items_.size() && repl_.slaveMode);
	cancelCommit_ = true;
	WLock lck(mtx_, &ctx);
	cancelCommit_ = false;  // -V519
	tagsMatcher_ = tm;
	tagsMatcher_.UpdatePayloadType(payloadType_);
}

void Namespace::Rename(Namespace::Ptr dst, const std::string &storagePath, const RdxContext &ctx) {
	if (this == dst.get() || dst == nullptr) {
		return;
	}
	doRename(dst, std::string(), storagePath, ctx);
}

void Namespace::Rename(const std::string &newName, const std::string &storagePath, const RdxContext &ctx) {
	if (newName.empty()) {
		return;
	}
	doRename(nullptr, newName, storagePath, ctx);
}

void Namespace::updateTagsMatcherFromItem(ItemImpl *ritem) {
	if (ritem->tagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name_, ritem->tagsMatcher().dump());
	}

	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		string jsonSliceBuf(ritem->GetJSON());
		logPrintf(LogTrace, "Conflict TagsMatcher of namespace '%s' on modify: item:\n%s\ntm is\n%s\nnew tm is\n %s\n", name_, jsonSliceBuf,
				  tagsMatcher_.dump(), ritem->tagsMatcher().dump());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Value().SetLSN(ritem->Value().GetLSN());
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

bool Namespace::isEmptyAfterStorageReload() const { return items_.empty() && !storageLoaded_; }

VariantArray Namespace::preprocessUpdateFieldValues(const UpdateEntry &updateEntry, IdType itemId) {
	if (!updateEntry.isExpression) return updateEntry.values;
	assert(updateEntry.values.size() > 0);
	FunctionExecutor funcExecutor(*this);
	ExpressionEvaluator expressionEvaluator(payloadType_, funcExecutor, updateEntry.column);
	return {expressionEvaluator.Evaluate(static_cast<string_view>(updateEntry.values.front()), items_[itemId])};
}

void Namespace::updateFieldsFromQuery(IdType itemId, const Query &q, bool store) {
	if (isEmptyAfterStorageReload()) {
		reloadStorage();
	}

	assert(items_.exists(itemId));

	for (const UpdateEntry &updateField : q.updateFields_) {
		int fieldIdx = 0;
		if (!getIndexByName(updateField.column, fieldIdx)) {
			bool updated = false;
			tagsMatcher_.path2tag(updateField.column, updated);
		}
	}

	PayloadValue &pv = items_[itemId];
	Payload pl(payloadType_, pv);
	repl_.dataHash ^= pl.GetHash();
	pv.Clone(pl.RealSize());

	for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Delete(Variant(pv), itemId);
	}

	for (const UpdateEntry &updateField : q.updateFields_) {
		int fieldIdx = 0;
		bool isIndexedField = getIndexByName(updateField.column, fieldIdx);

		Index &index = *indexes_[fieldIdx];
		bool isIndexSparse = index.Opts().IsSparse();
		assert(!isIndexSparse || (isIndexSparse && index.Fields().getTagsPathsLength() > 0));
		VariantArray values = preprocessUpdateFieldValues(updateField, itemId);

		if (isIndexSparse) {
			pl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else {
			pl.Get(fieldIdx, skrefs, index.Opts().IsArray());
		}

		if (index.Opts().GetCollateMode() == CollateUTF8)
			for (const Variant &key : values) key.EnsureUTF8();

		if (isIndexedField) {
			if (skrefs.empty()) index.Delete(Variant(), itemId);
			for (const Variant &key : skrefs) index.Delete(key, itemId);

			krefs.resize(0);
			krefs.reserve(values.size());
			for (Variant key : values) {
				key.convert(index.KeyType());
				krefs.push_back(index.Upsert(key, itemId));
			}
			if (krefs.empty()) index.Upsert(Variant(), itemId);
			if (!isIndexSparse) {
				pl.Set(fieldIdx, krefs);
			}
		}

		bool isIndexedArray = (isIndexedField && index.Opts().IsArray());
		if (isIndexSparse || !isIndexedField || isIndexedArray) {
			ItemImpl item(payloadType_, pv, tagsMatcher_);
			item.SetField(updateField.column, values);
			Variant tupleValue = indexes_[0]->Upsert(item.GetField(0), itemId);
			pl.Set(0, {tupleValue});
		}
	}

	for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		indexes_[field]->Upsert(Variant(pv), itemId);
	}

	repl_.dataHash ^= pl.GetHash();
	if (storage_ && store) {
		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			ser.PutUInt64(sysRecordsVersions_.tagsVersion);
			tagsMatcher_.serialize(ser);
			tagsMatcher_.clearUpdated();
			writeSysRecToStorage(ser.Slice(), kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, false);
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
		}

		WrSerializer pk;
		WrSerializer data;
		pk << kStorageItemPrefix;
		pl.SerializeFields(pk, pkFields());
		data.PutUInt64(pv.GetLSN());
		ItemImpl item(payloadType_, pv, tagsMatcher_);
		item.GetCJSON(data);
		writeToStorage(pk.Slice(), data.Slice());
	}

	markUpdated();
}

void Namespace::modifyItem(Item &item, const RdxContext &ctx, bool store, int mode, bool noLock) {
	checkApplySlaveUpdate(item.GetLSN());

	// Item to doUpsert
	ItemImpl *itemImpl = item.impl_;
	WLock lock(mtx_, defer_lock, &ctx);
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	if (!noLock) {
		cancelCommit_ = true;  // -V519
		lock.lock();
		cancelCommit_ = false;  // -V519
	}
	calc.LockHit();

	updateTagsMatcherFromItem(itemImpl);
	auto newPl = itemImpl->GetPayload();

	auto realItem = findByPK(itemImpl, ctx);
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

	if (!isEmptyAfterStorageReload()) {
		item.setLSN(lsn);
		item.setID(id);
		doUpsert(itemImpl, id, exists);
	}

	if (storage_ && store) {
		if (tagsMatcher_.isUpdated()) {
			WrSerializer ser;
			ser.PutUInt64(sysRecordsVersions_.tagsVersion);
			tagsMatcher_.serialize(ser);
			tagsMatcher_.clearUpdated();
			writeSysRecToStorage(ser.Slice(), kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, false);
			logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
		}

		WrSerializer pk, data;
		pk << kStorageItemPrefix;
		newPl.SerializeFields(pk, pkFields());
		data.PutUInt64(lsn);
		itemImpl->GetCJSON(data);
		writeToStorage(pk.Slice(), data.Slice());
	}

	observers_->OnModifyItem(lsn, name_, item.impl_, mode);

	markUpdated();
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> Namespace::findByPK(ItemImpl *ritem, const RdxContext &ctx) {
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
	assertf(krefs.size() == 1, "Pkey field must contain 1 key, but there '%d' in '%s.%s'", krefs.size(), name_, pkIndex->Name());

	SelectKeyResult res = pkIndex->SelectKey(krefs, CondEq, 0, Index::SelectOpts(), nullptr, ctx)[0];
	if (res.size() && res[0].ids_.size()) return {res[0].ids_[0], true};
	return {-1, false};
}

void Namespace::optimizeIndexes(const RdxContext &ctx) {
	// This is read lock only atomics based implementation of rebuild indexes
	// If sortOrdersBuilt_ is true, then indexes are completely built
	// In this case reset sortOrdersBuilt_ to false and/or any idset's and sort orders builds are allowed only protected by write lock
	if (sortOrdersBuilt_) return;
	int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	auto lastUpdateTime = lastUpdateTime_.load(std::memory_order_acquire);

	RLock lck(mtx_, &ctx);
	if (!lastUpdateTime || !config_.optimizationTimeout || now - lastUpdateTime < config_.optimizationTimeout) {
		return;
	}

	if (!indexes_.size()) {
		return;
	}

	if (sortOrdersBuilt_ || cancelCommit_) return;

	logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) enter", name_);
	assert(indexes_.firstCompositePos() != 0);
	int field = indexes_.firstCompositePos();
	do {
		field %= indexes_.totalSize();
		PerfStatCalculatorMT calc(indexes_[field]->GetCommitPerfCounter(), enablePerfCounters_);
		calc.LockHit();
		indexes_[field]->Commit();
	} while (++field != indexes_.firstCompositePos() && !cancelCommit_);

	// Update sort orders and sort_id for each index

	int i = 1;
	int maxIndexWorkers = std::min(int(std::thread::hardware_concurrency()), config_.optimizationSortWorkers);
	for (auto &idxIt : indexes_) {
		if (idxIt->IsOrdered() && maxIndexWorkers != 0) {
			NSUpdateSortedContext sortCtx(*this, i++);
			idxIt->MakeSortOrders(sortCtx);
			// Build in multiple threads
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
	sortOrdersBuilt_ = !cancelCommit_ && maxIndexWorkers;
	if (!cancelCommit_) {
		lastUpdateTime_.store(0, std::memory_order_release);
	}
	logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) leave %s", name_, cancelCommit_ ? "(cancelled by concurent update)" : "");
}

uint32_t Namespace::GetItemsCount() { return itemsCount_.load(); }

void Namespace::markUpdated() {
	itemsCount_ = items_.size();
	sortOrdersBuilt_ = false;
	queryCache_->Clear();
	joinCache_->Clear();
	lastUpdateTime_.store(
		std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(),
		std::memory_order_release);
	repl_.updatedUnixNano = getTimeNow("nsec"_sv);
}

void Namespace::updateSelectTime() {
	lastSelectTime_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

int64_t Namespace::getLastSelectTime() const { return lastSelectTime_; }

void Namespace::Select(QueryResults &result, SelectCtx &params, const RdxContext &ctx) {
	if (params.query.entries.Size() == 1 && params.query.entries.IsEntry(0) && params.query.entries[0].index == kLSNIndexName) {
		WALSelecter selecter(this);
		selecter(result, params);
	} else {
		NsSelecter selecter(this);
		selecter(result, params, ctx);
	}
}

IndexDef Namespace::getIndexDefinition(size_t i) const {
	assert(i < indexes_.size());
	IndexDef indexDef;
	const Index &index = *indexes_[i];
	indexDef.name_ = index.Name();
	indexDef.opts_ = index.Opts();
	indexDef.FromType(index.Type());

	if (index.Opts().IsSparse() || static_cast<int>(i) >= payloadType_.NumFields()) {
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
	return indexDef;
}

NamespaceDef Namespace::getDefinition() const {
	auto pt = this->payloadType_;
	NamespaceDef nsDef(name_, StorageOpts().Enabled(!dbpath_.empty()));
	nsDef.indexes.reserve(indexes_.size());
	for (size_t i = 1; i < indexes_.size(); ++i) {
		nsDef.AddIndex(getIndexDefinition(i));
	}
	return nsDef;
}

NamespaceDef Namespace::GetDefinition(const RdxContext &ctx) {
	RLock rlock(mtx_, &ctx);
	return getDefinition();
}

NamespaceMemStat Namespace::GetMemStat(const RdxContext &ctx) {
	RLock lck(mtx_, &ctx);

	NamespaceMemStat ret;
	ret.name = name_;
	ret.joinCache = joinCache_->GetMemStat();
	ret.queryCache = queryCache_->GetMemStat();

	ret.itemsCount = items_.size() - free_.size();
	for (auto &item : items_) {
		if (!item.IsFree()) ret.dataSize += item.GetCapacity() + sizeof(PayloadValue::dataHeader);
	}
	*(static_cast<ReplicationState *>(&ret.replication)) = getReplState();
	ret.replication.walCount = wal_.size();
	ret.replication.walSize = wal_.heap_size();

	ret.emptyItemsCount = free_.size();

	ret.Total.dataSize = ret.dataSize + items_.capacity() * sizeof(PayloadValue);
	ret.Total.cacheSize = ret.joinCache.totalSize + ret.queryCache.totalSize;

	ret.indexes.reserve(indexes_.size());
	for (auto &idx : indexes_) {
		auto istat = idx->GetMemStat();
		istat.sortOrdersSize = idx->IsOrdered() ? (items_.size() * sizeof(IdType)) : 0;
		ret.Total.indexesSize += istat.idsetPlainSize + istat.idsetBTreeSize + istat.sortOrdersSize + istat.fulltextSize + istat.columnSize;
		ret.Total.dataSize += istat.dataSize;
		ret.Total.cacheSize += istat.idsetCache.totalSize;
		ret.indexes.push_back(istat);
	}

	ret.storageOK = storage_ != nullptr;
	ret.storagePath = dbpath_;
	ret.storageLoaded = storageLoaded_.load();
	return ret;
}

NamespacePerfStat Namespace::GetPerfStat(const RdxContext &ctx) {
	RLock lck(mtx_, &ctx);

	NamespacePerfStat ret;
	ret.name = name_;
	ret.selects = selectPerfCounter_.Get<PerfStat>();
	ret.updates = updatePerfCounter_.Get<PerfStat>();
	for (unsigned i = 1; i < indexes_.size(); i++) {
		ret.indexes.emplace_back(indexes_[i]->GetIndexPerfStat());
	}
	return ret;
}

void Namespace::ResetPerfStat(const RdxContext &ctx) {
	RLock lck(mtx_, &ctx);
	selectPerfCounter_.Reset();
	updatePerfCounter_.Reset();
	for (auto &i : indexes_) i->ResetIndexPerfStat();
}

Error Namespace::loadLatestSysRecord(string_view baseSysTag, uint64_t &version, string &content) {
	std::string key(baseSysTag);
	key.append(".");
	std::string latestContent;
	version = 0;
	Error err = errOK;
	for (int i = 0; i < kSysRecordsBackupCount; ++i) {
		Error status = storage_->Read(StorageOpts().FillCache(), string_view(key + std::to_string(i)), content);
		if (!status.ok() && status.code() != errNotFound) {
			logPrintf(LogTrace, "Error on namespace service info(tag: %s, id: %u) load '%s': %s", baseSysTag, i, name_, status.what());
			err = Error(errNotValid, "Error load namespace from storage '%s': %s", name_, status.what());
		}

		if (content.size()) {
			Serializer ser(content.data(), content.size());
			auto curVersion = ser.GetUInt64();
			if (curVersion >= version) {
				version = curVersion;
				latestContent = std::move(content);
				err = errOK;
			}
		}
	}

	if (latestContent.empty()) {
		Error status = storage_->Read(StorageOpts().FillCache(), baseSysTag, content);
		if (!content.empty()) {
			logPrintf(LogTrace, "Converting %s for %s to new format", baseSysTag, name_);
			WrSerializer ser;
			ser.PutUInt64(version);
			ser.Write(string_view(content));
			writeSysRecToStorage(ser.Slice(), baseSysTag, version, true);
		}
		if (!status.ok() && status.code() != errNotFound) {
			return Error(errNotValid, "Error load namespace from storage '%s': %s", name_, status.what());
		}
		return status;
	} else {
		version++;
	}
	latestContent.erase(0, sizeof(uint64_t));
	content = std::move(latestContent);
	return err;
}

bool Namespace::loadIndexesFromStorage() {
	// Check if indexes structures are ready.
	assert(indexes_.size() == 1);
	assert(items_.size() == 0);

	string def;
	Error status = loadLatestSysRecord(kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (def.size()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logPrintf(LogTrace, "Loaded tags(version: %lld) of namespace %s:\n%s",
				  sysRecordsVersions_.tagsVersion ? sysRecordsVersions_.tagsVersion - 1 : 0, name_, tagsMatcher_.dump());
	}

	def.clear();
	status = loadLatestSysRecord(kStorageIndexesPrefix, sysRecordsVersions_.idxVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
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
			string_view indexData = ser.GetVString();
			Error err = indexDef.FromJSON(giftStr(indexData));
			if (!err.ok()) throw err;

			addIndex(indexDef);
		}
	}

	logPrintf(LogTrace, "Loaded index structure(version %lld) of namespace '%s'\n%s",
			  sysRecordsVersions_.idxVersion ? sysRecordsVersions_.idxVersion - 1 : 0, name_, payloadType_->ToString());

	return true;
}

void Namespace::loadReplStateFromStorage() {
	string json;
	Error status = loadLatestSysRecord(kStorageReplStatePrefix, sysRecordsVersions_.replVersion, json);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}

	if (json.size()) {
		logPrintf(LogTrace, "Loading replication state(version %lld) of namespace %s: %s",
				  sysRecordsVersions_.replVersion ? sysRecordsVersions_.replVersion - 1 : 0, name_, json);
		repl_.FromJSON(giftStr(json));
	}
}

void Namespace::saveIndexesToStorage() {
	// clear ItemImpl pool on payload change
	pool_.clear();

	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveIndexesToStorage (%s)", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.idxVersion);
	ser.PutUInt32(kStorageMagic);
	ser.PutUInt32(kStorageVersion);

	ser.PutVarUint(indexes_.size() - 1);
	NamespaceDef nsDef = getDefinition();

	for (const IndexDef &indexDef : nsDef.indexes) {
		WrSerializer wrser;
		indexDef.GetJSON(wrser);
		ser.PutVString(wrser.Slice());
	}

	writeSysRecToStorage(ser.Slice(), kStorageIndexesPrefix, sysRecordsVersions_.idxVersion, true);

	saveReplStateToStorage();
}

void Namespace::saveReplStateToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveReplStateToStorage (%s)", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.replVersion);
	JsonBuilder builder(ser);
	getReplState().GetJSON(builder);
	builder.End();
	writeSysRecToStorage(ser.Slice(), kStorageReplStatePrefix, sysRecordsVersions_.replVersion, true);
}

bool Namespace::needToLoadData(const RdxContext &ctx) const {
	RLock lk(mtx_, &ctx);

	return (storage_ && (dbpath_.length() > 0)) ? !storageLoaded_.load() : false;
}

void Namespace::EnableStorage(const string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx) {
	string dbpath = fs::JoinPath(path, name_);

	WLock lock(mtx_, &ctx);
	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", name_, path);
	}

	bool success = false;
	while (!success) {
		if (!opts.IsCreateIfMissing() && fs::Stat(dbpath) != fs::StatDir) {
			throw Error(errLogic, "Storage directory doesn't exist for namespace '%s' on path '%s' and CreateIfMissing option is not set",
						name_, path);
		}
		storage_.reset(datastorage::StorageFactory::create(storageType));
		Error status = storage_->Open(dbpath, opts);
		if (!status.ok()) {
			if (!opts.IsDropOnFileFormatError()) {
				storage_.reset();
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s' - %s", name_, path, status.what());
			}
		} else {
			success = loadIndexesFromStorage();
			if (!success && !opts.IsDropOnFileFormatError()) {
				storage_.reset();
				throw Error(errLogic, "Can't enable storage for namespace '%s' on path '%s': format error", name_, dbpath);
			}
			loadReplStateFromStorage();
		}
		if (!success && opts.IsDropOnFileFormatError()) {
			logPrintf(LogWarning, "Dropping storage for namespace '%s' on path '%s' due to format error", name_, dbpath);
			opts.DropOnFileFormatError(false);
			storage_->Destroy(dbpath);
			storage_.reset();
		}
	}

	storageOpts_ = opts;
	updates_.reset(storage_->GetUpdatesCollection());
	dbpath_ = dbpath;
}

StorageOpts Namespace::getStorageOpts(const RdxContext &ctx) {
	RLock lk(mtx_, &ctx);
	return storageOpts_;
}

void Namespace::SetStorageOpts(StorageOpts opts, const RdxContext &ctx) {
	WLock lk(mtx_, &ctx);
	if (opts.IsSlaveMode()) {
		storageOpts_.SlaveMode();
		repl_.slaveMode = true;
		logPrintf(LogInfo, "Enable slave mode for namespace '%s'", name_);
	}
}

void Namespace::LoadFromStorage(const RdxContext &ctx) {
	WLock lock(mtx_, &ctx);

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
	if (!repl_.slaveMode) initWAL(maxLSN);

	logPrintf(LogInfo, "[%s] Done loading storage. %d items loaded (%d errors %s), lsn #%ld%s, total size=%dM, dataHash=%ld", name_,
			  items_.size(), errCount, lastErr.what(), repl_.lastLsn, repl_.slaveMode ? " (slave)" : "", ldcount / (1024 * 1024),
			  repl_.dataHash);
	storageLoaded_ = true;
	if (dataHash != repl_.dataHash) {
		logPrintf(LogError, "[%s] Warning dataHash mismatch %lu != %lu", name_, dataHash, repl_.dataHash);
		unflushedCount_.fetch_add(1, std::memory_order_release);
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
	logPrintf(LogInfo, "[%s] WAL initalized lsn #%ld", name_, repl_.lastLsn);
}

void Namespace::removeExpiredItems(RdxActivityContext *ctx) {
	const RdxContext rdxCtx{ctx};
	WLock wlock(mtx_, &rdxCtx);
	if (repl_.slaveMode) {
		return;
	}
	for (const std::unique_ptr<Index> &index : indexes_) {
		if ((index->Type() != IndexTtl) || (index->Size() == 0)) continue;
		int64_t expirationthreshold =
			std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count() -
			index->GetTTLValue();
		QueryResults qr;
		Delete(Query(name_).Where(index->Name(), CondLt, expirationthreshold), qr, rdxCtx, -1, true);
	}
}

void Namespace::BackgroundRoutine(RdxActivityContext *ctx) {
	flushStorage(ctx);
	optimizeIndexes(ctx);
	removeExpiredItems(ctx);
}

void Namespace::flushStorage(const RdxContext &ctx) {
	RLock rlock(mtx_, &ctx);
	if (storage_) {
		if (unflushedCount_.load(std::memory_order_acquire) > 0) {
			std::unique_lock<std::mutex> lck(storage_mtx_);
			Error status = storage_->Write(StorageOpts().FillCache(), *(updates_.get()));
			if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage: %s", name_, status.what());
			unflushedCount_.store(0, std::memory_order_release);
			updates_->Clear();
			saveReplStateToStorage();
		}
	}
}

void Namespace::DeleteStorage(const RdxContext &ctx) {
	WLock lck(mtx_, &ctx);
	deleteStorage();
}

void Namespace::CloseStorage(const RdxContext &ctx) {
	flushStorage(ctx);
	WLock lck(mtx_, &ctx);
	dbpath_.clear();
	storage_.reset();
}

bool Namespace::tryToReload(const RdxContext &ctx) {
	uint16_t noQueryIdleThresholdSec = getStorageOpts(ctx).noQueryIdleThresholdSec;
	if (noQueryIdleThresholdSec > 0) {
		int64_t now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
		if ((now - getLastSelectTime()) > noQueryIdleThresholdSec) {
			reloadStorage();
			return true;
		}
	}
	return true;
}

void Namespace::reloadStorage() {
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
}

std::string Namespace::sysRecordName(string_view sysTag, uint64_t version) {
	std::string backupRecord(sysTag);
	static_assert(kSysRecordsBackupCount && ((kSysRecordsBackupCount & (kSysRecordsBackupCount - 1)) == 0),
				  "kBackupsCount has to be power of 2");
	backupRecord.append(".").append(std::to_string(version & (kSysRecordsBackupCount - 1)));
	return backupRecord;
}

void Namespace::writeSysRecToStorage(string_view data, string_view sysTag, uint64_t &version, bool direct) {
	size_t iterCount = (version > 0) ? 1 : kSysRecordsFirstWriteCopies;
	for (size_t i = 0; i < iterCount; ++i, ++version) {
		*(reinterpret_cast<uint64_t *>(const_cast<char *>(data.data()))) = version;
		if (direct) {
			storage_->Write(StorageOpts().FillCache().Sync(0 == version), sysRecordName(sysTag, version), data);
		} else {
			writeToStorage(sysRecordName(sysTag, version), data);
		}
	}
}

Item Namespace::NewItem(const RdxContext &ctx) {
	RLock lock(mtx_, &ctx);
	auto impl_ = pool_.get(ItemImpl(payloadType_, tagsMatcher_, pkFields()));
	impl_->tagsMatcher() = tagsMatcher_;
	impl_->tagsMatcher().clearUpdated();
	return Item(impl_);
}
void Namespace::ToPool(ItemImpl *item) {
	item->Clear();
	pool_.put(item);
}

// Get meta data from storage by key
string Namespace::GetMeta(const string &key, const RdxContext &ctx) {
	RLock lock(mtx_, &ctx);
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
void Namespace::PutMeta(const string &key, const string_view &data, const RdxContext &ctx, int64_t lsn) {
	WLock lock(mtx_, &ctx);
	checkApplySlaveUpdate(lsn);
	putMeta(key, data);
}

// Put meta data to storage by key
void Namespace::putMeta(const string &key, const string_view &data) {
	meta_[key] = string(data);

	if (storage_) {
		storage_->Write(StorageOpts().FillCache(), kStorageMetaPrefix + key, data);
	}

	WALRecord wrec(WalPutMeta, key, data);

	int64_t lsn = repl_.slaveMode ? -1 : wal_.Add(wrec);
	observers_->OnWALUpdate(lsn, name_, wrec);
}

vector<string> Namespace::EnumMeta(const RdxContext &ctx) {
	vector<string> ret;

	RLock lck(mtx_, &ctx);
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
			string key(keySlice.substr(prefixLen));
			if (meta_.find(key) == meta_.end()) {
				ret.push_back(key);
			}
		}
	}
	return ret;
}

int Namespace::getSortedIdxCount() const {
	if (!config_.optimizationSortWorkers) return 0;
	int cnt = 0;
	for (auto &it : indexes_)
		if (it->IsOrdered()) cnt++;
	return cnt;
}

void Namespace::updateSortedIdxCount() {
	int sortedIdxCount = getSortedIdxCount();
	for (auto &idx : indexes_) idx->SetSortedIdxCount(sortedIdxCount);
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

void Namespace::deleteStorage() {
	if (storage_) {
		storage_->Destroy(dbpath_);
		dbpath_.clear();
		storage_.reset();
	}
}

void Namespace::doRename(Namespace::Ptr dst, const std::string &newName, const std::string &storagePath, const RdxContext &ctx) {
	std::string dbpath;
	flushStorage(ctx);
	WLock srcLck(mtx_, &ctx);
	if (dst) {
		dst->mtx_.lock();
		dbpath = dst->dbpath_;
	} else if (newName == name_) {
		return;
	}

	if (dbpath.empty()) {
		dbpath = fs::JoinPath(storagePath, newName);
	} else {
		dst->deleteStorage();
	}

	bool hadStorage = (storage_ != nullptr);
	auto storageType = StorageType::LevelDB;
	if (hadStorage) {
		storageType = storage_->Type();
		storage_.reset();
		fs::RmDirAll(dbpath);
		int renameRes = fs::Rename(dbpath_, dbpath);
		if (renameRes < 0) {
			if (dst) {
				dst->mtx_.unlock();
			}
			throw Error(errParams, "Unable to rename '%s' to '%s'", dbpath_, dbpath);
		}
	}
	if (dst) {
		name_ = dst->name_;
		dst->mtx_.unlock();
	} else {
		name_ = newName;
	}

	if (hadStorage) {
		logPrintf(LogTrace, "Storage was moved from %s to %s", dbpath_, dbpath);
		dbpath_ = std::move(dbpath);
		storage_.reset(datastorage::StorageFactory::create(storageType));
		auto status = storage_->Open(dbpath_, storageOpts_);
		if (!status.ok()) {
			throw status;
		}
		if (repl_.temporary) {
			repl_.temporary = false;
			saveReplStateToStorage();
		}
	}
}

void Namespace::checkApplySlaveUpdate(int64_t lsn) {
	if (repl_.slaveMode) {
		if (lsn == -1) {
			throw Error(errLogic, "Can't modify slave ns '%s'", name_);
		} else if (!repl_.replError.ok()) {
			throw Error(errLogic, "Can't modify slave ns '%s', ns has replication error: %s", name_, repl_.replError.what());
		}
	}
}

void Namespace::setFieldsBasedOnPrecepts(ItemImpl *ritem) {
	for (auto &precept : ritem->GetPrecepts()) {
		SelectFuncParser sqlFunc;
		SelectFuncStruct sqlFuncStruct = sqlFunc.Parse(precept);

		VariantArray krs;
		Variant field = ritem->GetPayload().Get(sqlFuncStruct.field, krs)[0];

		Variant value(make_key_string(sqlFuncStruct.value));
		if (sqlFuncStruct.isFunction) {
			value = FunctionExecutor(*this).Execute(sqlFuncStruct);
		}

		value.convert(field.Type());
		VariantArray refs{value};

		ritem->GetPayload().Set(sqlFuncStruct.field, refs, false);
	}
}

int64_t Namespace::GetSerial(const string &field) {
	int64_t counter = kStorageSerialInitial;

	string ser = getMeta("_SERIAL_" + field);
	if (ser != "") {
		counter = stoi(ser) + 1;
	}

	string s = to_string(counter);
	putMeta("_SERIAL_" + field, string_view(s));

	return counter;
}

void Namespace::FillResult(QueryResults &result, IdSet::Ptr ids) const {
	for (auto &id : *ids) {
		result.Add({id, items_[id], 0, 0});
	}
}

void Namespace::GetFromJoinCache(JoinCacheRes &ctx) const {
	if (config_.cacheMode == CacheModeOff || !sortOrdersBuilt_) return;
	auto it = joinCache_->Get(ctx.key);
	ctx.needPut = false;
	ctx.haveData = false;
	if (it.valid) {
		if (!it.val.inited) {
			ctx.needPut = true;
		} else {
			ctx.haveData = true;
			ctx.it = std::move(it);
		}
	}
}

void Namespace::GetIndsideFromJoinCache(JoinCacheRes &ctx) const {
	if (config_.cacheMode != CacheModeAggressive || !sortOrdersBuilt_) return;
	auto it = joinCache_->Get(ctx.key);
	ctx.needPut = false;
	ctx.haveData = false;
	if (it.valid) {
		if (!it.val.inited) {
			ctx.needPut = true;
		} else {
			ctx.haveData = true;
			ctx.it = std::move(it);
		}
	}
}

void Namespace::PutToJoinCache(JoinCacheRes &res, JoinPreResult::Ptr preResult) const {
	JoinCacheVal joinCacheVal;
	res.needPut = false;
	joinCacheVal.inited = true;
	joinCacheVal.preResult = preResult;
	joinCache_->Put(res.key, joinCacheVal);
}
void Namespace::PutToJoinCache(JoinCacheRes &res, JoinCacheVal &val) const {
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
