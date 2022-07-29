#include "core/namespace/namespaceimpl.h"
#include <algorithm>
#include <chrono>
#include <ctime>
#include <memory>
#include <string>
#include <thread>
#include "core/cjson/jsonbuilder.h"
#include "core/index/index.h"
#include "core/index/ttlindex.h"
#include "core/itemimpl.h"
#include "core/itemmodifier.h"
#include "core/nsselecter/nsselecter.h"
#include "core/payload/payloadiface.h"
#include "core/rdxcontext.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/storage/storagefactory.h"
#include "namespace.h"
#include "snapshot/snapshothandler.h"
#include "tools/errors.h"
#include "tools/flagguard.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/timetools.h"
#include "wal/walselecter.h"

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using std::make_shared;
using std::thread;
using std::to_string;
using std::defer_lock;
using reindexer::cluster::UpdateRecord;

#define kStorageItemPrefix "I"
#define kStorageIndexesPrefix "indexes"
#define kStorageSchemaPrefix "schema"
#define kStorageReplStatePrefix "repl"
#define kStorageTagsPrefix "tags"
#define kStorageMetaPrefix "meta"
#define kStorageCachePrefix "cache"
#define kTupleName "-tuple"

static const string kPKIndexName = "#pk";
constexpr int kWALStatementItemsThreshold = 5;

#define kStorageMagic 0x1234FEDC
#define kStorageVersion 0x8

namespace reindexer {

constexpr int64_t kStorageSerialInitial = 1;
constexpr uint8_t kSysRecordsBackupCount = 8;
constexpr uint8_t kSysRecordsFirstWriteCopies = 3;
constexpr size_t kMaxMemorySizeOfStringsHolder = 1ull << 24;

NamespaceImpl::IndexesStorage::IndexesStorage(const NamespaceImpl &ns) : ns_(ns) {}

void NamespaceImpl::IndexesStorage::MoveBase(IndexesStorage &&src) { Base::operator=(move(src)); }
// private implementation and NOT THREADSAFE of copy CTOR
NamespaceImpl::NamespaceImpl(const NamespaceImpl &src)
	: indexes_{*this},
	  indexesNames_{src.indexesNames_},
	  items_{src.items_},
	  free_{src.free_},
	  name_{src.name_},
	  payloadType_{src.payloadType_},
	  tagsMatcher_{src.tagsMatcher_},
	  storage_{src.storage_},
	  updates_{src.updates_},
	  unflushedCount_{src.unflushedCount_.load(std::memory_order_acquire)},	 // 0
	  meta_{src.meta_},
	  dbpath_{src.dbpath_},
	  queryCache_{make_shared<QueryCache>()},
	  sparseIndexesCount_{src.sparseIndexesCount_},
	  krefs(src.krefs),
	  skrefs(src.skrefs),
	  sysRecordsVersions_{src.sysRecordsVersions_},
	  locker_(src.clusterizator_, *this),
	  schema_(src.schema_),
	  joinCache_{make_shared<JoinCache>()},
	  enablePerfCounters_{src.enablePerfCounters_.load()},
	  config_{src.config_},
	  wal_{src.wal_},
	  repl_{src.repl_},
	  storageOpts_{src.storageOpts_},
	  lastSelectTime_{0},
	  cancelCommitCnt_{0},
	  lastUpdateTime_{src.lastUpdateTime_.load(std::memory_order_acquire)},
	  itemsCount_{static_cast<uint32_t>(items_.size())},
	  itemsCapacity_{static_cast<uint32_t>(items_.capacity())},
	  nsIsLoading_{false},
	  itemsDataSize_{src.itemsDataSize_},
	  optimizationState_{NotOptimized},
	  strHolder_{makeStringsHolder()},
	  clusterizator_{src.clusterizator_} {
	for (auto &idxIt : src.indexes_) indexes_.push_back(idxIt->Clone());

	markUpdated(true);
	logPrintf(LogInfo, "Namespace::CopyContentsFrom (%s).Workers: %d, timeout: %d, tm_statetoken: %08X", name_,
			  config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher_.stateToken());
}

NamespaceImpl::NamespaceImpl(const string &name, std::optional<int32_t> stateToken, cluster::INsDataReplicator *clusterizator)
	: indexes_(*this),
	  name_(name),
	  payloadType_(name),
	  tagsMatcher_(payloadType_, stateToken.has_value() ? stateToken.value() : tools::RandomGenerator::gets32()),
	  unflushedCount_{0},
	  queryCache_(make_shared<QueryCache>()),
	  locker_(clusterizator, *this),
	  joinCache_(make_shared<JoinCache>()),
	  enablePerfCounters_(false),
	  wal_(config_.walSize),
	  lastSelectTime_{0},
	  cancelCommitCnt_{0},
	  lastUpdateTime_{0},
	  nsIsLoading_(false),
	  strHolder_{makeStringsHolder()},
	  clusterizator_(clusterizator) {
	logPrintf(LogTrace, "NamespaceImpl::NamespaceImpl (%s)", name_);
	FlagGuardT nsLoadingGuard(nsIsLoading_);
	items_.reserve(10000);
	itemsCapacity_.store(items_.capacity());
	optimizationState_.store(NotOptimized);

	// Add index and payload field for tuple of non indexed fields
	IndexDef tupleIndexDef(kTupleName, {}, IndexStrStore, IndexOpts());
	addIndex(tupleIndexDef, false);
	updateSelectTime();

	logPrintf(LogInfo, "Namespace::Construct (%s).Workers: %d, timeout: %d, tm_statetoken: %08X(%s)", name_,
			  config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher_.stateToken(),
			  stateToken.has_value() ? "preset" : "rand");
}

NamespaceImpl::~NamespaceImpl() {
	try {
		flushStorage(RdxContext());
	} catch (Error &e) {
		logPrintf(LogWarning, "Namespace::~Namespace (%s), flushStorage() error: %s", name_, e.what());
	}

#ifndef NDEBUG
	for (const auto &strHldr : strHoldersWaitingToBeDeleted_) {
		assertrx(strHldr.unique());
		(void)strHldr;
	}
	assertrx(strHolder_.unique());
	assertrx(cancelCommitCnt_.load() == 0);
#endif

	logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items", name_, items_.size());
}

void NamespaceImpl::SetNsVersion(lsn_t version, const RdxContext &ctx) {
	auto wlck = simpleWLock(ctx);
	repl_.nsVersion = version;
	saveReplStateToStorage();
}

void NamespaceImpl::OnConfigUpdated(DBConfigProvider &configProvider, const RdxContext &ctx) {
	NamespaceConfigData configData;
	configProvider.GetNamespaceConfig(GetName(ctx), configData);
	const int serverId = configProvider.GetReplicationConfig().serverID;

	enablePerfCounters_ = configProvider.GetProfilingConfig().perfStats;

	auto wlck = simpleWLock(ctx);

	const bool needReoptimizeIndexes = (config_.optimizationSortWorkers == 0) != (configData.optimizationSortWorkers == 0);
	if (config_.optimizationSortWorkers != configData.optimizationSortWorkers ||
		config_.optimizationTimeout != configData.optimizationTimeout) {
		logPrintf(LogInfo, "[%s] Setting new index optimization config. Workers: %d->%d, timeout: %d->%d", name_,
				  config_.optimizationSortWorkers, configData.optimizationSortWorkers, config_.optimizationTimeout,
				  configData.optimizationSortWorkers);
	}
	config_ = configData;
	storageOpts_.LazyLoad(configData.lazyLoad);
	storageOpts_.noQueryIdleThresholdSec = configData.noQueryIdleThreshold;

	for (auto &idx : indexes_) {
		idx->EnableUpdatesCountingMode(configData.idxUpdatesCountingMode);
	}

	if (needReoptimizeIndexes) {
		updateSortedIdxCount();
	}

	if (wal_.Resize(config_.walSize)) {
		logPrintf(LogInfo, "[%s] WAL has been resized lsn #%s, max size %ld", name_, repl_.lastLsn, wal_.Capacity());
	}

	if (isSystem()) return;

	if (wal_.GetServer() != serverId) {
		if (itemsCount_ != 0) {
			repl_.clusterStatus.role = ClusterizationStatus::Role::None;  // TODO: Maybe we have to add separate role for this case
			logPrintf(LogWarning, "Change serverId on non empty ns [%s]. Cluster role was reset to None", name_);
		}
		wal_.SetServer(serverId);
		logPrintf(LogWarning, "[repl:%s]:%d Changing serverId. Tm_statetoken: %08X", name_, serverId, tagsMatcher_.stateToken());
	}
}

void NamespaceImpl::recreateCompositeIndexes(int startIdx, int endIdx) {
	for (int i = startIdx; i < endIdx; ++i) {
		std::unique_ptr<reindexer::Index> &index(indexes_[i]);
		if (IsComposite(index->Type())) {
			IndexDef indexDef;
			indexDef.name_ = index->Name();
			indexDef.opts_ = index->Opts();
			indexDef.FromType(index->Type());

			auto newIndex{Index::New(indexDef, payloadType_, index->Fields())};
			if (index->HoldsStrings()) {
				strHolder_->Add(std::move(index));
			}
			std::swap(index, newIndex);
		}
	}
}

void NamespaceImpl::updateItems(PayloadType oldPlType, const FieldsSet &changedFields, int deltaFields) {
	logPrintf(LogTrace, "Namespace::updateItems(%s) delta=%d", name_, deltaFields);

	assertrx(oldPlType->NumFields() + deltaFields == payloadType_->NumFields());

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

	VariantArray skrefsDel, skrefsUps;
	ItemImpl newItem(payloadType_, tagsMatcher_);
	newItem.Unsafe(true);
	int errCount = 0;
	Error lastErr = errOK;
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
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
				bool needClearCache{false};
				index.Delete(skrefsDel, rowId, *strHolder_, needClearCache);
				if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
			}

			if ((fieldIdx == 0) || deltaFields >= 0) {
				newItem.GetPayload().Get(fieldIdx, skrefsUps);
				krefs.resize(0);
				bool needClearCache{false};
				index.Upsert(krefs, skrefsUps, rowId, needClearCache);
				if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
				newValue.Set(fieldIdx, krefs);
			}
		}

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			bool needClearCache{false};
			indexes_[fieldIdx]->Upsert(Variant(plNew), rowId, needClearCache);
			if (needClearCache && indexes_[fieldIdx]->IsOrdered()) indexesCacheCleaner.Add(indexes_[fieldIdx]->SortId());
		}

		plCurr = std::move(plNew);
		repl_.dataHash ^= Payload(payloadType_, plCurr).GetHash();
		itemsDataSize_ += plCurr.GetCapacity() + sizeof(PayloadValue::dataHeader);
	}
	markUpdated(false);
	if (errCount != 0) {
		logPrintf(LogError, "Can't update indexes of %d items in namespace %s: %s", errCount, name_, lastErr.what());
	}
}

void NamespaceImpl::addToWAL(const IndexDef &indexDef, WALRecType type, const NsContext &ctx) {
	WrSerializer ser;
	indexDef.GetJSON(ser);
	processWalRecord(WALRecord(type, ser.Slice()), ctx);
}

void NamespaceImpl::addToWAL(std::string_view json, WALRecType type, const NsContext &ctx) { processWalRecord(WALRecord(type, json), ctx); }

void NamespaceImpl::AddIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	if (!validateIndexName(indexDef.name_, indexDef.Type())) {
		throw Error(errParams,
					"Cannot add index '%s' in namespace '%s'. Index name contains invalid characters. Only alphas, digits, '+' (for "
					"composite indexes only), '.', '_' "
					"and '-' are allowed",
					indexDef.name_, name_);
	} else if (indexDef.opts_.IsPK()) {
		if (indexDef.opts_.IsArray()) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't be array", indexDef.name_, GetName(ctx));
		} else if (isStore(indexDef.Type())) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't have '-' type", indexDef.name_, GetName(ctx));
		} else if (IsFullText(indexDef.Type())) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't be fulltext index", indexDef.name_,
						GetName(ctx));
		}
	}
	UpdatesContainer pendedRepl;

	auto wlck = dataWLock(ctx, true);

	const bool checkIdxEqualityNow = ctx.GetOriginLSN().isEmpty() && repl_.clusterStatus.role != ClusterizationStatus::Role::None;
	// Check index existance before cluster role check, to allow followers "add" their indexes locally
	if (checkIdxEqualityNow && checkIfSameIndexExists(indexDef, false)) {
		if (ctx.HasEmmiterServer()) {
			// Make sure, that index was already replicated to emmiter
			pendedRepl.emplace_back(UpdateRecord::Type::EmptyUpdate, name_, ctx.EmmiterServerId());
			replicate(std::move(pendedRepl), std::move(wlck), ctx);
		}
		return;
	}

	checkClusterStatus(ctx);

	doAddIndex(indexDef, checkIdxEqualityNow, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::DumpIndex(std::ostream &os, std::string_view index, const RdxContext &ctx) const {
	auto rlck = rLock(ctx);
	dumpIndex(os, index);
}

void NamespaceImpl::UpdateIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);
	doUpdateIndex(indexDef, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::DropIndex(const IndexDef &indexDef, const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);
	doDropIndex(indexDef, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::SetSchema(std::string_view schema, const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx, true);

	if (ctx.GetOriginLSN().isEmpty() && schema_ &&
		schema_->GetJSON() == Schema::AppendProtobufNumber(schema, schema_->GetProtobufNsNumber())) {
		if (ctx.HasEmmiterServer()) {
			// Make sure, that schema was already replicated to emmiter
			pendedRepl.emplace_back(UpdateRecord::Type::EmptyUpdate, name_, ctx.EmmiterServerId());
			replicate(std::move(pendedRepl), std::move(wlck), ctx);
		}
		return;
	}

	checkClusterStatus(ctx);

	setSchema(schema, pendedRepl, ctx);

	saveSchemaToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

std::string NamespaceImpl::GetSchema(int format, const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	WrSerializer ser;
	if (schema_) {
		if (format == JsonSchemaType) {
			schema_->GetJSON(ser);
		} else if (format == ProtobufSchemaType) {
			Error err = schema_->GetProtobufSchema(ser);
			if (!err.ok()) throw err;
		} else {
			throw Error(errParams, "Unknown schema type: %d", format);
		}
	}
	return std::string(ser.Slice());
}

void NamespaceImpl::dumpIndex(std::ostream &os, std::string_view index) const {
	auto itIdxName = indexesNames_.find(index);
	if (itIdxName == indexesNames_.end()) {
		const char *errMsg = "Cannot dump index %s: doesn't exist";
		logPrintf(LogError, errMsg, index);
		throw Error(errParams, errMsg, index);
	}
	indexes_[itIdxName->second]->Dump(os);
}

void NamespaceImpl::dropIndex(const IndexDef &index, bool disableTmVersionInc) {
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

	std::unique_ptr<Index> &indexToRemove = indexes_[fieldIdx];
	if (indexToRemove->Opts().IsPK()) {
		indexesNames_.erase(kPKIndexName);
	}

	// Update indexes fields refs
	for (const std::unique_ptr<Index> &idx : indexes_) {
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

	if (!IsComposite(indexToRemove->Type()) && !indexToRemove->Opts().IsSparse()) {
		PayloadType oldPlType = payloadType_;
		payloadType_.Drop(index.name_);
		tagsMatcher_.UpdatePayloadType(payloadType_, !disableTmVersionInc);
		FieldsSet changedFields{0, fieldIdx};
		updateItems(oldPlType, changedFields, -1);
	}

	removeIndex(indexToRemove);
	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
	updateSortedIdxCount();
}

void NamespaceImpl::doDropIndex(const IndexDef &index, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	dropIndex(index, ctx.inSnapshot);

	addToWAL(index, WalIndexDrop, ctx);
	pendedRepl.emplace_back(UpdateRecord::Type::IndexDrop, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), index);
}

static void verifyConvertTypes(KeyValueType from, KeyValueType to, const PayloadType &payloadType, const FieldsSet &fields) {
	if ((from == KeyValueString || to == KeyValueString) && (from != to)) {
		throw Error(errParams, "Cannot convert key from type %s to %s", Variant::TypeName(from), Variant::TypeName(to));
	}
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

void NamespaceImpl::verifyUpdateIndex(const IndexDef &indexDef) const {
	const auto idxNameIt = indexesNames_.find(indexDef.name_);
	const auto currentPKIt = indexesNames_.find(kPKIndexName);

	if (idxNameIt == indexesNames_.end()) {
		throw Error(errParams, "Cannot update index %s: doesn't exist", indexDef.name_);
	}
	const auto &oldIndex = indexes_[idxNameIt->second];
	if (indexDef.opts_.IsPK() && !oldIndex->Opts().IsPK() && currentPKIt != indexesNames_.end()) {
		throw Error(errConflict, "Cannot add PK index '%s.%s'. Already exists another PK index - '%s'", name_, indexDef.name_,
					indexes_[currentPKIt->second]->Name());
	}
	if (indexDef.opts_.IsArray() != oldIndex->Opts().IsArray()) {
		throw Error(errParams, "Cannot update index '%s' in namespace '%s'. Can't convert array index to not array and vice versa",
					indexDef.name_, name_);
	}
	if (indexDef.opts_.IsPK() && indexDef.opts_.IsArray()) {
		throw Error(errParams, "Cannot update index '%s' in namespace '%s'. PK field can't be array", indexDef.name_, name_);
	}
	if (indexDef.opts_.IsPK() && isStore(indexDef.Type())) {
		throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't have '-' type", indexDef.name_, name_);
	}

	if (IsComposite(indexDef.Type())) {
		verifyUpdateCompositeIndex(indexDef);
		return;
	}

	const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet()));
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

void NamespaceImpl::addIndex(const IndexDef &indexDef, bool disableTmVersionInc, bool skipEqualityCheck) {
	const string &indexName = indexDef.name_;

	int idxNo = payloadType_->NumFields();
	IndexOpts opts = indexDef.opts_;
	JsonPaths jsonPaths = indexDef.jsonPaths_;
	auto currentPKIndex = indexesNames_.find(kPKIndexName);

	bool requireTtlUpdate = false;
	if (!skipEqualityCheck && checkIfSameIndexExists(indexDef, true, &requireTtlUpdate)) {
		if (requireTtlUpdate) {
			auto idxNameIt = indexesNames_.find(indexDef.name_);
			assertrx(idxNameIt != indexesNames_.end());
			UpdateExpireAfter(indexes_[idxNameIt->second].get(), indexDef.expireAfter_);
		}
		return;
	}

	// New index case. Just add
	if (currentPKIndex != indexesNames_.end() && opts.IsPK()) {
		throw Error(errConflict, "Cannot add PK index '%s.%s'. Already exists another PK index - '%s'", name_, indexName,
					indexes_[currentPKIndex->second]->Name());
	}

	if (IsComposite(indexDef.Type())) {
		addCompositeIndex(indexDef);
		return;
	}

	if (idxNo >= maxIndexes) {
		throw Error(errConflict, "Cannot add index '%s.%s'. Too many non-composite indexes. %d non-composite indexes are allowed only",
					name_, indexName, maxIndexes - 1);
	}
	std::unique_ptr<Index> newIndex = Index::New(indexDef, PayloadType(), FieldsSet());
	FieldsSet fields;
	if (opts.IsSparse()) {
		for (const string &jsonPath : jsonPaths) {
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPath, true);
			assertrx(tagsPath.size() > 0);
			fields.push_back(jsonPath);
			fields.push_back(tagsPath);
		}

		++sparseIndexesCount_;
		insertIndex(Index::New(indexDef, payloadType_, fields), idxNo, indexName);
	} else {
		PayloadType oldPlType = payloadType_;

		payloadType_.Add(PayloadFieldType(newIndex->KeyType(), indexName, jsonPaths, newIndex->Opts().IsArray()));
		tagsMatcher_.UpdatePayloadType(payloadType_, !disableTmVersionInc);
		newIndex->SetFields(FieldsSet{idxNo});
		newIndex->UpdatePayloadType(payloadType_);

		FieldsSet changedFields{0, idxNo};
		insertIndex(std::move(newIndex), idxNo, indexName);
		updateItems(oldPlType, changedFields, 1);
	}
	updateSortedIdxCount();
}

void NamespaceImpl::doAddIndex(const IndexDef &indexDef, bool skipEqualityCheck, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	addIndex(indexDef, ctx.inSnapshot, skipEqualityCheck);

	addToWAL(indexDef, WalIndexAdd, ctx);
	pendedRepl.emplace_back(UpdateRecord::Type::IndexAdd, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(),
							indexDef);
}

void NamespaceImpl::updateIndex(const IndexDef &indexDef, bool disableTmVersionInc) {
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
	dropIndex(indexDef, disableTmVersionInc);
	addIndex(indexDef, disableTmVersionInc);
}

void NamespaceImpl::doUpdateIndex(const IndexDef &indexDef, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	updateIndex(indexDef, ctx.inSnapshot);
	addToWAL(indexDef, WalIndexUpdate, ctx.rdxContext);
	pendedRepl.emplace_back(UpdateRecord::Type::IndexUpdate, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(),
							indexDef);
}

IndexDef NamespaceImpl::getIndexDefinition(const string &indexName) const {
	NamespaceDef nsDef = getDefinition();

	auto indexes = nsDef.indexes;
	auto indexDefIt = std::find_if(indexes.begin(), indexes.end(), [&](const IndexDef &idxDef) { return idxDef.name_ == indexName; });
	if (indexDefIt == indexes.end()) {
		throw Error(errParams, "Index '%s' not found in '%s'", indexName, name_);
	}

	return *indexDefIt;
}

void NamespaceImpl::verifyUpdateCompositeIndex(const IndexDef &indexDef) const {
	IndexType type = indexDef.Type();

	for (auto &jsonPathOrSubIdx : indexDef.jsonPaths_) {
		auto idxNameIt = indexesNames_.find(jsonPathOrSubIdx);
		if (idxNameIt != indexesNames_.end() && !indexes_[idxNameIt->second]->Opts().IsSparse() &&
			indexes_[idxNameIt->second]->Opts().IsArray() && (type == IndexCompositeBTree || type == IndexCompositeHash)) {
			throw Error(errParams, "Cannot add array subindex '%s' to composite index '%s'", jsonPathOrSubIdx, indexDef.name_);
		}
	}
	const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, payloadType_, {}));
}

void NamespaceImpl::addCompositeIndex(const IndexDef &indexDef) {
	string indexName = indexDef.name_;
	IndexType type = indexDef.Type();
	IndexOpts opts = indexDef.opts_;

	FieldsSet fields;

	for (auto &jsonPathOrSubIdx : indexDef.jsonPaths_) {
		auto idxNameIt = indexesNames_.find(jsonPathOrSubIdx);
		if (idxNameIt == indexesNames_.end()) {
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPathOrSubIdx, true);
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
				throw Error(errParams, "Cannot add array subindex '%s' to composite index '%s'", jsonPathOrSubIdx, indexName);
			}
			fields.push_back(idxNameIt->second);
		}
	}

	assertrx(fields.getJsonPathsLength() == fields.getTagsPathsLength());
	assertrx(indexesNames_.find(indexName) == indexesNames_.end());

	int idxPos = indexes_.size();
	insertIndex(Index::New(indexDef, payloadType_, fields), idxPos, indexName);

	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (IdType rowId = 0; rowId < int(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			bool needClearCache{false};
			indexes_[idxPos]->Upsert(Variant(items_[rowId]), rowId, needClearCache);
			if (needClearCache && indexes_[idxPos]->IsOrdered()) indexesCacheCleaner.Add(indexes_[idxPos]->SortId());
		}
	}
	updateSortedIdxCount();
}

bool NamespaceImpl::checkIfSameIndexExists(const IndexDef &indexDef, bool discardConfig, bool *requireTtlUpdate) {
	auto idxNameIt = indexesNames_.find(indexDef.name_);
	if (idxNameIt != indexesNames_.end()) {
		IndexDef oldIndexDef = getIndexDefinition(indexDef.name_);
		bool indexIsSame = false;
		if (discardConfig) {
			IndexDef newIndexDef = indexDef;
			oldIndexDef.opts_.config = "";
			newIndexDef.opts_.config = "";
			if (requireTtlUpdate && oldIndexDef.Type() == IndexTtl) {
				if (oldIndexDef.expireAfter_ != newIndexDef.expireAfter_) {
					*requireTtlUpdate = true;
				}
				oldIndexDef.expireAfter_ = newIndexDef.expireAfter_;
			}
			indexIsSame = (newIndexDef == oldIndexDef);
		} else {
			indexIsSame = (indexDef == oldIndexDef);
		}
		if (indexIsSame) {
			return true;
		} else {
			throw Error(errConflict, "Index '%s.%s' already exists with different settings", name_, indexDef.name_);
		}
	}
	return false;
}

void NamespaceImpl::insertIndex(std::unique_ptr<Index> newIndex, int idxNo, const string &realName) {
	const bool isPK = newIndex->Opts().IsPK();
	indexes_.insert(indexes_.begin() + idxNo, std::move(newIndex));

	for (auto &n : indexesNames_) {
		if (n.second >= idxNo) {
			n.second++;
		}
	}

	indexesNames_.insert({realName, idxNo});

	if (isPK) {
		indexesNames_.insert({kPKIndexName, idxNo});
	}
}

int NamespaceImpl::getIndexByName(const string &index) const {
	auto idxIt = indexesNames_.find(index);

	if (idxIt == indexesNames_.end()) throw Error(errParams, "Index '%s' not found in '%s'", index, name_);

	return idxIt->second;
}

bool NamespaceImpl::getIndexByName(const string &name, int &index) const {
	auto it = indexesNames_.find(name);
	if (it == indexesNames_.end()) return false;
	index = it->second;
	return true;
}

void NamespaceImpl::Insert(Item &item, const RdxContext &ctx) { ModifyItem(item, ModeInsert, ctx); }

void NamespaceImpl::Update(Item &item, const RdxContext &ctx) { ModifyItem(item, ModeUpdate, ctx); }

void NamespaceImpl::Update(const Query &query, LocalQueryResults &result, const RdxContext &ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	UpdatesContainer pendedRepl;

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();

	doUpdate(query, result, pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::Upsert(Item &item, const RdxContext &ctx) { ModifyItem(item, ModeUpsert, ctx); }

void NamespaceImpl::Delete(Item &item, const RdxContext &ctx) { ModifyItem(item, ModeDelete, ctx); }

void NamespaceImpl::doDelete(IdType id) {
	assertrx(items_.exists(id));

	Payload pl(payloadType_, items_[id]);

	WrSerializer pk;
	pk << kStorageItemPrefix;
	pl.SerializeFields(pk, pkFields());

	repl_.dataHash ^= pl.GetHash();
	wal_.Set(WALRecord(), items_[id].GetLSN(), false);

	if (storage_) {
		try {
			std::unique_lock<std::mutex> lck(locker_.StorageLock());
			updates_->Remove(pk.Slice());
			unflushedCount_.fetch_add(1, std::memory_order_release);
		} catch (const Error &err) {
			if (err.code() != errNamespaceInvalidated) {
				throw;
			}
		}
	}

	// erase last item
	VariantArray skrefs;
	int field;

	// erase from composite indexes
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		bool needClearCache{false};
		indexes_[field]->Delete(Variant(items_[id]), id, *strHolder_, needClearCache);
		if (needClearCache && indexes_[field]->IsOrdered()) indexesCacheCleaner.Add(indexes_[field]->SortId());
	}

	// Holder for tuple. It is required for sparse indexes will be valid
	VariantArray tupleHolder(pl.Get(0, skrefs));

	// Deleteing fields from dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assertrx(indexes_.firstCompositePos() != 0);
	const int borderIdx = indexes_.totalSize() > 1 ? 1 : 0;
	field = borderIdx;
	do {
		field %= indexes_.firstCompositePos();

		Index &index = *indexes_[field];
		if (index.Opts().IsSparse()) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			pl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else {
			pl.Get(field, skrefs, index.Opts().IsArray());
		}
		// Delete value from index
		bool needClearCache{false};
		index.Delete(skrefs, id, *strHolder_, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
	} while (++field != borderIdx);

	// free PayloadValue
	itemsDataSize_ -= items_[id].GetCapacity() + sizeof(PayloadValue::dataHeader);
	items_[id].Free();
	free_.push_back(id);
	if (free_.size() == items_.size()) {
		free_.resize(0);
		items_.resize(0);
	}
	markUpdated(true);
}

void NamespaceImpl::removeIndex(std::unique_ptr<Index> &idx) {
	if (idx->HoldsStrings() && !(strHoldersWaitingToBeDeleted_.empty() && strHolder_.unique())) {
		strHolder_->Add(std::move(idx));
	}
}

void NamespaceImpl::doTruncate(UpdatesContainer &pendedRepl, const NsContext &ctx) {
	if (storage_) {
		for (PayloadValue &pv : items_) {
			if (pv.IsFree()) continue;
			Payload pl(payloadType_, pv);
			WrSerializer pk;
			pk << kStorageItemPrefix;
			pl.SerializeFields(pk, pkFields());
			try {
				std::unique_lock<std::mutex> lck(locker_.StorageLock());
				updates_->Remove(pk.Slice());
				unflushedCount_.fetch_add(1, std::memory_order_release);
			} catch (const Error &err) {
				if (err.code() != errNamespaceInvalidated) {
					throw;
				}
			}
		}
	}
	items_.clear();
	free_.clear();
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	for (size_t i = 0; i < indexes_.size(); ++i) {
		const IndexOpts opts = indexes_[i]->Opts();
		std::unique_ptr<Index> newIdx{Index::New(getIndexDefinition(i), indexes_[i]->GetPayloadType(), indexes_[i]->Fields())};
		newIdx->SetOpts(opts);
		std::swap(indexes_[i], newIdx);
		removeIndex(newIdx);
	}

	WrSerializer ser;
	WALRecord wrec(WalUpdateQuery, (ser << "TRUNCATE " << name_).Slice());

	wal_.Add(wrec, ctx.GetOriginLSN());
	markUpdated(true);

	pendedRepl.emplace_back(UpdateRecord::Type::Truncate, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId());
}

void NamespaceImpl::Delete(const Query &q, LocalQueryResults &result, const RdxContext &ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	UpdatesContainer pendedRepl;

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();
	calc.LockHit();

	doDelete(q, result, pendedRepl, NsContext(ctx));
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::ModifyItem(Item &item, int mode, const RdxContext &ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	UpdatesContainer pendedRepl;

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();
	calc.LockHit();
	modifyItem(item, mode, pendedRepl, NsContext(ctx));

	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::Truncate(const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();

	calc.LockHit();

	doTruncate(pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::Refill(vector<Item> &items, const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();

	assertrx(isSystem());  // TODO: Refill currently will not be replicated, so it's available for system ns only

	doTruncate(pendedRepl, ctx);
	NsContext nsCtx(ctx);
	for (Item &i : items) {
		doModifyItem(i, ModeUpsert, pendedRepl, nsCtx);
	}
}

ReplicationState NamespaceImpl::GetReplState(const RdxContext &ctx) const {
	auto rlck = rLock(ctx);
	return getReplState();
}

ReplicationStateV2 NamespaceImpl::GetReplStateV2(const RdxContext &ctx) const {
	ReplicationStateV2 state;
	auto rlck = rLock(ctx);
	state.lastLsn = wal_.LastLSN();
	state.dataHash = repl_.dataHash;
	state.nsVersion = repl_.nsVersion;
	state.clusterStatus = repl_.clusterStatus;
	return state;
}

ReplicationState NamespaceImpl::getReplState() const {
	ReplicationState ret = repl_;
	ret.dataCount = ItemsCount();
	ret.lastLsn = wal_.LastLSN();
	return ret;
}

LocalTransaction NamespaceImpl::NewTransaction(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return LocalTransaction(name_, payloadType_, tagsMatcher_, pkFields(), schema_, ctx.GetOriginLSN());
}

void NamespaceImpl::CommitTransaction(LocalTransaction &tx, LocalQueryResults &result, const NsContext &ctx) {
	Locker::WLockT wlck;
	if (!ctx.isCopiedNsRequest) {
		PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
		CounterGuardAIR32 cg(cancelCommitCnt_);
		wlck = dataWLock(ctx.rdxContext, true);
		cg.Reset();
		calc.LockHit();
	}

	checkClusterRole(ctx.rdxContext);  // Check request source. Throw exception if false
	if (!ctx.IsWalSyncItem()) {
		checkClusterStatus(tx.GetLSN());  // Check tx itself. Throw exception if false
	}

	logPrintf(LogTrace, "[repl:%s]:%d CommitTransaction start", name_, wal_.GetServer());

	{
		WALRecord initWrec(WalInitTransaction, 0, true);
		auto lsn = wal_.Add(initWrec, tx.GetLSN());
		if (!ctx.inSnapshot) {
			replicateAsync({UpdateRecord::Type::BeginTx, name_, lsn, repl_.nsVersion, ctx.rdxContext.EmmiterServerId()}, ctx.rdxContext);
		}
	}

	for (auto &&step : tx.GetSteps()) {
		UpdatesContainer pendedRepl;
		switch (step.type_) {
			case TransactionStep::Type::ModifyItem: {
				Item item = tx.GetItem(std::move(step));
				auto &data = std::get<TransactionItemStep>(step.data_);
				modifyItem(item, data.mode, pendedRepl, NsContext(ctx).InTransaction(step.lsn_));
				result.AddItem(item);
				break;
			}
			case TransactionStep::Type::Query: {
				LocalQueryResults qr;
				qr.AddNamespace(std::shared_ptr<NamespaceImpl>{this, [](NamespaceImpl *) {}}, true, ctx.rdxContext);
				auto &data = std::get<TransactionQueryStep>(step.data_);
				if (data.query->type_ == QueryDelete) {
					doDelete(*data.query, qr, pendedRepl, NsContext(ctx).InTransaction(step.lsn_));
				} else {
					doUpdate(*data.query, qr, pendedRepl, NsContext(ctx).InTransaction(step.lsn_));
				}
				break;
			}
			case TransactionStep::Type::Nop:
				assertrx(ctx.inSnapshot);
				wal_.Add(WALRecord(WalEmpty), step.lsn_);
				break;
			case TransactionStep::Type::PutMeta: {
				auto &data = std::get<TransactionMetaStep>(step.data_);
				putMeta(data.key, data.value, pendedRepl, NsContext(ctx).InTransaction(step.lsn_));
				break;
			}
			case TransactionStep::Type::SetTM: {
				auto &data = std::get<TransactionTmStep>(step.data_);
				auto tmCopy = data.tm;
				setTagsMatcher(std::move(tmCopy), pendedRepl, NsContext(ctx).InTransaction(step.lsn_));
				break;
			}
			default:
				std::abort();
		}

		if (!ctx.inSnapshot) {
			replicateAsync(std::move(pendedRepl), ctx.rdxContext);
		}
	}

	processWalRecord(WALRecord(WalCommitTransaction, 0, true), ctx);
	logPrintf(LogTrace, "[repl:%s]:%d CommitTransaction end", name_, wal_.GetServer());
	if (!ctx.inSnapshot && !ctx.isCopiedNsRequest) {
		// If commit happens in ns copy, than the copier have to handle replication
		UpdatesContainer pendedRepl;
		pendedRepl.emplace_back(UpdateRecord::Type::CommitTx, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId());
		replicate(std::move(pendedRepl), std::move(wlck), ctx);
	} else if (ctx.inSnapshot && ctx.isRequireResync) {
		replicateAsync(
			{ctx.isInitialLeaderSync ? UpdateRecord::Type::ResyncNamespaceLeaderInit : UpdateRecord::Type::ResyncNamespaceGeneric, name_,
			 lsn_t(0, 0), lsn_t(0, 0), ctx.rdxContext.EmmiterServerId()},
			ctx.rdxContext);
	}
}

void NamespaceImpl::doUpsert(ItemImpl *ritem, IdType id, bool doUpdate) {
	// Upsert fields to indexes
	assertrx(items_.exists(id));
	auto &plData = items_[id];

	// Inplace payload
	Payload pl(payloadType_, plData);
	Payload plNew = ritem->GetPayload();
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	Variant oldData;
	h_vector<bool, 32> needUpdateCompIndexes;
	if (doUpdate) {
		repl_.dataHash ^= pl.GetHash();
		itemsDataSize_ -= plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
		plData.Clone(pl.RealSize());
		const size_t compIndexesCount = indexes_.compositeIndexesSize();
		needUpdateCompIndexes = h_vector<bool, 32>(compIndexesCount, false);
		bool needUpdateAnyCompIndex = false;
		for (size_t field = 0; field < compIndexesCount; ++field) {
			const auto &fields = indexes_[field + indexes_.firstCompositePos()]->Fields();
			for (const auto f : fields) {
				if (f == IndexValueType::SetByJsonPath) continue;
				pl.Get(f, skrefs);
				plNew.Get(f, krefs);
				if (skrefs != krefs) {
					needUpdateCompIndexes[field] = true;
					needUpdateAnyCompIndex = true;
					break;
				}
			}
			if (needUpdateCompIndexes[field]) continue;
			for (size_t i = 0, end = fields.getTagsPathsLength(); i < end; ++i) {
				const auto &tp = fields.getTagsPath(i);
				pl.GetByJsonPath(tp, skrefs, KeyValueUndefined);
				plNew.GetByJsonPath(tp, krefs, KeyValueUndefined);
				if (skrefs != krefs) {
					needUpdateCompIndexes[field] = true;
					needUpdateAnyCompIndex = true;
					break;
				}
			}
		}
		if (needUpdateAnyCompIndex) {
			PayloadValue oldValue = plData;
			oldValue.Clone(pl.RealSize());
			oldData = Variant{oldValue};
		}
	}

	plData.SetLSN(ritem->Value().GetLSN());

	// Upserting fields to dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assertrx(indexes_.firstCompositePos() != 0);
	const int borderIdx = indexes_.totalSize() > 1 ? 1 : 0;
	int field = borderIdx;
	do {
		field %= indexes_.firstCompositePos();
		Index &index = *indexes_[field];
		bool isIndexSparse = index.Opts().IsSparse();
		if (isIndexSparse) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
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
			if (krefs == skrefs) continue;
			bool needClearCache{false};
			index.Delete(krefs, id, *strHolder_, needClearCache);
			if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
		}
		// Put value to index
		krefs.resize(0);
		bool needClearCache{false};
		index.Upsert(krefs, skrefs, id, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());

		if (!isIndexSparse) {
			// Put value to payload
			pl.Set(field, krefs);
		}
	} while (++field != borderIdx);

	// Upsert to composite indexes
	for (int field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		bool needClearCache{false};
		if (doUpdate) {
			if (!needUpdateCompIndexes[field - indexes_.firstCompositePos()]) continue;
			// Delete from composite indexes first
			indexes_[field]->Delete(oldData, id, *strHolder_, needClearCache);
		}
		indexes_[field]->Upsert(Variant{plData}, id, needClearCache);
		if (needClearCache && indexes_[field]->IsOrdered()) indexesCacheCleaner.Add(indexes_[field]->SortId());
	}
	repl_.dataHash ^= pl.GetHash();
	itemsDataSize_ += plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
	ritem->RealValue() = plData;
}

void NamespaceImpl::updateTagsMatcherFromItem(ItemImpl *ritem, const NsContext &ctx) {
	if (ritem->tagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name_, ritem->tagsMatcher().dump());
		if (!ctx.GetOriginLSN().isEmpty()) {
			throw Error(errLogic, "%s: Replicated item requires explicit tagsmatcher update: %s", name_, ritem->GetJSON());
		}
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
	} else if (ritem->tagsMatcher().isUpdated()) {
		ritem->tagsMatcher() = tagsMatcher_;
		ritem->tagsMatcher().setUpdated();
	}
}

void NamespaceImpl::modifyItem(Item &item, int mode, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	if (mode == ModeDelete) {
		deleteItem(item, pendedRepl, ctx);
	} else {
		doModifyItem(item, mode, pendedRepl, ctx);
	}
}

void NamespaceImpl::deleteItem(Item &item, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	ItemImpl *ritem = item.impl_;
	const auto oldTmV = tagsMatcher_.version();
	updateTagsMatcherFromItem(ritem, ctx);

	auto itItem = findByPK(ritem, ctx.rdxContext);
	IdType id = itItem.first;

	item.setID(-1);
	if (itItem.second || ctx.IsWalSyncItem()) {
		item.setID(id);

		WrSerializer cjson;
		ritem->GetCJSON(cjson, false);
		WALRecord wrec{WalItemModify, cjson.Slice(), ritem->tagsMatcher().version(), ModeDelete, ctx.inTransaction};

		if (itItem.second) {
			ritem->RealValue() = items_[id];
			doDelete(id);
		}

		replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);

		lsn_t itemLsn(item.GetLSN());
		processWalRecord(std::move(wrec), ctx, itemLsn, &item);

		pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemDeleteTx : UpdateRecord::Type::ItemDelete, name_,
								wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
	}
}

void NamespaceImpl::doModifyItem(Item &item, int mode, UpdatesContainer &pendedRepl, const NsContext &ctx, IdType suggestedId) {
	// Item to doUpsert
	assertrx(mode != ModeDelete);
	const auto oldTmV = tagsMatcher_.version();
	ItemImpl *itemImpl = item.impl_;
	setFieldsBasedOnPrecepts(itemImpl, pendedRepl, ctx);
	updateTagsMatcherFromItem(itemImpl, ctx);
	auto newPl = itemImpl->GetPayload();

	auto realItem = findByPK(itemImpl, ctx.rdxContext);
	const bool exists = realItem.second;

	if ((exists && mode == ModeInsert) || (!exists && mode == ModeUpdate)) {
		item.setID(-1);
		replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
		return;
	}

	if (suggestedId >= 0 && exists && suggestedId != realItem.first) {
		throw Error(errParams, "Suggested ID doesn't correspond to real ID: %d vs %d", suggestedId, realItem.first);
	}
	const IdType id = exists ? realItem.first : createItem(newPl.RealSize(), suggestedId);

	replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
	lsn_t lsn;
	if (ctx.IsForceSyncItem()) {
		lsn = ctx.GetOriginLSN();
	} else {
		lsn = wal_.Add(WALRecord(WalItemUpdate, id, ctx.inTransaction), ctx.GetOriginLSN(), exists ? lsn_t(items_[id].GetLSN()) : lsn_t());
	}
	assertrx(!lsn.isEmpty());

	item.setLSN(lsn);
	item.setID(id);
	doUpsert(itemImpl, id, exists);

	WrSerializer cjson;
	item.impl_->GetCJSON(cjson, false);

	if (storage_) {
		saveTagsMatcherToStorage();

		WrSerializer pk, data;
		pk << kStorageItemPrefix;
		newPl.SerializeFields(pk, pkFields());
		data.PutUInt64(int64_t(lsn));
		data.Write(cjson.Slice());
		writeToStorage(pk.Slice(), data.Slice());
	}

	markUpdated(!exists);

	switch (mode) {
		case ModeUpdate:
			pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemUpdateTx : UpdateRecord::Type::ItemUpdate, name_, lsn,
									repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
			break;
		case ModeInsert:
			pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemInsertTx : UpdateRecord::Type::ItemInsert, name_, lsn,
									repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
			break;
		case ModeUpsert:
			pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemUpsertTx : UpdateRecord::Type::ItemUpsert, name_, lsn,
									repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
			break;
		case ModeDelete:
			pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemDeleteTx : UpdateRecord::Type::ItemDelete, name_, lsn,
									repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
			break;
	}
}

PayloadType NamespaceImpl::getPayloadType(const RdxContext &ctx) const {
	auto rlck = rLock(ctx);
	return payloadType_;
}

// find id by PK. NOT THREAD SAFE!
pair<IdType, bool> NamespaceImpl::findByPK(ItemImpl *ritem, const RdxContext &ctx) {
	auto pkIndexIt = indexesNames_.find(kPKIndexName);

	if (pkIndexIt == indexesNames_.end()) {
		throw Error(errLogic, "Trying to modify namespace '%s', but it doesn't contain PK index", name_);
	}
	Index *pkIndex = indexes_[pkIndexIt->second].get();

	Payload pl = ritem->GetPayload();
	// It is a faster alternative of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	VariantArray krefs;
	if (IsComposite(pkIndex->Type())) {
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

void NamespaceImpl::optimizeIndexes(const NsContext &ctx) {
	static const auto kHardwareConcurrency = std::thread::hardware_concurrency();
	// This is read lock only atomics based implementation of rebuild indexes
	// If optimizationState_ == OptimizationCompleted is true, then indexes are completely built.
	// In this case reset optimizationState_ and/or any idset's and sort orders builds are allowed only protected by write lock
	if (optimizationState_.load(std::memory_order_relaxed) == OptimizationCompleted) return;
	int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	auto lastUpdateTime = lastUpdateTime_.load(std::memory_order_acquire);

	Locker::RLockT rlck;
	if (!ctx.isCopiedNsRequest) {
		rlck = rLock(ctx.rdxContext);
	}

	if (isSystem()) {
		return;
	}
	if (!lastUpdateTime || !config_.optimizationTimeout) {
		return;
	}
	if (!ctx.isCopiedNsRequest && now - lastUpdateTime < config_.optimizationTimeout) {
		return;
	}

	if (!indexes_.size()) {
		return;
	}

	const auto optState{optimizationState_.load(std::memory_order_acquire)};
	if (optState == OptimizationCompleted || cancelCommitCnt_.load(std::memory_order_relaxed)) return;
	const bool forceBuildAllIndexes = optState == NotOptimized;

	logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) enter", name_);
	assertrx(indexes_.firstCompositePos() != 0);
	int field = indexes_.firstCompositePos();
	do {
		field %= indexes_.totalSize();
		PerfStatCalculatorMT calc(indexes_[field]->GetCommitPerfCounter(), enablePerfCounters_);
		calc.LockHit();
		indexes_[field]->Commit();
	} while (++field != indexes_.firstCompositePos() && !cancelCommitCnt_.load(std::memory_order_relaxed));

	// Update sort orders and sort_id for each index
	size_t currentSortId = 1;
	const size_t maxIndexWorkers = kHardwareConcurrency
									   ? std::min<size_t>(std::thread::hardware_concurrency(), config_.optimizationSortWorkers)
									   : config_.optimizationSortWorkers;
	for (auto &idxIt : indexes_) {
		if (idxIt->IsOrdered() && maxIndexWorkers != 0) {
			NSUpdateSortedContext sortCtx(*this, currentSortId++);
			const bool forceBuildAll = forceBuildAllIndexes || idxIt->IsBuilt() || idxIt->SortId() != currentSortId;
			idxIt->MakeSortOrders(sortCtx);
			// Build in multiple threads
			std::unique_ptr<thread[]> thrs(new thread[maxIndexWorkers]);
			for (size_t i = 0; i < maxIndexWorkers; i++) {
				thrs[i] = std::thread([&, i]() {
					for (size_t j = i; j < this->indexes_.size() && !cancelCommitCnt_.load(std::memory_order_relaxed);
						 j += maxIndexWorkers) {
						auto &idx = this->indexes_[j];
						if (forceBuildAll || !idx->IsBuilt()) {
							idx->UpdateSortedIds(sortCtx);
						}
					}
				});
			}
			for (size_t i = 0; i < maxIndexWorkers; i++) thrs[i].join();
		}
		if (cancelCommitCnt_.load(std::memory_order_relaxed)) break;
	}
	if (maxIndexWorkers && !cancelCommitCnt_.load(std::memory_order_relaxed)) {
		optimizationState_.store(OptimizationCompleted, std::memory_order_release);
		for (auto &idxIt : indexes_) {
			if (!idxIt->IsFulltext()) {
				idxIt->MarkBuilt();
			}
		}
	}
	if (cancelCommitCnt_.load(std::memory_order_relaxed)) {
		logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) done", name_);
	} else {
		lastUpdateTime_.store(0, std::memory_order_release);
		logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) was cancelled by concurent update", name_);
	}
}

void NamespaceImpl::markUpdated(bool forceOptimizeAllIndexes) {
	using namespace std::string_view_literals;
	itemsCount_.store(items_.size(), std::memory_order_relaxed);
	itemsCapacity_.store(items_.capacity(), std::memory_order_relaxed);
	if (forceOptimizeAllIndexes) {
		optimizationState_.store(NotOptimized);
	} else {
		int expected{OptimizationCompleted};
		optimizationState_.compare_exchange_strong(expected, OptimizedPartially);
	}
	queryCache_->Clear();
	joinCache_->Clear();
	lastUpdateTime_.store(
		std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count(),
		std::memory_order_release);
	if (!nsIsLoading_) {
		repl_.updatedUnixNano = getTimeNow("nsec"sv);
	}
}

Item NamespaceImpl::newItem() {
	auto impl_ = pool_.get(0, payloadType_, tagsMatcher_, pkFields(), schema_);
	impl_->tagsMatcher() = tagsMatcher_;
	impl_->tagsMatcher().clearUpdated();
	return Item(impl_.release());
}

void NamespaceImpl::doUpdate(const Query &query, LocalQueryResults &result, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	NsSelecter selecter(this);
	SelectCtx selCtx(query, nullptr);
	SelectFunctionsHolder func;
	selCtx.functions = &func;
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.inTransaction;
	selecter(result, selCtx, ctx.rdxContext);

	const auto tmStart = high_resolution_clock::now();

	bool updateWithJson = false;
	bool withExpressions = false;
	for (const UpdateEntry &ue : query.UpdateFields()) {
		if (!withExpressions && ue.isExpression) withExpressions = true;
		if (!updateWithJson && ue.mode == FieldModeSetJson) updateWithJson = true;
		if (withExpressions && updateWithJson) break;
	}

	if (!ctx.rdxContext.GetOriginLSN().isEmpty() && withExpressions)
		throw Error(errLogic, "Can't apply update query with expression to follower's ns '%s'", name_);

	if (!ctx.inTransaction) ThrowOnCancel(ctx.rdxContext);

	// If update statement is expression and contains function calls then we use
	// row-based replication (to preserve data consistency), otherwise we update
	// it via 'WalUpdateQuery' (statement-based replication). If Update statement
	// contains update of entire object (via JSON) then statement replication is not possible.
	// bool statementReplication =
	// 	(!updateWithJson && !withExpressions && !query.HasLimit() && !query.HasOffset() && (result.Count() >= kWALStatementItemsThreshold));
	constexpr bool statementReplication = false;

	ItemModifier itemModifier(query.UpdateFields(), *this, pendedRepl, ctx);
	pendedRepl.reserve(pendedRepl.size() + result.Items().size());	// Required for item-based replication only
	for (ItemRef &item : result.Items()) {
		assertrx(items_.exists(item.Id()));
		const auto oldTmV = tagsMatcher_.version();
		PayloadValue &pv(items_[item.Id()]);
		Payload pl(payloadType_, pv);
		uint64_t oldPlHash = pl.GetHash();
		size_t oldItemCapacity = pv.GetCapacity();
		pv.Clone(pl.RealSize());
		itemModifier.Modify(item.Id(), ctx.rdxContext, pendedRepl);
		replicateItem(item.Id(), ctx, statementReplication, oldPlHash, oldItemCapacity, oldTmV, pendedRepl);
		item.Value() = items_[item.Id()];
	}
	result.getTagsMatcher(0) = tagsMatcher_;
	assertrx(result.IsNamespaceAdded(this));

	// Disabled due to statement base replication logic conflicts
	// lsn_t lsn;
	//	if (statementReplication) {
	//		WrSerializer ser;
	//		const_cast<Query &>(query).type_ = QueryUpdate;
	//		WALRecord wrec(WalUpdateQuery, query.GetSQL(ser).Slice(), ctx.inTransaction);
	//		lsn = wal_.Add(wrec, ctx.GetOriginLSN());
	//		if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;
	//		for (ItemRef &item : result.Items()) {
	//			item.Value().SetLSN(lsn);
	//		}
	//		if (!repl_.temporary)
	//			observers_->OnWALUpdate(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_, wrec);
	//		if (!ctx.rdxContext.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
	//	}

	if (query.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Updated %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
	//	if (statementReplication) {
	//		assertrx(!lsn.isEmpty());
	//		pendedRepl.emplace_back(UpdateRecord::Type::UpdateQuery, name_, lsn, query);
	//	}
}

void NamespaceImpl::replicateItem(IdType itemId, const NsContext &ctx, bool statementReplication, uint64_t oldPlHash,
								  size_t oldItemCapacity, int oldTmVersion, UpdatesContainer &pendedRepl) {
	PayloadValue &pv(items_[itemId]);
	Payload pl(payloadType_, pv);

	if (!statementReplication) {
		replicateTmUpdateIfRequired(pendedRepl, oldTmVersion, ctx);
		lsn_t lsn;
		if (ctx.IsForceSyncItem()) {
			lsn = ctx.GetOriginLSN();
		} else {
			lsn = wal_.Add(WALRecord(WalItemUpdate, itemId, ctx.inTransaction), lsn_t(), lsn_t(items_[itemId].GetLSN()));
		}
		assertrx(!lsn.isEmpty());

		pv.SetLSN(lsn);
		ItemImpl item(payloadType_, pv, tagsMatcher_);

		WrSerializer cjson;
		item.GetCJSON(cjson, false);
		pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemUpdateTx : UpdateRecord::Type::ItemUpdate, name_, lsn,
								repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
	}

	repl_.dataHash ^= oldPlHash;
	repl_.dataHash ^= pl.GetHash();
	itemsDataSize_ -= oldItemCapacity;
	itemsDataSize_ += pl.Value()->GetCapacity();

	saveTagsMatcherToStorage();
	if (storage_) {
		WrSerializer pk;
		WrSerializer data;
		pk << "I";
		pl.SerializeFields(pk, pkFields());
		data.PutUInt64(int64_t(pv.GetLSN()));
		ItemImpl item(payloadType_, pv, tagsMatcher_);
		item.GetCJSON(data);
		writeToStorage(pk.Slice(), data.Slice());
	}
}

void NamespaceImpl::doDelete(const Query &q, LocalQueryResults &result, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	NsSelecter selecter(this);
	SelectCtx selCtx(q, nullptr);
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.inTransaction;
	SelectFunctionsHolder func;
	selCtx.functions = &func;
	selecter(result, selCtx, ctx.rdxContext);
	assertrx(result.IsNamespaceAdded(this));

	const auto tmStart = high_resolution_clock::now();
	const auto oldTmV = tagsMatcher_.version();
	for (auto &r : result.Items()) {
		doDelete(r.Id());
	}

	if (ctx.IsWalSyncItem() || (!q.HasLimit() && !q.HasOffset() && result.Count() >= kWALStatementItemsThreshold)) {
		WrSerializer ser;
		const_cast<Query &>(q).type_ = QueryDelete;
		processWalRecord(WALRecord(WalUpdateQuery, q.GetSQL(ser).Slice(), ctx.inTransaction), ctx);
		pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::DeleteQueryTx : UpdateRecord::Type::DeleteQuery, name_,
								wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::string(ser.Slice()));
	} else {
		replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
		if (result.Count() > 0) {
			WrSerializer cjson;
			for (auto it : result) {
				cjson.Reset();
				it.GetCJSON(cjson, false);
				const int id = it.GetItemRef().Id();
				processWalRecord(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete, ctx.inTransaction), ctx,
								 lsn_t(items_[id].GetLSN()));
				pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemDeleteTx : UpdateRecord::Type::ItemDelete, name_,
										wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
			}
		}
	}
	if (q.debugLevel >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void NamespaceImpl::updateSelectTime() {
	lastSelectTime_ = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

void NamespaceImpl::checkClusterRole(lsn_t originLsn) const {
	switch (repl_.clusterStatus.role) {
		case ClusterizationStatus::Role::None:
			if (!originLsn.isEmpty()) {
				throw Error(errWrongReplicationData, "Can't modify ns '%s' with 'None' replication status from node %d", name_,
							originLsn.Server());
			}
			break;
		case ClusterizationStatus::Role::SimpleReplica:
			if (originLsn.isEmpty()) {
				throw Error(errWrongReplicationData, "Can't modify replica's ns '%s' without origin LSN", name_, originLsn.Server());
			}
			break;
		case ClusterizationStatus::Role::ClusterReplica:
			if (originLsn.isEmpty() || originLsn.Server() != repl_.clusterStatus.leaderId) {
				throw Error(errWrongReplicationData, "Can't modify cluster ns '%s' with incorrect origin LSN: (%d) (s1:%d s2:%d)", name_,
							originLsn, originLsn.Server(), repl_.clusterStatus.leaderId);
			}
			break;
	}
}

void NamespaceImpl::checkClusterStatus(lsn_t originLsn) const {
	checkClusterRole(originLsn);
	if (!originLsn.isEmpty() && wal_.LSNCounter() != originLsn.Counter()) {
		throw Error(errWrongReplicationData, "Can't modify cluster ns '%s' with incorrect origin LSN: (%d). Expected counter value: (%d)",
					name_, originLsn, wal_.LSNCounter());
	}
}

void NamespaceImpl::replicateTmUpdateIfRequired(UpdatesContainer &pendedRepl, int oldTmVersion, const NsContext &ctx) noexcept {
	if (oldTmVersion != tagsMatcher_.version()) {
		assertrx(ctx.GetOriginLSN().isEmpty());
		const auto lsn = wal_.Add(WALRecord(WalEmpty, 0, ctx.inTransaction), lsn_t());
		pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::SetTagsMatcherTx : UpdateRecord::Type::SetTagsMatcher, name_, lsn,
								repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), tagsMatcher_);
	}
}

void NamespaceImpl::Select(LocalQueryResults &result, SelectCtx &params, const RdxContext &ctx) {
	if (params.query.IsWALQuery()) {
		WALSelecter selecter(this, true);
		selecter(result, params);
	} else {
		NsSelecter selecter(this);
		selecter(result, params, ctx);
	}
}

IndexDef NamespaceImpl::getIndexDefinition(size_t i) const {
	assertrx(i < indexes_.size());
	IndexDef indexDef;
	const Index &index = *indexes_[i];
	indexDef.name_ = index.Name();
	indexDef.opts_ = index.Opts();
	indexDef.FromType(index.Type());
	indexDef.expireAfter_ = index.GetTTLValue();

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

NamespaceDef NamespaceImpl::getDefinition() const {
	auto pt = this->payloadType_;
	NamespaceDef nsDef(name_, StorageOpts().Enabled(!dbpath_.empty()));
	nsDef.indexes.reserve(indexes_.size());
	for (size_t i = 1; i < indexes_.size(); ++i) {
		nsDef.AddIndex(getIndexDefinition(i));
	}
	nsDef.isTemporary = repl_.temporary;
	if (schema_) {
		WrSerializer ser;
		schema_->GetJSON(ser);
		nsDef.schemaJson = string(ser.Slice());
	}
	return nsDef;
}

NamespaceDef NamespaceImpl::GetDefinition(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return getDefinition();
}

NamespaceMemStat NamespaceImpl::GetMemStat(const RdxContext &ctx) {
	NamespaceMemStat ret;
	auto rlck = rLock(ctx);
	ret.name = name_;
	ret.joinCache = joinCache_->GetMemStat();
	ret.queryCache = queryCache_->GetMemStat();

	ret.itemsCount = ItemsCount();
	*(static_cast<ReplicationState *>(&ret.replication)) = getReplState();
	ret.replication.walCount = size_t(wal_.size());
	ret.replication.walSize = wal_.heap_size();

	ret.emptyItemsCount = free_.size();

	ret.Total.dataSize = itemsDataSize_ + items_.capacity() * sizeof(PayloadValue);
	ret.Total.cacheSize = ret.joinCache.totalSize + ret.queryCache.totalSize;

	ret.indexes.reserve(indexes_.size());
	for (auto &idx : indexes_) {
		ret.indexes.emplace_back(idx->GetMemStat());
		auto &istat = ret.indexes.back();
		istat.sortOrdersSize = idx->IsOrdered() ? (items_.size() * sizeof(IdType)) : 0;
		ret.Total.indexesSize += istat.GetIndexStructSize();
		ret.Total.dataSize += istat.dataSize;
		ret.Total.cacheSize += istat.idsetCache.totalSize;
	}

	ret.storageOK = storage_ != nullptr;
	ret.storagePath = dbpath_;
	ret.optimizationCompleted = (optimizationState_ == OptimizationCompleted);

	ret.stringsWaitingToBeDeletedSize = strHolder_->MemStat();
	for (const auto &idx : strHolder_->Indexes()) {
		const auto &istat = idx->GetMemStat();
		ret.stringsWaitingToBeDeletedSize += istat.GetIndexStructSize() + istat.dataSize;
	}
	for (const auto &strHldr : strHoldersWaitingToBeDeleted_) {
		ret.stringsWaitingToBeDeletedSize += strHldr->MemStat();
		for (const auto &idx : strHldr->Indexes()) {
			const auto &istat = idx->GetMemStat();
			ret.stringsWaitingToBeDeletedSize += istat.GetIndexStructSize() + istat.dataSize;
		}
	}

	logPrintf(LogTrace, "[GetMemStat:%s]:%d replication (dataHash=%ld  dataCount=%d  lastLsn=%s)", ret.name, wal_.GetServer(),
			  ret.replication.dataHash, ret.replication.dataCount, ret.replication.lastLsn);

	return ret;
}

NamespacePerfStat NamespaceImpl::GetPerfStat(const RdxContext &ctx) {
	auto rlck = rLock(ctx);

	NamespacePerfStat ret;
	ret.name = name_;
	ret.selects = selectPerfCounter_.Get<PerfStat>();
	ret.updates = updatePerfCounter_.Get<PerfStat>();
	for (unsigned i = 1; i < indexes_.size(); i++) {
		ret.indexes.emplace_back(indexes_[i]->GetIndexPerfStat());
	}
	return ret;
}

void NamespaceImpl::ResetPerfStat(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	selectPerfCounter_.Reset();
	updatePerfCounter_.Reset();
	for (auto &i : indexes_) i->ResetIndexPerfStat();
}

Error NamespaceImpl::loadLatestSysRecord(std::string_view baseSysTag, uint64_t &version, string &content) {
	std::string key(baseSysTag);
	key.append(".");
	std::string latestContent;
	version = 0;
	Error err = errOK;
	for (int i = 0; i < kSysRecordsBackupCount; ++i) {
		Error status = storage_->Read(StorageOpts().FillCache(), std::string_view(key + std::to_string(i)), content);
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
			ser.Write(std::string_view(content));
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

bool NamespaceImpl::loadIndexesFromStorage() {
	// Check if indexes structures are ready.
	assertrx(indexes_.size() == 1);
	assertrx(items_.size() == 0);

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
	status = loadLatestSysRecord(kStorageSchemaPrefix, sysRecordsVersions_.schemaVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (def.size()) {
		schema_ = make_shared<Schema>();
		Serializer ser(def.data(), def.size());
		status = schema_->FromJSON(ser.GetSlice());
		if (!status.ok()) {
			throw status;
		}
		logPrintf(LogTrace, "Loaded schema(version: %lld) of namespace %s",
				  sysRecordsVersions_.schemaVersion ? sysRecordsVersions_.schemaVersion - 1 : 0, name_);
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
			std::string_view indexData = ser.GetVString();
			Error err = indexDef.FromJSON(giftStr(indexData));
			if (err.ok()) {
				try {
					addIndex(indexDef, false);
				} catch (const Error &e) {
					err = e;
				}
			}
			if (!err.ok()) {
				logPrintf(LogError, "Error adding index '%s': %s", indexDef.name_, err.what());
			}
		}
	}

	if (schema_) schema_->BuildProtobufSchema(tagsMatcher_, payloadType_);

	logPrintf(LogTrace, "Loaded index structure(version %lld) of namespace '%s'\n%s",
			  sysRecordsVersions_.idxVersion ? sysRecordsVersions_.idxVersion - 1 : 0, name_, payloadType_->ToString());

	return true;
}

void NamespaceImpl::loadReplStateFromStorage() {
	string json;
	Error status = loadLatestSysRecord(kStorageReplStatePrefix, sysRecordsVersions_.replVersion, json);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}

	if (json.size()) {
		logPrintf(LogTrace, "[load_repl:%s]:%d Loading replication state(version %lld) of namespace %s: %s", name_, wal_.GetServer(),
				  sysRecordsVersions_.replVersion ? sysRecordsVersions_.replVersion - 1 : 0, name_, json);
		repl_.FromJSON(giftStr(json));
	}
	{
		WrSerializer ser_log;
		JsonBuilder builder_log(ser_log, ObjType::TypePlain);
		repl_.GetJSON(builder_log);
		logPrintf(LogTrace, "[load_repl:%s]:%d Loading replication state %s", name_, wal_.GetServer(), ser_log.c_str());
	}
}

void NamespaceImpl::saveIndexesToStorage() {
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

	WrSerializer wrser;
	for (const IndexDef &indexDef : nsDef.indexes) {
		wrser.Reset();
		indexDef.GetJSON(wrser);
		ser.PutVString(wrser.Slice());
	}

	writeSysRecToStorage(ser.Slice(), kStorageIndexesPrefix, sysRecordsVersions_.idxVersion, true);

	saveTagsMatcherToStorage();
	saveReplStateToStorage();
}

void NamespaceImpl::saveSchemaToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveSchemaToStorage (%s)", name_);

	if (!schema_) return;

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.schemaVersion);
	{
		auto sliceHelper = ser.StartSlice();
		schema_->GetJSON(ser);
	}

	writeSysRecToStorage(ser.Slice(), kStorageSchemaPrefix, sysRecordsVersions_.schemaVersion, true);

	saveTagsMatcherToStorage();
	saveReplStateToStorage();
}

void NamespaceImpl::saveTagsMatcherToStorage() {
	if (storage_ && tagsMatcher_.isUpdated()) {
		WrSerializer ser;
		ser.PutUInt64(sysRecordsVersions_.tagsVersion);
		tagsMatcher_.serialize(ser);
		tagsMatcher_.clearUpdated();
		writeSysRecToStorage(ser.Slice(), kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, false);
		logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
	}
}

void NamespaceImpl::saveReplStateToStorage() {
	if (!storage_) return;

	logPrintf(LogTrace, "Namespace::saveReplStateToStorage (%s)", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.replVersion);
	JsonBuilder builder(ser);
	ReplicationState st = getReplState();
	st.GetJSON(builder);
	builder.End();
	writeSysRecToStorage(ser.Slice(), kStorageReplStatePrefix, sysRecordsVersions_.replVersion, true);
}

void NamespaceImpl::EnableStorage(const string &path, StorageOpts opts, StorageType storageType, const RdxContext &ctx) {
	string dbpath = fs::JoinPath(path, name_);

	auto wlck = simpleWLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", name_, path);
	}

	bool success = false;
	const bool storageDirExists = (fs::Stat(dbpath) == fs::StatDir);
	try {
		while (!success) {
			if (!opts.IsCreateIfMissing() && !storageDirExists) {
				throw Error(errNotFound,
							"Storage directory doesn't exist for namespace '%s' on path '%s' and CreateIfMissing option is not set", name_,
							path);
			}
			storage_.reset(datastorage::StorageFactory::create(storageType));
			Error status = storage_->Open(dbpath, opts);
			if (!status.ok()) {
				if (!opts.IsDropOnFileFormatError()) {
					storage_.reset();
					throw Error(errLogic, "Cannot enable storage for namespace '%s' on path '%s' - %s", name_, path, status.what());
				}
			} else {
				success = loadIndexesFromStorage();
				if (!success && !opts.IsDropOnFileFormatError()) {
					storage_.reset();
					throw Error(errLogic, "Cannot enable storage for namespace '%s' on path '%s': format error", name_, dbpath);
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
	} catch (...) {
		// if storage was created by this call
		if (!storageDirExists && (fs::Stat(dbpath) == fs::StatDir)) {
			logPrintf(LogWarning, "Dropping storage (via %s), which was created with errors ('%s':'%s')",
					  storage_ ? "storage interface" : "filesystem", name_, dbpath);
			if (storage_) {
				storage_->Destroy(dbpath);
				storage_.reset();
			} else {
				fs::RmDirAll(dbpath);
			}
		}
		throw;
	}

	storageOpts_ = opts;
	updates_.reset(storage_->GetUpdatesCollection());
	dbpath_ = dbpath;
}

StorageOpts NamespaceImpl::GetStorageOpts(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return storageOpts_;
}

std::shared_ptr<const Schema> NamespaceImpl::GetSchemaPtr(const RdxContext &ctx) const {
	auto rlck = rLock(ctx);
	return schema_;
}

Error NamespaceImpl::SetClusterizationStatus(ClusterizationStatus &&status, const RdxContext &ctx) {
	auto wlck = simpleWLock(ctx);
	if (repl_.temporary && status.role == ClusterizationStatus::Role::None) {
		return Error(errParams, "Unable to set replication role 'none' to temporary namespace");
	}
	repl_.clusterStatus = std::move(status);
	saveReplStateToStorage();
	return Error();
}

void NamespaceImpl::ApplySnapshotChunk(const SnapshotChunk &ch, bool isInitialLeaderSync, const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	SnapshotHandler handler(*this);
	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx, true);
	cg.Reset();
	checkClusterRole(ctx);

	handler.ApplyChunk(ch, isInitialLeaderSync, pendedRepl);
	if (ch.IsLastChunk()) {
		replicateAsync({isInitialLeaderSync ? UpdateRecord::Type::ResyncNamespaceLeaderInit : UpdateRecord::Type::ResyncNamespaceGeneric,
						name_, lsn_t(0, 0), lsn_t(0, 0), ctx.EmmiterServerId()},
					   ctx);
	}
}

void NamespaceImpl::GetSnapshot(Snapshot &snapshot, const SnapshotOpts &opts, const RdxContext &ctx) {
	SnapshotHandler handler(*this);
	auto rlck = rLock(ctx);
	snapshot = handler.CreateSnapshot(opts);
}

void NamespaceImpl::SetTagsMatcher(TagsMatcher &&tm, const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);
	setTagsMatcher(std::move(tm), pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

void NamespaceImpl::LoadFromStorage(const RdxContext &ctx) {
	auto wlck = simpleWLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	StorageOpts opts;
	opts.FillCache(false);
	size_t ldcount = 0;
	logPrintf(LogTrace, "Loading items to '%s' from storage", name_);
	std::unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	ItemImpl item(payloadType_, tagsMatcher_);
	item.Unsafe(true);
	int errCount = 0;
	int64_t maxLSN = -1;
	int64_t minLSN = std::numeric_limits<int64_t>::max();
	Error lastErr = errOK;

	uint64_t dataHash = repl_.dataHash;
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	for (dbIter->Seek(kStorageItemPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageItemPrefix "\xFF")) < 0;
		 dbIter->Next()) {
		std::string_view dataSlice = dbIter->Value();
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
			assertrx(lsn >= 0);
			lsn_t l(lsn);
			if (isSystem()) {
				l.SetServer(0);
			}

			maxLSN = std::max(maxLSN, l.Counter());
			minLSN = std::min(minLSN, l.Counter());
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
			item.Value().SetLSN(l);

			doUpsert(&item, id, false);
			ldcount += dataSlice.size();
		}
	}

	initWAL(minLSN, maxLSN);
	if (!isSystem()) {
		repl_.lastLsn.SetServer(wal_.GetServer());
	}

	logPrintf(LogInfo, "[%s] Done loading storage. %d items loaded (%d errors %s), lsn #%s, total size=%dM, dataHash=%ld", name_,
			  items_.size(), errCount, lastErr.what(), repl_.lastLsn, ldcount / (1024 * 1024), repl_.dataHash);
	if (dataHash != repl_.dataHash) {
		logPrintf(LogError, "[%s] Warning dataHash mismatch %lu != %lu", name_, dataHash, repl_.dataHash);
		unflushedCount_.fetch_add(1, std::memory_order_release);
	}

	markUpdated(true);
}

void NamespaceImpl::initWAL(int64_t minLSN, int64_t maxLSN) {
	wal_.Init(config_.walSize, minLSN, maxLSN, storage_);
	// Fill existing records
	for (IdType rowId = 0; rowId < IdType(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			wal_.Set(WALRecord(WalItemUpdate, rowId), items_[rowId].GetLSN(), true);
		}
	}
	repl_.lastLsn = wal_.LastLSN();
	logPrintf(LogInfo, "[%s] WAL has been initalized lsn #%s, max size %ld", name_, repl_.lastLsn, wal_.Capacity());
}

void NamespaceImpl::removeExpiredItems(RdxActivityContext *ctx) {
	const RdxContext rdxCtx{ctx};
	{
		auto rlck = rLock(rdxCtx);
		if (repl_.clusterStatus.role != ClusterizationStatus::Role::None) {
			return;
		}
	}
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(rdxCtx);
	const auto now = std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch());
	if (now == lastExpirationCheckTs_) {
		return;
	}
	lastExpirationCheckTs_ = now;
	for (const std::unique_ptr<Index> &index : indexes_) {
		if ((index->Type() != IndexTtl) || (index->Size() == 0)) continue;
		const int64_t expirationthreshold =
			std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count() -
			index->GetTTLValue();
		LocalQueryResults qr;
		qr.AddNamespace(std::shared_ptr<NamespaceImpl>{this, [](NamespaceImpl *) {}}, true, ctx);
		doDelete(Query(name_).Where(index->Name(), CondLt, expirationthreshold), qr, pendedRepl, NsContext(ctx));
	}
	replicate(std::move(pendedRepl), std::move(wlck), RdxContext(ctx));
}

void NamespaceImpl::removeExpiredStrings(RdxActivityContext *ctx) {
	auto wlck = simpleWLock(RdxContext{ctx});
	while (!strHoldersWaitingToBeDeleted_.empty()) {
		if (strHoldersWaitingToBeDeleted_.front().unique()) {
			strHoldersWaitingToBeDeleted_.pop_front();
		} else {
			break;
		}
	}
	if (strHoldersWaitingToBeDeleted_.empty() && strHolder_.unique()) {
		strHolder_->Clear();
	} else if (strHolder_->HoldsIndexes() || strHolder_->MemStat() > kMaxMemorySizeOfStringsHolder) {
		strHoldersWaitingToBeDeleted_.push_back(std::move(strHolder_));
		strHolder_ = makeStringsHolder();
	}
}

void NamespaceImpl::setSchema(std::string_view schema, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	schema_ = make_shared<Schema>(schema);
	auto fields = schema_->GetPaths();
	for (auto &field : fields) {
		tagsMatcher_.path2tag(field, true);
	}

	schema_->BuildProtobufSchema(tagsMatcher_, payloadType_);

	addToWAL(schema, WalSetSchema, ctx);
	pendedRepl.emplace_back(UpdateRecord::Type::SetSchema, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(),
							std::string(schema));
}

void NamespaceImpl::setTagsMatcher(TagsMatcher &&tm, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	if (ctx.GetOriginLSN().isEmpty()) {
		throw Error(errLogic, "Tagsmatcher may be set by replication only");
	}
	if (tm.stateToken() != tagsMatcher_.stateToken()) {
		throw Error(errParams, "Tagsmatcher have different statetokens: %08X vs %08X", tagsMatcher_.stateToken(), tm.stateToken());
	}
	tagsMatcher_ = tm;
	tagsMatcher_.UpdatePayloadType(payloadType_, false);
	tagsMatcher_.setUpdated();

	const auto lsn = wal_.Add(WALRecord(WalEmpty, 0, ctx.inTransaction), ctx.GetOriginLSN());
	pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::SetTagsMatcherTx : UpdateRecord::Type::SetTagsMatcher, name_, lsn,
							repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(tm));

	saveTagsMatcherToStorage();
}

StringsHolderPtr NamespaceImpl::StrHolder(bool noLock, const RdxContext &ctx) {
	assertrx(noLock);
	Locker::RLockT rlck;
	if (!noLock) {
		rlck = rLock(ctx);
	}
	StringsHolderPtr ret{strHolder_};
	return ret;
}

void NamespaceImpl::BackgroundRoutine(RdxActivityContext *ctx) {
	flushStorage(ctx);
	optimizeIndexes(RdxContext(ctx));
	try {
		removeExpiredItems(ctx);
	} catch (Error &e) {
		// Catch exception from AwaitInitialSync in WLock for cluster replica in follower mode
		if (e.code() != errWrongReplicationData) {
			throw e;
		}
	}
	removeExpiredStrings(ctx);
}

void NamespaceImpl::flushStorage(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	if (storage_) {
		if (unflushedCount_.load(std::memory_order_acquire) > 0) {
			try {
				std::unique_lock<std::mutex> lck(locker_.StorageLock());
				doFlushStorage();
			} catch (const Error &err) {
				if (err.code() != errNamespaceInvalidated) {
					throw;
				}
			}
		}
	}
}

void NamespaceImpl::doFlushStorage() {
	Error status = storage_->Write(StorageOpts(), *(updates_.get()));
	if (!status.ok()) throw Error(errLogic, "Error write ns '%s' to storage: %s", name_, status.what());
	unflushedCount_.store(0, std::memory_order_release);
	updates_->Clear();
	saveReplStateToStorage();
}

void NamespaceImpl::DeleteStorage(const RdxContext &ctx) {
	auto wlck = simpleWLock(ctx);
	deleteStorage();
}

void NamespaceImpl::CloseStorage(const RdxContext &ctx) {
	flushStorage(ctx);
	auto wlck = simpleWLock(ctx);
	dbpath_.clear();
	storage_.reset();
}

std::string NamespaceImpl::sysRecordName(std::string_view sysTag, uint64_t version) {
	std::string backupRecord(sysTag);
	static_assert(kSysRecordsBackupCount && ((kSysRecordsBackupCount & (kSysRecordsBackupCount - 1)) == 0),
				  "kBackupsCount has to be power of 2");
	backupRecord.append(".").append(std::to_string(version & (kSysRecordsBackupCount - 1)));
	return backupRecord;
}

void NamespaceImpl::writeSysRecToStorage(std::string_view data, std::string_view sysTag, uint64_t &version, bool direct) {
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

Item NamespaceImpl::NewItem(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return newItem();
}

void NamespaceImpl::ToPool(ItemImpl *item) {
	item->Clear();
	pool_.put(std::unique_ptr<ItemImpl>{item});
}

// Get meta data from storage by key
string NamespaceImpl::GetMeta(const string &key, const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return getMeta(key);
}

string NamespaceImpl::getMeta(const string &key) const {
	auto it = meta_.find(key);
	if (it != meta_.end()) {
		return it->second;
	}

	if (storage_) {
		string data;
		Error status = storage_->Read(StorageOpts().FillCache(), std::string_view(kStorageMetaPrefix + key), data);
		if (status.ok()) {
			return data;
		}
	}

	return string();
}

// Put meta data to storage by key
void NamespaceImpl::PutMeta(const string &key, std::string_view data, const RdxContext &ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);

	putMeta(key, data, pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), ctx);
}

// Put meta data to storage by key
void NamespaceImpl::putMeta(const string &key, std::string_view data, UpdatesContainer &pendedRepl, const NsContext &ctx) {
	meta_[key] = string(data);

	if (storage_) {
		storage_->Write(StorageOpts().FillCache(), kStorageMetaPrefix + key, data);
	}

	processWalRecord(WALRecord(WalPutMeta, key, data, ctx.inTransaction), ctx);
	pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::PutMetaTx : UpdateRecord::Type::PutMeta, name_, wal_.LastLSN(),
							repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), key, string(data));
}

vector<string> NamespaceImpl::EnumMeta(const RdxContext &ctx) {
	auto rlck = rLock(ctx);
	return enumMeta();
}

vector<string> NamespaceImpl::enumMeta() const {
	vector<string> ret;
	ret.reserve(meta_.size());
	for (auto &m : meta_) {
		ret.push_back(m.first);
	}
	if (!storage_) return ret;

	StorageOpts opts;
	opts.FillCache(false);
	std::unique_ptr<datastorage::Cursor> dbIter(storage_->GetCursor(opts));
	size_t prefixLen = strlen(kStorageMetaPrefix);

	for (dbIter->Seek(std::string_view(kStorageMetaPrefix));
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageMetaPrefix "\xFF")) < 0;
		 dbIter->Next()) {
		std::string_view keySlice = dbIter->Key();
		if (keySlice.size() > prefixLen) {
			string key(keySlice.substr(prefixLen));
			if (meta_.find(key) == meta_.end()) {
				ret.push_back(key);
			}
		}
	}
	return ret;
}

void NamespaceImpl::warmupFtIndexes() {
	h_vector<std::thread, 8> warmupThreads;
	h_vector<Index *, 8> warmupIndexes;
	for (auto &idx : indexes_) {
		if (idx->RequireWarmupOnNsCopy()) {
			warmupIndexes.emplace_back(idx.get());
		}
	}
	auto threadsCnt = config_.optimizationSortWorkers > 0 ? std::min(unsigned(config_.optimizationSortWorkers), warmupIndexes.size())
														  : std::min(4u, warmupIndexes.size());
	warmupThreads.resize(threadsCnt);
	std::atomic<unsigned> next = {0};
	for (unsigned i = 0; i < warmupThreads.size(); ++i) {
		warmupThreads[i] = std::thread([&warmupIndexes, &next] {
			unsigned num = next.fetch_add(1);
			while (num < warmupIndexes.size()) {
				warmupIndexes[num]->CommitFulltext();
				num = next.fetch_add(1);
			}
		});
	}
	for (auto &th : warmupThreads) {
		th.join();
	}
}

int NamespaceImpl::getSortedIdxCount() const {
	if (!config_.optimizationSortWorkers) return 0;
	int cnt = 0;
	for (auto &it : indexes_)
		if (it->IsOrdered()) cnt++;
	return cnt;
}

void NamespaceImpl::updateSortedIdxCount() {
	int sortedIdxCount = getSortedIdxCount();
	for (auto &idx : indexes_) idx->SetSortedIdxCount(sortedIdxCount);
	markUpdated(true);
}

IdType NamespaceImpl::createItem(size_t realSize, IdType suggestedId) {
	IdType id = 0;
	if (suggestedId < 0) {
		if (free_.size()) {
			id = free_.back();
			free_.pop_back();
			assertrx(id < IdType(items_.size()));
			assertrx(items_[id].IsFree());
			items_[id] = PayloadValue(realSize);
		} else {
			id = items_.size();
			items_.emplace_back(PayloadValue(realSize));
		}
	} else {
		id = suggestedId;
		auto found = std::find(free_.begin(), free_.end(), id);
		if (found == free_.end()) {
			if (items_.size() > size_t(id)) {
				if (!items_[size_t(id)].IsFree()) {
					throw Error(errParams, "Suggested ID %d is not empty", id);
				}
			} else {
				auto sz = items_.size();
				items_.resize(size_t(id) + 1);
				free_.reserve(free_.size() + items_.size() - sz);
				while (sz < items_.size() - 1) {
					free_.push_back(sz++);
				}
				items_[id] = PayloadValue(realSize);
			}
		} else {
			free_.erase(found);
			assertrx(id < IdType(items_.size()));
			assertrx(items_[id].IsFree());
			items_[id] = PayloadValue(realSize);
		}
	}
	return id;
}

void NamespaceImpl::deleteStorage() {
	if (storage_) {
		storage_->Destroy(dbpath_);
		dbpath_.clear();
		storage_.reset();
	}
}

void NamespaceImpl::setFieldsBasedOnPrecepts(ItemImpl *ritem, UpdatesContainer &replUpdates, const NsContext ctx) {
	for (auto &precept : ritem->GetPrecepts()) {
		SelectFuncParser sqlFunc;
		SelectFuncStruct sqlFuncStruct = sqlFunc.Parse(precept);

		VariantArray krs;
		Variant field = ritem->GetPayload().Get(sqlFuncStruct.field, krs)[0];

		Variant value(make_key_string(sqlFuncStruct.value));
		if (sqlFuncStruct.isFunction) {
			value = FunctionExecutor(*this, replUpdates).Execute(sqlFuncStruct, ctx);
		}

		value.convert(field.Type());
		VariantArray refs{value};

		ritem->GetPayload().Set(sqlFuncStruct.field, refs, false);
	}
}

int64_t NamespaceImpl::GetSerial(const string &field, UpdatesContainer &replUpdates, const NsContext &ctx) {
	int64_t counter = kStorageSerialInitial;

	std::string key(kSerialPrefix);
	key.append(field);
	auto ser = getMeta(key);
	if (ser != "") {
		counter = reindexer::stoll(ser) + 1;
	}

	std::string s = to_string(counter);
	putMeta(key, std::string_view(s), replUpdates, ctx);

	return counter;
}

void NamespaceImpl::FillResult(LocalQueryResults &result, IdSet::Ptr ids) const {
	for (auto &id : *ids) {
		result.Add({id, items_[id], 0, 0});
	}
}

void NamespaceImpl::getFromJoinCache(JoinCacheRes &ctx) const {
	if (config_.cacheMode == CacheModeOff || optimizationState_ != OptimizationCompleted) return;
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

void NamespaceImpl::getIndsideFromJoinCache(JoinCacheRes &ctx) const {
	if (config_.cacheMode != CacheModeAggressive || optimizationState_ != OptimizationCompleted) return;
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

void NamespaceImpl::putToJoinCache(JoinCacheRes &res, JoinPreResult::Ptr preResult) const {
	JoinCacheVal joinCacheVal;
	res.needPut = false;
	joinCacheVal.inited = true;
	joinCacheVal.preResult = preResult;
	joinCache_->Put(res.key, joinCacheVal);
}
void NamespaceImpl::putToJoinCache(JoinCacheRes &res, JoinCacheVal &val) const {
	val.inited = true;
	joinCache_->Put(res.key, val);
}

const FieldsSet &NamespaceImpl::pkFields() {
	auto it = indexesNames_.find(kPKIndexName);
	if (it != indexesNames_.end()) {
		return indexes_[it->second]->Fields();
	}

	static FieldsSet ret;
	return ret;
}

void NamespaceImpl::writeToStorage(std::string_view key, std::string_view data) {
	try {
		std::unique_lock<std::mutex> lck(locker_.StorageLock());
		updates_->Put(key, data);
		if (unflushedCount_.fetch_add(1, std::memory_order_release) > 20000) doFlushStorage();
	} catch (const Error &err) {
		if (err.code() != errNamespaceInvalidated) {
			throw;
		}
	}
}

void NamespaceImpl::processWalRecord(WALRecord &&wrec, const NsContext &ctx, lsn_t itemLsn, Item *item) {
	lsn_t lsn;
	if (item && ctx.IsForceSyncItem()) {
		lsn = ctx.GetOriginLSN();
	} else {
		lsn = wal_.Add(wrec, ctx.GetOriginLSN(), itemLsn);
	}

	if (item) {
		assertrx(!lsn.isEmpty());
		item->setLSN(lsn);
	}
}

void NamespaceImpl::replicateAsync(cluster::UpdateRecord &&rec, const RdxContext &ctx) {
	if (!repl_.temporary) {
		auto err = clusterizator_->ReplicateAsync(std::move(rec), ctx);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.what());
		}
	}
}

void NamespaceImpl::replicateAsync(NamespaceImpl::UpdatesContainer &&recs, const RdxContext &ctx) {
	if (!repl_.temporary) {
		auto err = clusterizator_->ReplicateAsync(std::move(recs), ctx);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.what());
		}
	}
}

void NamespaceImpl::replicate(UpdateRecord &&rec, Locker::WLockT &&wlck, const RdxContext &ctx) {
	if (!repl_.temporary) {
		auto err = clusterizator_->Replicate(
			std::move(rec),
			[&wlck]() {
				assertrx(wlck.isClusterLck());
				wlck.unlock();
			},
			ctx);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.what());
		}
	}
}

void NamespaceImpl::replicate(UpdatesContainer &&recs, NamespaceImpl::Locker::WLockT &&wlck, const NsContext &ctx) {
	if (!repl_.temporary) {
		assertrx(!ctx.isCopiedNsRequest);
		auto err = clusterizator_->Replicate(
			std::move(recs),
			[&wlck]() {
				assertrx(wlck.isClusterLck());
				wlck.unlock();
			},
			ctx.rdxContext);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.what());
		}
	}
}

std::set<std::string> NamespaceImpl::GetFTIndexes(const RdxContext &ctx) const {
	auto rlck = rLock(ctx);
	std::set<std::string> ret;
	for (const auto &idx : indexes_) {
		switch (idx->Type()) {
			case IndexFastFT:
			case IndexFuzzyFT:
			case IndexCompositeFastFT:
			case IndexCompositeFuzzyFT:
				ret.insert(idx->Name());
				break;
			default:
				break;
		}
	}
	return ret;
}

NamespaceImpl::IndexesCacheCleaner::~IndexesCacheCleaner() {
	for (auto &idx : ns_.indexes_) idx->ClearCache(sorts_);
}

}  // namespace reindexer
