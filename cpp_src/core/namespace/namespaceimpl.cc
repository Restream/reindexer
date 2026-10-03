#include "core/namespace/namespaceimpl.h"

#include <algorithm>
#include "core/cjson/baseencoder.h"
#include "core/cjson/cjsonbuilder.h"
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/jsonbuilder.h"
#include "core/embedding/embedder.h"
#include "core/embedding/embedderscache.h"
#include "core/formatters/id_type_fmt.h"
#include "core/function/function_invoker.h"
#include "core/function/function_parser.h"
#include "core/function/precomputed_values.h"
#include "core/id_type.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/index/index.h"
#include "core/index/indexfastupdate.h"
#include "core/itemimpl.h"
#include "core/itemmodifier.h"
#include "core/key_value_type.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/float_vectors_keeper.h"
#include "core/namespace/indexes/composite_fields.h"
#include "core/namespace/indexes/ddl_transaction.h"
#include "core/namespace/indexes/index_names.h"
#include "core/namespace/migrations/pk_migration_service.h"
#include "core/nsselecter/nsselecter.h"
#include "core/nsselecter/selectctx_traits.h"
#include "core/payload/payloadiface.h"
#include "core/query/functions_optimizations.h"
#include "core/query/query_impl.h"
#include "core/querystat.h"
#include "core/rdxcontext.h"
#include "core/storage/storage_prefixes.h"
#include "debug/crashqueryreporter.h"
#include "estl/gift_str.h"
#include "hashmapstatsloading.h"
#include "itemsloader.h"
#include "snapshot/snapshothandler.h"
#include "threadtaskqueueimpl.h"
#include "tools/errors.h"
#include "tools/flagguard.h"
#include "tools/fsops.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"
#include "tools/scope_guard.h"
#include "tools/timetools.h"
#include "tools/unaligned.h"
#include "tx_concurrent_inserter.h"
#include "wal/walselecter.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::nanoseconds;
using namespace std::string_view_literals;

namespace {

const std::string kFFFFFFFF{"\xFF\xFF\xFF\xFF"};

// TODO disabled due to #1771
// constexpr int kWALStatementItemsThreshold{5};

constexpr uint32_t kStorageMagic{0x1234FEDC};
constexpr uint32_t kStorageVersion{0x8};
constexpr size_t kTxReplAsyncBatchSize{100};
}  // namespace

namespace reindexer {

using ns_indexes::kPKIndexName;
using ns_indexes::kTupleName;

std::atomic_bool rxAllowNamespaceLeak = {false};

constexpr int64_t kStorageSerialInitial = 1;
constexpr uint8_t kSysRecordsBackupCount = 8;
constexpr uint8_t kSysRecordsFirstWriteCopies = 3;
constexpr size_t kMaxMemorySizeOfStringsHolder = 1ull << 24;
constexpr size_t kMaxSchemaCharsToPrint = 128;

// private implementation and NOT THREADSAFE of copy CTOR
NamespaceImpl::NamespaceImpl(const NamespaceImpl& src, size_t newCapacity, AsyncStorage::FullLock& storageLock)
	: intrusive_atomic_rc_base(),
	  items_{src.items_},
	  free_{src.free_},
	  name_{src.name_},
	  indexRegistry_{src.indexRegistry_, newCapacity},
	  storage_{src.storage_, storageLock},
	  replStateUpdates_{src.replStateUpdates_.load()},
	  meta_{src.meta_},
	  krefs(src.krefs),
	  skrefs(src.skrefs),
	  sysRecordsVersions_{src.sysRecordsVersions_},
	  locker_(src.locker_.Syncer(), *this),
	  schema_(src.schema_),
	  enablePerfCounters_{src.enablePerfCounters_.load()},
	  config_{src.config_},
	  queryCountCache_{config_.cacheConfig.queryCountCacheSize, config_.cacheConfig.queryCountHitsToCache},
	  joinCache_{config_.cacheConfig.joinCacheSize, config_.cacheConfig.joinHitsToCache},
	  wal_{src.wal_, storage_},
	  repl_{src.repl_},
	  storageOpts_{src.storageOpts_},
	  lastSelectTime_{0},
	  cancelCommitCnt_{0},
	  lastUpdateTime_{src.lastUpdateTime_.load(std::memory_order_acquire)},
	  itemsCount_{static_cast<uint32_t>(items_.size())},
	  itemsCapacity_{static_cast<uint32_t>(items_.capacity())},
	  nsIsLoading_{false},
	  itemsDataSize_{src.itemsDataSize_},
	  indexOptimizer_{src.indexOptimizer_},
	  strHolder_{makeStringsHolder()},
	  dbDestroyed_{false},
	  incarnationTag_{src.incarnationTag_},
	  observers_{src.observers_},
	  embeddersCache_{src.embeddersCache_} {
	queryCountCache_.CopyInternalPerfStatsFrom(src.queryCountCache_);
	joinCache_.CopyInternalPerfStatsFrom(src.joinCache_);

	markUpdated(IndexOptimization::Full);
	logFmt(LogInfo, "Namespace::CopyContentsFrom ({}).Workers: {}, timeout: {}, tm: {{ state_token: {:#08x}, version: {} }}", name_,
		   config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher().stateToken(), tagsMatcher().version());
}

static int64_t GetCurrentTimeUS() noexcept { return duration_cast<microseconds>(system_clock_w::now().time_since_epoch()).count(); }

NamespaceImpl::NamespaceImpl(const std::string& name, std::optional<int32_t> stateToken, const cluster::IDataSyncer& syncer,
							 UpdatesObservers& observers, const std::shared_ptr<EmbeddersCache>& embeddersCache)
	: intrusive_atomic_rc_base(),
	  name_{name},
	  indexRegistry_{name, stateToken.has_value() ? stateToken.value() : tools::RandomGenerator::gets32()},
	  locker_(syncer, *this),
	  enablePerfCounters_{false},
	  queryCountCache_{config_.cacheConfig.queryCountCacheSize, config_.cacheConfig.queryCountHitsToCache},
	  joinCache_{config_.cacheConfig.joinCacheSize, config_.cacheConfig.joinHitsToCache},
	  wal_{getWalSize(config_)},
	  lastSelectTime_{0},
	  cancelCommitCnt_{0},
	  lastUpdateTime_{0},
	  nsIsLoading_{false},
	  strHolder_{makeStringsHolder()},
	  dbDestroyed_{false},
	  incarnationTag_(GetCurrentTimeUS() % lsn_t::kDefaultCounter, 0),
	  observers_{observers},
	  embeddersCache_{embeddersCache} {
	logFmt(LogTrace, "NamespaceImpl::NamespaceImpl ({})", name_);
	FlagGuardT nsLoadingGuard(nsIsLoading_);
	items_.reserve(10000);
	itemsCapacity_.store(items_.capacity());

	// Add index and payload field for tuple of non indexed fields
	IndexDef tupleIndexDef(kTupleName, {}, IndexStrStore, IndexOpts().Dense().NoIndexColumn());
	addIndex(tupleIndexDef, false);

	logFmt(LogInfo, "Namespace::Construct ({}).Workers: {}, timeout: {}, tm: {{ state_token: {:#08x} ({}), version: {} }}", name_,
		   config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher().stateToken(),
		   stateToken.has_value() ? "preset" : "rand", tagsMatcher().version());
}

NamespaceImpl::~NamespaceImpl() {
	const unsigned int kMaxItemCountNoThread = 1'000'000;
	const bool allowLeak = rxAllowNamespaceLeak.load(std::memory_order_relaxed);
	unsigned int threadsCount = 0;

	ThreadTaskQueueImpl tasks;
	if (!allowLeak && items_.size() > kMaxItemCountNoThread) {
		static constexpr double kDeleteRxDestroy = 0.5;
		static constexpr double kDeleteNs = 0.25;

		const double k = dbDestroyed_.load(std::memory_order_relaxed) ? kDeleteRxDestroy : kDeleteNs;
		threadsCount = static_cast<unsigned int>(k * hardware_concurrency());
		if (threadsCount > indexes().size() + 1) {
			threadsCount = indexes().size() + 1;
		}
	}
	const bool multithreadingMode = (threadsCount > 1);

	logFmt(LogInfo, "NamespaceImpl::~NamespaceImpl:{} allowLeak: {}, threadCount: {}", name_, allowLeak, threadsCount);
	try {
		constexpr bool skipTimeCheck = true;
		UpdateANNStorageCache(skipTimeCheck, RdxContext());	 // Update storage cache even if not enough time is passed
	} catch (...) {
		assertrx_dbg(false);  // Should never happen in test scenarios
		logFmt(LogWarning, "NamespaceImpl::~NamespaceImpl:{} got exception during ANN storage cache update", name_);
	}

	auto flushStorage = [this]() {
		try {
			if (locker_.IsValid()) {
				saveReplStateToStorage(false);
				storage_.Flush(StorageFlushOpts().WithImmediateReopen());
			}
		} catch (Error& e) {
			logFmt(LogWarning, "Namespace::~Namespace:{} flushStorage() error: '{}'", name_, e.what());
		} catch (...) {
			logFmt(LogWarning, "Namespace::~Namespace:{} flushStorage() error: <unknown exception>", name_);
		}
	};
#ifndef NDEBUG
	auto checkStrHoldersWaitingToBeDeleted = [this]() {
		for (const auto& strHldr : strHoldersWaitingToBeDeleted_) {
			assertrx(strHldr.unique());
			(void)strHldr;
		}
		assertrx(strHolder_.unique());
		assertrx(cancelCommitCnt_.load() == 0);
	};
#endif	// NDEBUG

	if (multithreadingMode) {
		assertrx_dbg(!allowLeak);  // Storage flush must be called synchronously for leak mode
		tasks.AddTask(flushStorage);
#ifndef NDEBUG
		tasks.AddTask(checkStrHoldersWaitingToBeDeleted);
#endif	// NDEBUG
	} else {
		flushStorage();
#ifndef NDEBUG
		checkStrHoldersWaitingToBeDeleted();
#endif	// NDEBUG
	}

	UpdateNamespaceHashMapsStats(multithreadingMode ? threadsCount : 1, name_, indexes(), storage_,
								 std::string(kStorageHashTablesStatsPrefix) + "." + std::string(name_));

	if (allowLeak) {
		logFmt(LogTrace, "Namespace::~Namespace:{} {} items. Leak mode", name_, items_.size());
		indexRegistry_.LeakIndexes();
		return;
	}

	if (multithreadingMode) {
		logFmt(LogTrace, "Namespace::~Namespace:{} {} items. Multithread mode. Deletion threads: {}", name_, items_.size(), threadsCount);
		for (size_t i = 0; i < indexes().size(); i++) {
			if (indexes()[i]->IsDestroyPartSupported()) {
				indexes()[i]->AddDestroyTask(tasks);
			} else {
				tasks.AddTask([i, this]() { indexRegistry_.DestroyIndex(i); });
			}
		}
		std::vector<std::thread> threadPool;
		threadPool.reserve(threadsCount);
		for (size_t i = 0; i < threadsCount; ++i) {
			threadPool.emplace_back([&tasks]() {
				while (auto task = tasks.GetTask()) {
					assertrx_dbg(task);
					task();
				}
			});
		}
		for (auto& th : threadPool) {
			th.join();
		}
		logFmt(LogTrace, "Namespace::~Namespace:{}", name_);
	} else {
		logFmt(LogTrace, "Namespace::~Namespace:{} {} items. Simple mode", name_, items_.size());
	}
}

void NamespaceImpl::SetNsVersion(lsn_t version, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	repl_.nsVersion = version;
	saveReplStateToStorage();
}

void NamespaceImpl::OnConfigUpdated(const DBConfigProvider& configProvider, const RdxContext& ctx) {
	NamespaceConfigData configData;
	configProvider.GetNamespaceConfig(GetName(ctx), configData);
	const int serverId = configProvider.GetReplicationConfig().serverID;

	enablePerfCounters_ = configProvider.PerfStatsEnabled();
	auto asyncReplToken = configProvider.GetAsyncReplicationToken(GetName(ctx));

	// ! Updating storage under write lock
	auto wlck = simpleWLock(ctx);

	const bool needReconfigureIdxCache = !config_.cacheConfig.IsIndexesCacheEqual(configData.cacheConfig);
	const bool needReconfigureJoinCache = !config_.cacheConfig.IsJoinCacheEqual(configData.cacheConfig);
	const bool needReconfigureQueryCountCache = !config_.cacheConfig.IsQueryCountCacheEqual(configData.cacheConfig);
	config_ = configData;
	storage_.SetForceFlushLimit(config_.syncStorageFlushLimit);

	for (auto& idx : indexes()) {
		idx->EnableUpdatesCountingMode(config_.idxUpdatesCountingMode);
		if (auto vecIdx = dynamic_cast<FloatVectorIndex*>(idx.get()); vecIdx) {
			vecIdx->EnablePerfStat(enablePerfCounters_);
		}
	}
	if (needReconfigureIdxCache) {
		for (auto& idx : indexes()) {
			idx->ReconfigureCache(config_.cacheConfig);
		}
		logFmt(LogTrace,
			   "[{}] Indexes cache has been reconfigured. IdSets cache (for each index): {{ max_size {} KB; hits: {} }}. FullTextIdSets "
			   "cache (for each ft-index): {{ max_size {} KB; hits: {} }}",
			   name_, config_.cacheConfig.idxIdsetCacheSize / 1024, config_.cacheConfig.idxIdsetHitsToCache,
			   config_.cacheConfig.ftIdxCacheSize / 1024, config_.cacheConfig.ftIdxHitsToCache);
	}
	if (needReconfigureJoinCache) {
		joinCache_.Reinitialize(config_.cacheConfig.joinCacheSize, config_.cacheConfig.joinHitsToCache);
		logFmt(LogTrace, "[{}] Join cache has been reconfigured: {{ max_size {} KB; hits: {} }}", name_,
			   config_.cacheConfig.joinCacheSize / 1024, config_.cacheConfig.joinHitsToCache);
	}
	if (needReconfigureQueryCountCache) {
		queryCountCache_.Reinitialize(config_.cacheConfig.queryCountCacheSize, config_.cacheConfig.queryCountHitsToCache);
		logFmt(LogTrace, "[{}] Queries count cache has been reconfigured: {{ max_size {} KB; hits: {} }}", name_,
			   config_.cacheConfig.queryCountCacheSize / 1024, config_.cacheConfig.queryCountHitsToCache);
	}
	indexOptimizer_.SetConfig(name_, indexes(),
							  IndexOptimizer::Config{.optimizationTimeout = std::chrono::milliseconds{configData.optimizationTimeout},
													 .optimizationSortWorkers = configData.optimizationSortWorkers});

	if (wal_.Resize(getWalSize(config_))) {
		logFmt(LogInfo, "[{}] WAL has been resized lsn #{}, max size {}", name_, repl_.lastLsn, wal_.Capacity());
	}

	if (isSystem()) {
		try {
			repl_.nsVersion.SetServer(serverId);
		} catch (const Error& err) {
			logFmt(LogError, "[repl:{}]:{} Failed to change serverId to {}: {}.", name_, repl_.nsVersion.Server(), serverId, err.what());
		}
		return;
	}

	if (wal_.GetServer() != serverId) {
		if (itemsCount_ != 0) {
			repl_.clusterStatus.role = ClusterOperationStatus::Role::None;	// TODO: Maybe we have to add separate role for this case
			logFmt(LogWarning, "Changing serverId on NON EMPTY ns [{}]. Cluster role will be reset to None", name_);
		}
		logFmt(LogWarning, "[repl:{}]:{} Changing serverId to {}. Tm_statetoken: {:#08x}", name_, wal_.GetServer(), serverId,
			   tagsMatcher().stateToken());
		const auto oldServerId = wal_.GetServer();
		try {
			wal_.SetServer(serverId);
			incarnationTag_.SetServer(serverId);
		} catch (const Error& err) {
			logFmt(LogError, "[repl:{}]:{} Failed to change serverId to {}: {}. Resetting to 0(zero).", name_, wal_.GetServer(), serverId,
				   err.what());
			wal_.SetServer(oldServerId);
			incarnationTag_.SetServer(oldServerId);
		}
		replStateUpdates_.fetch_add(1, std::memory_order_release);
	}

	repl_.token = std::move(asyncReplToken);
}

Variant NamespaceImpl::getFloatVector(FloatVectorId id, const FloatVectorIndex& index, const PayloadType* pt, const TagsMatcher* tm,
									  const FieldsSet* oldPkFields) const {
	const FieldsSet* pk = oldPkFields ? oldPkFields : pkFields();
	assertrx_throw(pk);
	return FloatVectorExtractor(storage_, index, pk ? *pk : FieldsSet{}, pt ? *pt : payloadType(), tm ? *tm : tagsMatcher())
		.GetVector(id, items_[id.RowId()]);
}

h_vector<Variant, 1> NamespaceImpl::getFloatVectorArray(IdType rowId, size_t arrSize, const FloatVectorIndex& index, const PayloadType* pt,
														const TagsMatcher* tm, const FieldsSet* oldPkFields) const {
	const FieldsSet* pk = oldPkFields ? oldPkFields : pkFields();
	assertrx_throw(pk);
	return FloatVectorExtractor(storage_, index, pk ? *pk : FieldsSet{}, pt ? *pt : payloadType(), tm ? *tm : tagsMatcher())
		.GetVectorArray(rowId, arrSize, items_[rowId]);
}

FloatVectorsGetter NamespaceImpl::floatVectorsGetterFn(IdType rowId, const FieldsSet* oldPkFields) const noexcept {
	return FloatVectorsGetter(*this, rowId, oldPkFields);
}

Error NamespaceImpl::tryWriteItemIntoStorage(const FieldsSet& pkFields, ItemImpl& item, IdType rowId, WrSerializer& pk,
											 WrSerializer& data) noexcept {
	if (item.payloadValue_.IsFree()) {
		return Error{};
	}
	try {
		if (storage_.IsValid()) {
			item.CopyIndexedVectorsValuesFrom(floatVectorsGetterFn(rowId));
			auto pl = item.GetConstPayload();
			pk.Reset();
			data.Reset();
			pk << kRxStorageItemPrefix;
			pl.SerializeFields(pk, pkFields);
			data.PutUInt64(uint64_t(pl.Value()->GetLSN()));
			// NOTE: still may fall back to an async write internally - see AsyncStorage::modifySync()
			storage_.WriteSync(StorageOpts(), pk.Slice(), item.GetCJSON(data));
			return Error{};
		} else {
			return Error{errLogic, "Storage is not valid"};
		}
	} catch (Error& err) {
		logFmt(LogWarning, "[{}] Unable to write item into storage: {}", name_, err.what());
		return err;
	} catch (std::exception& err) {
		logFmt(LogWarning, "[{}] Unable to write item into storage: {}", name_, err.what());
		return Error{errLogic, err.what()};
	} catch (...) {
		Error e{errLogic, "[{}] Unable to write item into storage: <unknown exception>", name_};
		logFmt(LogWarning, fmt::runtime(e.what()));
		return e;
	}
}

uint64_t NamespaceImpl::calculateItemChecksum(IdType rowId, int removedIdxId) const noexcept {
	return ConstPayload{payloadType(), items_[rowId]}.GetChecksum(
		[this, rowId, removedIdxId](unsigned field, ConstFloatVectorView vec, unsigned arrayIndex) noexcept -> uint64_t {
			if (vec.IsStripped()) {
				unsigned actualField = field;
				if (removedIdxId >= 0 && field >= unsigned(removedIdxId)) {
					actualField = field + 1;
				}
				auto idx = dynamic_cast<const FloatVectorIndex*>(indexes()[actualField].get());
				assertf(idx, "Expecting '{}' in '{}' being float vector index", indexes()[actualField]->Name(), name_);
				return idx->GetHash({rowId, arrayIndex});
			}
			return vec.Hash();
		});
}

void NamespaceImpl::addToWAL(const IndexDef& indexDef, WALRecType type, const NsContext& ctx) {
	WrSerializer ser;
	indexDef.GetJSON(ser);
	processWalRecord(WALRecord(type, ser.Slice()), ctx);
}

void NamespaceImpl::addToWAL(std::string_view json, WALRecType type, const NsContext& ctx) { processWalRecord(WALRecord(type, json), ctx); }

void NamespaceImpl::AddIndex(const IndexDef& indexDef, const RdxContext& rdxCtx) {
	indexDef.Validate();

	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx, true);
	cg.Reset();

	verifyPkMigrationNotPending(indexDef);
	verifyUpsertIndex("add", indexDef);
	bool checkIdxEqualityNow = ctx.GetOriginLSN().isEmpty();
	// Check index existence before cluster role check, to allow followers "add" their indexes locally
	// FT indexes may have different config, it will be ignored during comparison
	bool requireTtlUpdate = false;
	if (checkIdxEqualityNow && checkIfSameIndexExists(indexDef, &requireTtlUpdate)) {
		if (requireTtlUpdate && repl_.clusterStatus.role == ClusterOperationStatus::Role::None) {
			checkIdxEqualityNow = false;
		} else {
			if (ctx.HasEmitterServer()) {
				// Make sure, that index was already replicated to emitter
				pendedRepl.emplace_back(updates::URType::EmptyUpdate, name_, ctx.EmitterServerId());
				replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
			}
			return;
		}
	}

	checkClusterStatus(ctx.rdxContext);

	doAddIndex(indexDef, checkIdxEqualityNow, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::DumpIndex(std::ostream& os, std::string_view index, const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	dumpIndex(os, index);
}

void NamespaceImpl::UpdateIndex(const IndexDef& indexDef, const RdxContext& rdxCtx) {
	indexDef.Validate();

	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();

	verifyPkMigrationNotPending(indexDef);

	if (doUpdateIndex(indexDef, pendedRepl, ctx)) {
		saveIndexesToStorage();
		replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
	}
}

void NamespaceImpl::DropIndex(const IndexDef& indexDef, const RdxContext& rdxCtx) {
	indexDef.Validate();

	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();

	verifyPkMigrationNotPending(indexDef);

	doDropIndex(indexDef, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::SetSchema(std::string_view schema, const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	// Intentionally do not set cancelCommitCnt_ here
	auto wlck = dataWLock(rdxCtx, true);

	if (ctx.GetOriginLSN().isEmpty()) {
		if (schema_ && schema_->GetJSON() == Schema::AppendProtobufNumber(schema, schema_->GetProtobufNsNumber())) {
			if (repl_.clusterStatus.role != ClusterOperationStatus::Role::None) {
				logFmt(LogWarning,
					   "[repl:{}]:{} Attempt to set new JSON-schema for the replicated namespace via user interface, which does not "
					   "correspond to the current schema. New schema was ignored to avoid force syncs",
					   name_, wal_.GetServer());
				return;
			}
			if (ctx.HasEmitterServer()) {
				// Make sure, that schema was already replicated to emitter
				pendedRepl.emplace_back(updates::URType::EmptyUpdate, name_, ctx.EmitterServerId());
				replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
			}
			return;
		}
	}

	checkClusterStatus(ctx.rdxContext);
	setSchema(schema, pendedRepl, ctx);
	saveSchemaToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

std::string NamespaceImpl::GetSchema(int format, const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	WrSerializer ser;
	if (schema_) {
		if (format == JsonSchemaType) {
			schema_->GetJSON(ser);
		} else if (format == ProtobufSchemaType) {
			Error err = schema_->GetProtobufSchema(ser);
			if (!err.ok()) {
				throw err;
			}
		} else {
			throw Error(errParams, "Unknown schema type: {}", format);
		}
	} else if (format == ProtobufSchemaType) [[unlikely]] {
		throw Error(errLogic, "Schema is not initialized either just empty");
	}
	return std::string(ser.Slice());
}

void NamespaceImpl::dumpIndex(std::ostream& os, std::string_view index) const {
	int idxPos = 0;
	if (!tryGetIndexByName(index, idxPos)) [[unlikely]] {
		constexpr auto errMsg = "Cannot dump index {}: doesn't exist";
		logFmt(LogError, errMsg, index);
		throw Error(errParams, errMsg, index);
	}
	indexes()[idxPos]->Dump(os);
}

void NamespaceImpl::clearNamespaceCaches() {
	queryCountCache_.Clear();
	joinCache_.Clear();
}

bool NamespaceImpl::isPkAffectingIndexChange(const IndexDef& indexDef) const noexcept {
	if (indexDef.Opts().IsPK()) {
		return true;
	}
	int pos = 0;
	return tryGetIndexByName(indexDef.Name(), pos) && indexes()[pos]->Opts().IsPK();
}

void NamespaceImpl::verifyPkMigrationNotPending(const IndexDef& indexDef) {
	if (!isPkAffectingIndexChange(indexDef)) {
		return;
	}
	if (migrations::PKMigrationService{*this}.HasIncompleteMigration()) {
		throw Error(errLogic,
					"Cannot modify PK index '{}' in namespace '{}': a previous PK migration has not completed and its storage recovery "
					"is still pending. Restart the server to complete the pending recovery, then retry",
					indexDef.Name(), name_);
	}
}

int NamespaceImpl::verifyDropIndex(const IndexDef& index) const {
	int pos = 0;
	if (!tryGetIndexByName(index.Name(), pos)) [[unlikely]] {
		constexpr auto errMsg = "Cannot remove index '{}': doesn't exist";
		logFmt(LogError, errMsg, index.Name());
		throw Error(errParams, errMsg, index.Name());
	}
	if (iequals(index.Name(), ns_indexes::kTupleName)) [[unlikely]] {
		// The tuple index is not optional - the registry always expects it at position 0 (see Registry::CheckConsistency)
		throw Error(errParams, "Cannot remove index '{}': it's a system index", index.Name());
	}
	// Check, that index to remove is not a part of float index with auto embedding
	auto embeddedIndexName = payloadType().CheckEmbeddersAuxiliaryField(index.Name());
	if (!embeddedIndexName.empty()) [[unlikely]] {
		throw Error(errLogic, "Cannot remove index '{}': it's a part of a auto embedding logic in index '{}'", index.Name(),
					embeddedIndexName);
	}
	if (indexes()[pos]->Opts().IsPK() && itemsCount() > 0) {
		for (const auto& idx : indexes()) {
			if (idx->IsFloatVector()) [[unlikely]] {
				// TODO remove this after #2220
				throw Error(errLogic, "Cannot remove PK index '{}' from namespace '{}': the namespace contains float vector index '{}'",
							index.Name(), name_, idx->Name());
			}
		}
	}
	return pos;
}

void NamespaceImpl::doDropIndex(const IndexDef& index, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	ns_indexes::TransactionDDL{*this}.DropIndex(index, ctx.IsInSnapshot());

	addToWAL(index, WalIndexDrop, ctx);
	pendedRepl.emplace_back(updates::URType::IndexDrop, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), index);
}

static void verifyConvertType(KeyValueType from, KeyValueType to, const PayloadType& payloadType, const FieldsSet& fields) {
	if (!from.IsSame(to) && (((from.Is<KeyValueType::String>() || from.Is<KeyValueType::Uuid>()) &&
							  !(to.Is<KeyValueType::String>() || to.Is<KeyValueType::Uuid>())) ||
							 ((to.Is<KeyValueType::String>() || to.Is<KeyValueType::Uuid>()) &&
							  !(from.Is<KeyValueType::String>() || from.Is<KeyValueType::Uuid>())) ||
							 from.Is<KeyValueType::FloatVector>() || to.Is<KeyValueType::FloatVector>())) {
		throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
	}
	static const std::string defaultStringValue;
	static const std::string nilUuidStringValue{Uuid{}};
	Variant value;
	from.EvaluateOneOf(
		[&](KeyValueType::Int64) noexcept { value = Variant(int64_t(0)); }, [&](KeyValueType::Double) noexcept { value = Variant(0.0); },
		[&](KeyValueType::Float) noexcept { value = Variant(0.0f); },
		[&](KeyValueType::String) { value = Variant{to.Is<KeyValueType::Uuid>() ? nilUuidStringValue : defaultStringValue}; },
		[&](KeyValueType::Bool) noexcept { value = Variant(false); }, [](KeyValueType::Null) noexcept {},
		[&](KeyValueType::Int) noexcept { value = Variant(0); }, [&](KeyValueType::Uuid) noexcept { value = Variant{Uuid{}}; },
		[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto) {
			if (!to.IsSame(from)) {
				throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
			}
		});
	std::ignore = value.convert(to, &payloadType, &fields);
}

static void verifyConvertSparseType(KeyValueType from, KeyValueType to) {
	if (from.IsSame(to)) {
		return;
	}
	from.EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto) {
			to.EvaluateOneOf([](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
												KeyValueType::Float> auto) noexcept {},
							 [&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::String, KeyValueType::Uuid, KeyValueType::Tuple,
												 KeyValueType::Composite, KeyValueType::Null, KeyValueType::FloatVector> auto) {
								 throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
							 });
		},
		[&](concepts::OneOf<KeyValueType::String, KeyValueType::Uuid> auto) {
			to.EvaluateOneOf([](concepts::OneOf<KeyValueType::String, KeyValueType::Uuid> auto) noexcept {},
							 [&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Bool,
												 KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Null,
												 KeyValueType::Float, KeyValueType::FloatVector> auto) {
								 throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
							 });
		},
		[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null,
							KeyValueType::FloatVector> auto) {
			throw Error(errParams, "Cannot convert key from type {} to {}", from.Name(), to.Name());
		});
}

void NamespaceImpl::verifyCompositeIndex(const IndexDef& indexDef) const {
	const auto type = indexDef.IndexType();
	if (indexDef.Opts().IsSparse()) [[unlikely]] {
		throw Error(errParams, "Composite index cannot be sparse. Use non-sparse composite instead");
	}
	int replacedIdx = -1;
	const bool replacing = tryGetIndexByName(indexDef.Name(), replacedIdx);
	for (const auto& jp : indexDef.JsonPaths()) {
		int idx;
		// Update keeps the old index in the live registry until commit. That index is being replaced, so it cannot
		// back a json-path of the new composite — after the update it will no longer be a scalar field index.
		if (!tryGetIndexByName(jp, idx) || (replacing && idx == replacedIdx)) {
			if (!IsFullText(indexDef.IndexType())) [[unlikely]] {
				throw Error(errParams,
							"Composite indexes over non-indexed field ('{}') are not supported yet (except for full-text indexes). Create "
							"at least column index('-') over each field inside the composite index",
							jp);
			}
		} else {
			const auto& index = *indexes()[idx];
			if (index.Opts().IsFloatVector()) [[unlikely]] {
				throw Error(errParams, "Composite indexes over float vector indexed field ('{}') are not supported yet", jp);
			}
			if (index.Opts().IsSparse()) [[unlikely]] {
				throw Error(errParams, "Composite indexes over sparse indexed field ('{}') are not supported yet", jp);
			}
			if (type != IndexCompositeHash && index.IsUuid()) [[unlikely]] {
				throw Error{errParams, "Only hash index allowed on UUID field"};
			}
			if (index.Opts().IsArray() && !IsFullText(type)) [[unlikely]] {
				throw Error(errParams, "Cannot add array subindex '{}' to not fulltext composite index '{}'", jp, indexDef.Name());
			}
			if (IsComposite(index.Type())) [[unlikely]] {
				throw Error(errParams, "Cannot create composite index '{}' over the other composite '{}'", indexDef.Name(), index.Name());
			}
		}
	}
}

void NamespaceImpl::verifyEmbeddingFields(const h_vector<std::string, 1>& fields, std::string_view fieldName,
										  std::string_view action) const {
	for (const auto& field : fields) {
		int idx = 0;
		if (!tryGetIndexByName(field, idx)) [[unlikely]] {
			throw Error(errLogic, "Cannot {} index field named '{}' in namespace '{}'. Auxiliary field '{}' not found", action, fieldName,
						name_, field);
		}
		if (idx >= indexes().firstCompositePos()) [[unlikely]] {
			throw Error(errParams,
						"Cannot {} index field named '{}' in namespace '{}'. Support for embedding only for "
						"scalar index fields. Using composite field '{}' for embedding is invalid",
						action, fieldName, name_, field);
		}
		if (indexes()[idx]->Opts().IsSparse()) [[unlikely]] {
			throw Error(errParams,
						"Cannot {} index field named '{}' in namespace '{}'. Support for embedding only for "
						"scalar index fields. Using field '{}' is sparse, so embedding is not supported",
						action, fieldName, name_, field);
		}
	}
}

void NamespaceImpl::verifyUpsertEmbedder(std::string_view action, const IndexDef& indexDef) const {
	const auto& indexDefOpts = indexDef.Opts();
	assertrx_throw(indexDefOpts.IsFloatVector());

	if (!iequals("update", action)) {
		return;	 // do only for update
	}

	auto embedding = indexDefOpts.FloatVector().Embedding();
	if (!embedding.has_value() || !embedding.value().upsertEmbedder.has_value()) {
		return;
	}

	verifyEmbeddingFields(embedding.value().upsertEmbedder.value().fields, indexDef.Name(), action);
}

void NamespaceImpl::verifyUpsertQuantizationConfigHNSWIndex(std::string_view action, const IndexDef& indexDef) const {
	const auto& indexDefOpts = indexDef.Opts();
	if (!indexDefOpts.IsFloatVector()) {
		return;
	}

	const auto& config = indexDefOpts.FloatVector().QuantizationConfig();
	if (!config) {
		return;
	}

	if (indexDef.IndexType() != IndexType::IndexHnsw) {
		throw Error(errParams,
					"Cannot {} quantization config in index '{}' in namespace '{}'. Only the HNSW float vector index can have a "
					"quantization config.",
					action, indexDef.Name(), name_);
	}

	if (config->quantizationType != hnswlib::QuantizationType::ScalarQuantization8bit) {
		throw Error(errParams, "Cannot {} quantization config in index '{}' in namespace '{}'. Unsupported quantization type - {}", action,
					indexDef.Name(), name_, int(config->quantizationType));
	}

	if (config->sampleSize == 0) {
		throw Error(errParams, "Cannot {} quantization config in index '{}' in namespace '{}'. The sampleSize value must be greater than 0",
					action, indexDef.Name(), name_);
	}

	if (config->quantizationThreshold == 0) {
		throw Error(errParams,
					"Cannot {} quantization config in index '{}' in namespace '{}'. The quantizationThreshold value must be greater than 0",
					action, indexDef.Name(), name_);
	}
}

void NamespaceImpl::verifyUpdateQuantizationConfigHNSWIndex(const Index* curIndex, const IndexDef& newIndexDef) const {
	const auto& curIndexDefOpts = curIndex->Opts();

	if (curIndexDefOpts.IsFloatVector() != newIndexDef.Opts().IsFloatVector() && itemsCount() > 0) {
		throw Error(errParams,
					"Cannot update index '{}' in namespace '{}'. Can't convert float vector index to not float vector index and vice versa "
					"in non-empty namespace",
					curIndex->Name(), name_);
	}

	auto curFloatIndex = dynamic_cast<const FloatVectorIndex*>(curIndex);
	if (!curFloatIndex) {
		return;
	}

	if (const auto& curConfig = curIndexDefOpts.FloatVector().QuantizationConfig();
		curConfig && curFloatIndex->Type() != IndexType::IndexHnsw) {
		logFmt(LogWarning,
			   "An incorrect float vector options were detected during update index '{}' in namespace '{}': the quantization config of "
			   "an index with a type other than 'hnsw'",
			   curFloatIndex->Name(), name_);
	}

	if (curFloatIndex->IsQuantized() && newIndexDef.Opts().FloatVector().QuantizationConfig()) {
		throw Error(errParams,
					"Cannot update quantization config in index '{}' in namespace '{}'. Index was quantized already. "
					"To set a new quantization config, restore the original non-quantized index by resetting the current config, "
					"then apply the new one.",
					curFloatIndex->Name(), name_);
	}
}

void NamespaceImpl::verifyUpsertIndex(std::string_view action, const IndexDef& indexDef) const {
	const auto idxType = indexDef.IndexType();
	if (!ValidateIndexName(indexDef.Name(), idxType)) [[unlikely]] {
		throw Error(errParams,
					"Cannot {} index '{}' in namespace '{}'. Index name contains invalid characters. Only alphas, digits, '+' (for "
					"composite indexes only), '.', '_' and '-' are allowed",
					action, indexDef.Name(), name_);
	}
	const auto& indexDefOpts = indexDef.Opts();
	if (indexDefOpts.IsPK()) {
		if (indexDefOpts.IsArray()) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be array", action, indexDef.Name(), name_);
		} else if (indexDefOpts.IsSparse()) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be sparse", action, indexDef.Name(), name_);
		} else if (IsStore(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't have '-' type", action, indexDef.Name(), name_);
		} else if (IsFullText(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be fulltext index", action, indexDef.Name(),
						name_);
		} else if (IsGeospatial(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be geospatial index", action, indexDef.Name(),
						name_);
		}
	}
	if ((idxType == IndexUuidHash || idxType == IndexUuidStore) && indexDefOpts.IsSparse()) [[unlikely]] {
		throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. UUID field can't be sparse", action, indexDef.Name(), name_);
	}
	if (indexDef.JsonPaths().size() > 1) [[unlikely]] {
		if (!IsComposite(idxType) && !indexDefOpts.IsArray()) {
			throw Error(
				errParams,
				"Cannot {} index '{}' in namespace '{}'. Scalar (non-array and non-composite) index can not have multiple JSON-paths. "
				"Use array index instead",
				action, indexDef.Name(), name_);
		}
		if (IsGeospatial(idxType)) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. Geospatial index can not have multiple JSON-paths", action,
						indexDef.Name(), name_);
		}
	}
	if (indexDef.JsonPaths().empty()) [[unlikely]] {
		throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. JSON paths array can not be empty", action, indexDef.Name(), name_);
	}
	for (const auto& jp : indexDef.JsonPaths()) {
		if (jp.empty()) [[unlikely]] {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. JSON path can not be empty", action, indexDef.Name(), name_);
		}
	}
	if (indexDefOpts.IsFloatVector()) {
		verifyUpsertEmbedder(action, indexDef);
		if (int pkPos = 0; !tryGetIndexByName(kPKIndexName, pkPos) && itemsCount() > 0) [[unlikely]] {
			// TODO remove this after #2220
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. The namespace does not have PK index", action, indexDef.Name(),
						name_);
		}

		verifyUpsertQuantizationConfigHNSWIndex(action, indexDef);
	}
}

void NamespaceImpl::verifyUpdateIndex(const IndexDef& indexDef, TagsMatcher& tm) const {
	int idxPos = 0;
	if (!tryGetIndexByName(indexDef.Name(), idxPos)) [[unlikely]] {
		throw Error(errParams, "Cannot update index '{}': doesn't exist", indexDef.Name());
	}
	const auto& oldIndex = indexes()[idxPos];
	const auto& indexDefOpts = indexDef.Opts();
	if (int pkPos = 0; indexDefOpts.IsPK() && !oldIndex->Opts().IsPK() && tryGetIndexByName(kPKIndexName, pkPos)) [[unlikely]] {
		throw Error(errConflict, "Cannot add PK index '{}.{}'. Already exists another PK index - '{}'", name_, indexDef.Name(),
					indexes()[pkPos]->Name());
	}
	if (indexDefOpts.IsArray() != oldIndex->Opts().IsArray() && itemsCount() > 0) [[unlikely]] {
		// Array may be converted to scalar and scalar to array only if there are no items in namespace
		throw Error(
			errParams,
			"Cannot update index '{}' in namespace '{}'. Can't convert array index to not array and vice versa in non-empty namespace",
			indexDef.Name(), name_);
	}

	verifyUpsertIndex("update", indexDef);

	if (IsComposite(indexDef.IndexType())) {
		verifyCompositeIndex(indexDef);
		// Composite text indexes require fair fields set to validate config
		auto fields = ns_indexes::CreateFieldsSetFromJsonPaths(
			indexDef, tm, [this](std::string_view jsonPath, int& idx) noexcept { return tryGetScalarIndexByName(jsonPath, idx); });
		const auto newIndex =
			std::unique_ptr<Index>(Index::New(indexDef, PayloadType{payloadType()}, std::move(fields), config_.cacheConfig, itemsCount()));
	} else if (indexDefOpts.IsSparse()) {
		if (indexDef.JsonPaths().size() != 1) [[unlikely]] {
			throw Error(errParams, "Sparse index must have exactly 1 JSON-path, but {} paths found for '{}'", indexDef.JsonPaths().size(),
						indexDef.Name());
		}
		FieldsSet fields;
		fields.push_back(indexDef.JsonPaths()[0]);
		const auto newSparseIndex = Index::New(indexDef, PayloadType{payloadType()}, std::move(fields), config_.cacheConfig, itemsCount());
		if (itemsCount() > 0) {
			verifyConvertSparseType(oldIndex->KeyType(), newSparseIndex->KeyType());
		}
	} else {
		const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet(), config_.cacheConfig, itemsCount()));
		PayloadType newPlType = payloadType();
		newPlType.Drop(indexDef.Name());
		newPlType.Add(PayloadFieldType(name_.ToLower(), *newIndex, indexDef, embeddersCache_, enablePerfCounters_));

		if (itemsCount() > 0) {
			FieldsSet changedFields{idxPos};
			verifyConvertType(oldIndex->KeyType(), newIndex->KeyType(), newPlType, changedFields);
		}
	}
	if (int pkPos = 0; indexDefOpts.IsFloatVector() && !tryGetIndexByName(kPKIndexName, pkPos) && itemsCount() > 0) [[unlikely]] {
		// TODO remove this after #2220
		throw Error(errParams, "Cannot update index '{}' in namespace '{}'. The namespace does not have PK index", indexDef.Name(), name_);
	}

	verifyUpdateQuantizationConfigHNSWIndex(oldIndex.get(), indexDef);
}

void NamespaceImpl::addIndex(const IndexDef& indexDef, bool disableTmVersionInc, bool skipEqualityCheck) {
	ns_indexes::TransactionDDL{*this}.AddIndex(indexDef, disableTmVersionInc, skipEqualityCheck);
}

void NamespaceImpl::doAddIndex(const IndexDef& indexDef, bool skipEqualityCheck, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	addIndex(indexDef, ctx.IsInSnapshot(), skipEqualityCheck);

	addToWAL(indexDef, WalIndexAdd, ctx);
	pendedRepl.emplace_back(updates::URType::IndexAdd, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), indexDef);
}

bool NamespaceImpl::doUpdateIndex(const IndexDef& indexDef, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (ns_indexes::TransactionDDL{*this}.UpdateIndex(indexDef, ctx.IsInSnapshot()) || !ctx.GetOriginLSN().isEmpty()) {
		addToWAL(indexDef, WalIndexUpdate, ctx);
		pendedRepl.emplace_back(updates::URType::IndexUpdate, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), indexDef);
		return true;
	}
	return false;
}

IndexDef NamespaceImpl::getIndexDefinition(const std::string& indexName) const {
	for (unsigned i = 0; i < indexes().size(); ++i) {
		if (indexes()[i]->Name() == indexName) {
			return getIndexDefinition(i);
		}
	}
	throw Error(errParams, "Index '{}' not found in '{}'", indexName, name_);
}

bool NamespaceImpl::checkIfSameIndexExists(const IndexDef& indexDef, bool* requireTtlUpdate) const {
	if (int idxPos = 0; tryGetIndexByName(indexDef.Name(), idxPos)) {
		IndexDef oldIndexDef = getIndexDefinition(indexDef.Name());
		if (oldIndexDef.IndexType() == IndexTtl && indexDef.IndexType() == IndexTtl) {
			if (requireTtlUpdate && oldIndexDef.ExpireAfter() != indexDef.ExpireAfter()) {
				*requireTtlUpdate = true;
			}
			oldIndexDef.SetExpireAfter(indexDef.ExpireAfter());
		}
		if (IndexDef::IsBasicCompatibility(indexDef.Compare(oldIndexDef))) {
			return true;
		}
		throw Error(errConflict, "Index '{}.{}' already exists with different settings", name_, indexDef.Name());
	}
	return false;
}

int NamespaceImpl::getIndexByName(std::string_view index) const {
	int pos = 0;
	if (!indexRegistry_.TryGetIndexPos(index, pos)) [[unlikely]] {
		throw Error(errParams, "Index '{}' not found in '{}'", index, name_);
	}
	return pos;
}

bool NamespaceImpl::tryGetIndexByName(std::string_view name, int& index) const noexcept {
	return indexRegistry_.TryGetIndexPos(name, index);
}

bool NamespaceImpl::tryGetIndexByNameOrJsonPath(std::string_view name, int& index, EnableMultiJsonPath multi) const {
	if (tryGetIndexByName(name, index)) {
		return true;
	}
	return tryGetIndexByJsonPath(name, index, multi);
}

bool NamespaceImpl::tryGetIndexByJsonPath(std::string_view name, int& index, EnableMultiJsonPath multi) const noexcept {
	// regular indexes handling
	auto idx = payloadType().FieldByJsonPath(name);
	if (idx > 0) {
		if (!multi && payloadType().Field(idx).JsonPaths().size() > 1) {
			return false;
		}
		index = idx;
		return true;
	}

	// sparse indexes handling
	auto field = tagsMatcher().tags2field(tagsMatcher().path2tag(name));
	assertrx_dbg(!field.IsIndexed() || field.IsSparse());
	if (field.IsIndexed() && field.IsSparse()) {
		try {
			auto& idxData = tagsMatcher().SparseIndex(field.SparseNumber());
			if (!multi && idxData.paths.size() > 1) {
				return false;
			}
			return tryGetIndexByName(idxData.name, index);
		} catch (const std::exception& err) {
			assertf(false, "Error getting sparse index {}: {}", field.SparseNumber(), err.what());
			return false;
		}
	}
	return false;
}

bool NamespaceImpl::tryGetScalarIndexByName(std::string_view name, int& index) const noexcept {
	int idx = 0;
	if (tryGetIndexByName(name, idx)) {
		if (idx < indexes().firstCompositePos()) {
			index = idx;
			return true;
		}
	}
	return false;
}

void NamespaceImpl::Insert(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeInsert, ctx); }

void NamespaceImpl::Update(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeUpdate, ctx); }

void NamespaceImpl::Upsert(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeUpsert, ctx); }

void NamespaceImpl::Delete(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeDelete, ctx); }

void NamespaceImpl::doDelete(IdType id, const NsContext& ctx) {
	assertrx(items_.exists(id));

	Payload pl(payloadType(), items_[id]);
	const FieldsSet* pk = pkFields();
	assertrx_dbg(pk);

	WrSerializer pkBuf;
	pkBuf << kRxStorageItemPrefix;
	pl.SerializeFields(pkBuf, pk ? *pk : FieldsSet{});

	repl_.checksum ^= calculateItemChecksum(id);
	std::ignore = wal_.Set(WALRecord(), items_[id].GetLSN(), false);

	storage_.Remove(pkBuf.Slice());

	// erase last item
	int field = 0;

	// erase from composite indexes
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (field = indexes().firstCompositePos(); field < indexes().totalSize(); ++field) {
		// No txCtx modification required for composite indexes

		bool needClearCache{false};
		indexes()[field]->Delete(Variant(items_[id]), id, MustExist_True, *strHolder_, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(*indexes()[field]);
		}
	}

	// Holder for tuple. It is required for sparse indexes will be valid
	VariantArray tupleHolder;
	pl.Get(0, tupleHolder);

	// Deleting fields from dense and sparse indexes: we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire payload value (and not only 0 item)
	assertrx(indexes().firstCompositePos() != 0);
	const int borderIdx = indexes().totalSize() > 1 ? 1 : 0;
	field = borderIdx;
	do {
		field %= indexes().firstCompositePos();

		Index& index = *indexes()[field];
		if (index.Opts().IsSparse()) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			pl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else if (index.Opts().IsArray()) {
			pl.Get(field, skrefs, Variant::hold);
		} else {
			pl.Get(field, skrefs);
		}

		// Data in vector multithreading transactions are not indexed, so they should not be deleted from the index
		bool IsVectorMTTxItem = ctx.txCtx && index.IsSupportMultithreadTransactions();
		if (IsVectorMTTxItem) {
			const auto count = pl.GetFieldLen(field);
			for (unsigned i = 0; i < count; ++i) {
				IsVectorMTTxItem = ctx.txCtx->Delete(field, {id, i}) && IsVectorMTTxItem;
			}
			IsVectorMTTxItem = IsVectorMTTxItem && count;
		}
		if (!IsVectorMTTxItem) {
			// Delete value from index
			bool needClearCache{false};
			assertrx_dbg(index.Opts().IsSparse() || skrefs.size() == pl.GetFieldLen(field));
			index.Delete(skrefs, id, MustExist_True, *strHolder_, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
		}
	} while (++field != borderIdx);

	// free PayloadValue
	itemsDataSize_ -= items_[id].GetCapacity() + sizeof(PayloadValue::dataHeader);
	items_[id].Free();
	free_.push_back(id);
	if (free_.size() == items_.size()) {
		free_.resize(0);
		items_.resize(0);
	}
	markUpdated(IndexOptimization::Full, ctx);
}

void NamespaceImpl::removeIndex(std::unique_ptr<Index>&& idx) {
	if (idx->HoldsStrings() && (!strHoldersWaitingToBeDeleted_.empty() || !strHolder_.unique())) {
		strHolder_->Add(std::move(idx));
	}
}

void NamespaceImpl::doTruncate(UpdatesContainer& pendedRepl, const NsContext& ctx) {
	const FieldsSet* pk = pkFields();
	if (!pk) {
		throwCannotModifyNsWithoutPK();
	}
	const bool storageIsValid = storage_.IsValid();
	for (size_t id = 0, sz = items_.size(); id < sz; ++id) {
		auto& pv = items_[IdType::FromNumber(id)];
		if (pv.IsFree()) {
			continue;
		}
		std::ignore = wal_.Set(WALRecord(), pv.GetLSN(), false);
		if (storageIsValid) {
			Payload pl(payloadType(), pv);
			WrSerializer pkBuf;
			pkBuf << kRxStorageItemPrefix;
			assertrx_dbg(pk);
			pl.SerializeFields(pkBuf, *pk);
			storage_.Remove(pkBuf.Slice());
		}
	}
	items_.clear();
	free_.clear();
	repl_.checksum = {};
	itemsDataSize_ = 0;
	for (size_t i = 0; i < indexes().size(); ++i) {
		if (indexes()[i]->IsFloatVector()) {
			storage_.Remove(ann_storage_cache::GetStorageKey(indexes()[i]->Name()));
			annStorageCacheState_.Remove(indexes()[i]->Name());
		}
		const IndexOpts opts = indexes()[i]->Opts();
		std::unique_ptr<Index> newIdx{Index::New(getIndexDefinition(i), PayloadType{indexes()[i]->GetPayloadType()},
												 FieldsSet{indexes()[i]->Fields()}, config_.cacheConfig, itemsCount())};
		newIdx->SetOpts(opts);
		removeIndex(indexRegistry_.ReplaceIndex(i, std::move(newIdx)));
	}
	indexOptimizer_.UpdateSortedIdxCount(indexes(), name_);

	WrSerializer ser;
	WALRecord wrec(WalUpdateQuery, (ser << "TRUNCATE " << name_).Slice());

	const auto lsn = wal_.Add(wrec, ctx.GetOriginLSN());
	markUpdated(IndexOptimization::Full);

	pendedRepl.emplace_back(updates::URType::Truncate, name_, lsn, repl_.nsVersion, ctx.EmitterServerId());
}

void NamespaceImpl::ModifyItem(Item& item, ItemModifyMode mode, const RdxContext& rdxCtx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	const NsContext ctx(rdxCtx);
	UpdatesContainer pendedRepl;

	if (ctx.GetOriginLSN().isEmpty() && (mode == ModeUpdate || mode == ModeInsert || mode == ModeUpsert)) {
		item.Embed(rdxCtx);
	}

	static PerfStatCounterMT dummyCounter;
	PerfStatCalculatorMT lockCalc(dummyCounter, enablePerfCounters_);
	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();
	lockCalc.SetCounter(updatePerfCounter_);
	lockCalc.LockHit();
	lockCalc.Disable();

	auto* pk = pkFields();
	if (!pk) {
		throwCannotModifyNsWithoutPK();
	}
	if (mode == ModeDelete && *pk != item.PkFields()) [[unlikely]] {
		throw Error(errNotValid, "Item has outdated PK metadata (probably PK has been changed during the Delete-call)");
	}
	modifyItem(item, mode, pendedRepl, ctx);

	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, ctx);
}

void NamespaceImpl::Truncate(const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx(rdxCtx);
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();

	calc.LockHit();

	doTruncate(pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, ctx);
}

void NamespaceImpl::Refill(std::vector<Item>& items, const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx(rdxCtx);

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();

	assertrx_throw(isSystem());	 // TODO: Refill currently will not be replicated, so it's available for system ns only

	doTruncate(pendedRepl, ctx);
	for (Item& i : items) {
		doModifyItem(i, ModeUpsert, pendedRepl, ctx);
	}
	tryForceFlush(std::move(wlck));
}

ReplicationState NamespaceImpl::GetReplState(const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	return getReplState();
}

ReplicationStateV2 NamespaceImpl::GetReplStateV2(const RdxContext& ctx) const {
	ReplicationStateV2 state;
	auto rlck = rLock(ctx);
	state.lastLsn = wal_.LastLSN();
	state.checksum = repl_.checksum;
	state.dataCount = itemsCount();
	state.nsVersion = repl_.nsVersion;
	state.clusterStatus = repl_.clusterStatus;
	return state;
}

ReplicationState NamespaceImpl::getReplState() const {
	ReplicationState ret = repl_;
	ret.dataCount = itemsCount();
	ret.lastLsn = wal_.LastLSN();
	return ret;
}

LocalTransaction NamespaceImpl::NewTransaction(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	const FieldsSet* pk = pkFields();
	if (!pk) {
		return LocalTransaction(Error{errLogic, "Cannot start transaction: namespace '{}' doesn't contain PK index", name_});
	}
	return LocalTransaction(name_, payloadType(), tagsMatcher(), *pk, schema_, ctx.GetOriginLSN());
}

void NamespaceImpl::CommitTransaction(LocalTransaction& tx, LocalQueryResults& result, const NsContext& ctx,
									  QueryStatCalculator<LocalTransaction, long_actions::Logger>& queryStatCalculator) {
	Locker::WLockT wlck;

	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	if (ctx.isCopiedNsRequest) {
		calc.Disable();	 // Those stats will be calculated on Namespace level
	} else {
		CounterGuardAIR32 cg(cancelCommitCnt_);
		wlck = queryStatCalculator.CreateLock(*this, &NamespaceImpl::dataWLock, ctx.rdxContext, true);
		cg.Reset();
		calc.LockHit();
		tx.ValidatePK(pkFields());
	}

	checkClusterRole(ctx.rdxContext);  // Check request source. Throw exception if false
	if (ctx.IsWalSyncItem()) {
		checkSnapshotLSN(tx.GetLSN());
	} else {
		checkClusterStatus(tx.GetLSN());  // Check tx itself. Throw exception if false
	}

	logFmt(LogTrace, "[repl:{}]:{} CommitTransaction start", name_, wal_.GetServer());

	constexpr static unsigned kMinItemsForMultithreadInsertion = 200;
	auto& steps = tx.GetSteps();
	const auto annInsertionThreads = config_.txVecInsertionThreads;
	TransactionContext txCtx(*this, tx);
	const bool useMultithreadANNInsertions = txCtx.HasMultithreadIndexes() && annInsertionThreads > 1 &&
											 (steps.size() - tx.DeletionsCount() >= kMinItemsForMultithreadInsertion) &&
											 tx.UpdateQueriesCount() == 0 && tx.DeleteQueriesCount() == 0;
	TransactionContext* txCtxPtr = nullptr;
	if (useMultithreadANNInsertions) {
		for (const auto& idx : tx.ExpectedFVInsertionsCount()) {
			indexes()[idx.IndexNo()]->GrowFor(idx.InsertionsCount());
		}
		txCtxPtr = &txCtx;
	}

	// markUpdated() is collapsed into a single call per tx: no one is able to observe the intermediate ns state under the write lock.
	// It still has to be applied before any select inside the commit (i.e. before the query steps), which relies on that state
	std::optional<IndexOptimization> deferredMarkUpdated;
	auto applyDeferredMarkUpdated = [&] {
		if (deferredMarkUpdated) {
			assertrx_dbg(ctx.isCopiedNsRequest || wlck.owns_lock());
			markUpdated(*deferredMarkUpdated);
			deferredMarkUpdated.reset();
		}
	};
	// Replication records are pushed by chunks, so the replication threads are able to drain the queue concurrently with the commit
	UpdatesContainer pendedRepl;
	pendedRepl.reserve(std::min(tx.GetSteps().size(), kTxReplAsyncBatchSize));
	auto flushPendedRepl = [&] {
		if (pendedRepl.empty()) {
			return;
		}
		if (ctx.IsInSnapshot()) {
			// Snapshot application does not emit any updates
			pendedRepl.resize(0);
			return;
		}
		UpdatesContainer toSend;
		std::swap(toSend, pendedRepl);
		replicateAsync(std::move(toSend), ctx.rdxContext);
	};

	// On error the followers never get the CommitTx record and drop the whole tx, so the pended records require no filtering here
	auto txCommitGuard = MakeScopeGuard([&]() noexcept {
		try {
			flushPendedRepl();
		} catch (const std::exception& err) {
			logFmt(LogError, "[repl:{}]:{} Unable to flush the pended replication records of the interrupted tx commit: {}", name_,
				   wal_.GetServer(), err.what());
		}
		try {
			applyDeferredMarkUpdated();
		} catch (const std::exception& err) {
			logFmt(LogError, "[repl:{}]:{} Unable to apply the deferred markUpdated of the interrupted tx commit: {}", name_,
				   wal_.GetServer(), err.what());
		}
	});

	{
		// Insert data in concurrent vector indexes in ScopeGuard
		TransactionConcurrentInserter mtInserter(*this, annInsertionThreads);
		auto mtInsertGuard = MakeScopeGuard([&mtInserter, txCtxPtr] {
			if (txCtxPtr) {
				mtInserter(*txCtxPtr);
			}
		});
		{
			WALRecord initWrec(WalInitTransaction, IdType::Zero(), true);
			auto lsn = wal_.Add(initWrec, tx.GetLSN());
			if (!ctx.IsInSnapshot()) {
				replicateAsync({updates::URType::BeginTx, name_, lsn, repl_.nsVersion, ctx.EmitterServerId()}, ctx.rdxContext);
			}
		}

		AsyncStorage::AdviceGuardT storageAdvice;
		if (tx.GetSteps().size() >= AsyncStorage::kLimitToAdviceBatching) {
			storageAdvice = storage_.AdviceBatching();
		}

		result.addNSContext(payloadType(), tagsMatcher(), nullptr, schema_, incarnationTag_);

		for (auto&& step : tx.GetSteps()) {
			switch (step.type_) {
				case TransactionStep::Type::ModifyItem: {
					const auto mode = std::get<TransactionItemStep>(step.data_).mode;
					const auto lsn = step.lsn_;
					Item item = tx.GetItem(std::move(step));
					modifyItem(item, mode, pendedRepl, NsContext(ctx).InTransaction(lsn, txCtxPtr).DeferMarkUpdated(deferredMarkUpdated));
					result.AddItemNoHold(item, incarnationTag_);
					break;
				}
				case TransactionStep::Type::Query: {
					// Query step performs a select over this ns, so the deferred ns state update has to be applied before it
					applyDeferredMarkUpdated();
					functions::PrecomputedValues precomputedValues;
					LocalQueryResults qr;
					auto& data = std::get<TransactionQueryStep>(step.data_);
					const auto lsn = step.lsn_;
					std::optional<Query> query{std::in_place, (std::move(*data.query))};
					OptimizeFunctionEntries(query.value(), query, precomputedValues);
					NsContext stepCtx(ctx);
					std::ignore = stepCtx.InTransaction(lsn, txCtxPtr).DeferMarkUpdated(deferredMarkUpdated);
					if (Impl(*query).Type() == QueryDelete) {
						doDeleteTr(qr, pendedRepl, Impl(query.value()), stepCtx, precomputedValues);
					} else {
						doUpdateTr(qr, pendedRepl, Impl(query.value()), stepCtx, precomputedValues);
					}
					for (const auto& it : qr.Items()) {
						result.AddItemRef(it.GetItemRef().Id(), PayloadValue());
					}
					break;
				}
				case TransactionStep::Type::Nop:
					assertrx(ctx.IsInSnapshot());
					// NOLINTNEXTLINE (bugprone-unused-return-value)
					std::ignore = wal_.Add(WALRecord(WalEmpty), step.lsn_);
					break;
				case TransactionStep::Type::PutMeta: {
					auto& data = std::get<TransactionMetaStep>(step.data_);
					putMeta(data.key, data.value, pendedRepl, NsContext(ctx).InTransaction(step.lsn_, txCtxPtr));
					break;
				}
				case TransactionStep::Type::SetTM: {
					auto& data = std::get<TransactionTmStep>(step.data_);
					auto tmCopy = data.tm;
					setTagsMatcher(std::move(tmCopy), pendedRepl, NsContext(ctx).InTransaction(step.lsn_, txCtxPtr));
					break;
				}
				default:
					std::abort();
			}
			if (pendedRepl.size() >= kTxReplAsyncBatchSize) {
				flushPendedRepl();
				pendedRepl.reserve(kTxReplAsyncBatchSize);
			}
		}

		flushPendedRepl();
		processWalRecord(WALRecord(WalCommitTransaction, IdType::Zero(), true), ctx);
		logFmt(LogTrace, "[repl:{}]:{} CommitTransaction end", name_, wal_.GetServer());

		// Concurrent insertions must be finished while the namespace write lock is still held.
		// replicate() below releases the lock, and mtInsertGuard would otherwise fire after that
		if (txCtxPtr) {
			mtInserter(*txCtxPtr);
		}
		mtInsertGuard.Disable();

		applyDeferredMarkUpdated();
		txCommitGuard.Disable();
		// Drop batching advice before replicate() releases the write lock. Otherwise quantize() Flush skips the open storage chunk.
		storageAdvice.Reset();

		if (!ctx.IsInSnapshot() && !ctx.isCopiedNsRequest) {
			// If commit happens in ns copy, then the copier have to handle replication
			UpdatesContainer commitRepl;
			commitRepl.emplace_back(updates::URType::CommitTx, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId());
			replicate(std::move(commitRepl), std::move(wlck), true, queryStatCalculator, ctx);
			return;
		} else if (ctx.IsInSnapshot() && ctx.isRequireResync) {
			replicateAsync({ctx.isInitialLeaderSync ? updates::URType::ResyncNamespaceLeaderInit : updates::URType::ResyncNamespaceGeneric,
							name_, lsn_t(0, 0), lsn_t(0, 0), ctx.EmitterServerId()},
						   ctx.rdxContext);
		}
	}
	if (!ctx.isCopiedNsRequest) {
		queryStatCalculator.LogFlushDuration(*this, &NamespaceImpl::tryForceFlush, std::move(wlck));
	}
}

void NamespaceImpl::doUpsert(ItemImpl& item, IdType id, bool doUpdate, TransactionContext* txCtx) {
	// upsert fields to indexes
	assertrx(items_.exists(id));
	auto& plData = items_[id];

	// inplace payload
	Payload pl(payloadType(), plData);

	Payload plNew = item.GetPayload();
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	Variant oldData;
	h_vector<bool, 32> needUpdateCompIndexes;
	if (doUpdate) {
		const size_t compIndexesCount = indexes().compositeIndexesSize();
		if (compIndexesCount) {
			oldData = Variant{items_[id]};
		}
		plData.Clone(pl.RealSize());

		repl_.checksum ^= calculateItemChecksum(id);
		itemsDataSize_ -= plData.GetCapacity() + sizeof(PayloadValue::dataHeader);

		needUpdateCompIndexes = h_vector<bool, 32>(compIndexesCount, false);
		for (size_t field = 0; field < compIndexesCount; ++field) {
			const auto& fields = indexes()[field + indexes().firstCompositePos()]->Fields();
			for (const auto f : fields) {
				if (f == IndexValueType::SetByJsonPath) {
					continue;
				}
				pl.Get(f, skrefs);
				plNew.Get(f, krefs);
				if (skrefs != krefs) {
					needUpdateCompIndexes[field] = true;
					break;
				}
			}
			if (needUpdateCompIndexes[field]) {
				continue;
			}
			for (size_t i = 0, end = fields.getTagsPathsLength(); i < end; ++i) {
				const auto& tp = fields.getTagsPath(i);
				pl.GetByJsonPath(tp, skrefs, KeyValueType::Undefined{});
				plNew.GetByJsonPath(tp, krefs, KeyValueType::Undefined{});
				if (skrefs != krefs) {
					needUpdateCompIndexes[field] = true;
					break;
				}
			}
		}
	}

	plData.SetLSN(item.Value().GetLSN());

	// Upserting fields to dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assertrx(indexes().firstCompositePos() != 0);
	const int borderIdx = indexes().totalSize() > 1 ? 1 : 0;
	int field = borderIdx;
	do {
		field %= indexes().firstCompositePos();
		Index& index = *indexes()[field];
		const auto isIndexSparse = index.Opts().IsSparse();
		if (isIndexSparse) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			try {
				plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
			} catch (const std::exception& e) {
				logFmt(LogError, "[{}]:{} Unable to index sparse value (index name: '{}'): '{}'", name_, wal_.GetServer(), index.Name(),
					   e.what());
				assertrx(false);
			}
		} else {
			plNew.Get(field, skrefs);
		}

		const bool isMultithreadTxInsertion = txCtx && index.IsSupportMultithreadTransactions();

		// Check for update
		if (doUpdate) {
			if (isIndexSparse) {
				try {
					pl.GetByJsonPath(index.Fields().getTagsPath(0), krefs, index.KeyType());
				} catch (const std::exception& e) {
					logFmt(LogError, "[{}]:{} Unable to remove sparse value from the index (index name: '{}'): '{}'", name_,
						   wal_.GetServer(), index.Name(), e.what());
					assertrx(false);
				}
			} else if (index.Opts().IsFloatVector()) {
				const size_t elemsCount = pl.GetFieldLen(field);
				getFloatVectorView(krefs, index, id, elemsCount, isMultithreadTxInsertion, txCtx, field);
			} else if (index.Opts().IsArray()) {
				pl.Get(field, krefs, Variant::hold);
			} else {
				pl.Get(field, krefs);
			}
			if (krefs == skrefs) {
				// Do not modify indexes, if documents content was not changed
				continue;
			}

			bool txCtxModificationNotRequired = !isMultithreadTxInsertion;
			if (!txCtxModificationNotRequired) {
				const auto count = pl.GetFieldLen(field);
				txCtxModificationNotRequired = true;
				for (unsigned i = 0; i < count; ++i) {
					if (txCtx->TryGetValue(field, {id, i}).has_value()) {
						txCtxModificationNotRequired = false;
						break;
					}
				}
			}
			if (txCtxModificationNotRequired) {
				// No txCtx modification required here
				bool needClearCache{false};
				index.Delete(krefs, id, MustExist_True, *strHolder_, needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(index);
				}
			}
		}
		if (isMultithreadTxInsertion) {
			assertrx(!isIndexSparse);
			if (!index.Opts().IsArray() && skrefs.empty()) {
				skrefs.emplace_back(ConstFloatVectorView{}, Variant::noHold);
			}
			for (unsigned i = 0, count = skrefs.size(); i < count; ++i) {
				skrefs[i] = Variant{txCtx->Upsert(field, {id, i}, skrefs[i].As<ConstFloatVectorView>()), Variant::noHold};
			}
			pl.Set(field, skrefs);
		} else {
			// Put value to index
			krefs.resize(0);
			bool needClearCache{false};
			index.Upsert(krefs, skrefs, id, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}

			if (!isIndexSparse) {
				// Put value to payload
				pl.Set(field, krefs);
			}
		}
	} while (++field != borderIdx);

	// Upsert to composite indexes
	for (int field2 = indexes().firstCompositePos(); field2 < indexes().totalSize(); ++field2) {
		// No txCtx modification required for composite indexes

		auto& idxRef = *indexes()[field2];
		bool needClearCache{false};
		if (doUpdate) {
			if (!needUpdateCompIndexes[field2 - indexes().firstCompositePos()]) {
				bool refreshed = idxRef.RefreshCompositeKey(Variant{plData}, id);
				assertrx_dbg(refreshed);
				if (!refreshed) [[unlikely]] {
					logFmt(LogError, "[{}]: Unable to refresh key for {} during item update", name_, idxRef.Name());
				}
				continue;
			}
			// Delete from composite indexes first
			assertrx_dbg(!oldData.IsNullValue());
			idxRef.Delete(oldData, id, MustExist_True, *strHolder_, needClearCache);
		}
		std::ignore = idxRef.Upsert(Variant{plData}, id, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(idxRef);
		}
	}
	repl_.checksum ^= calculateItemChecksum(id);
	itemsDataSize_ += plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
	item.RealValue() = plData;
}

void NamespaceImpl::getFloatVectorView(VariantArray& result, const Index& index, IdType rowId, size_t elementsCount,
									   bool isMultithreadTxInsertion, const TransactionContext* txCtx, int field) {
	krefs.clear<false>();
	krefs.reserve(elementsCount);
	for (unsigned i = 0; i < elementsCount; ++i) {
		if (isMultithreadTxInsertion) {
			if (auto fvView = txCtx->TryGetValue(field, {rowId, i}); fvView.has_value()) {
				result.emplace_back(fvView.value());
				continue;
			}
		}
		const FloatVectorIndex& fvIdx = static_cast<const FloatVectorIndex&>(index);
		result.emplace_back(getFloatVector({rowId, i}, fvIdx));
	}
}

void NamespaceImpl::updateTagsMatcherFromItem(ItemImpl* ritem, const NsContext& ctx) {
	if (ritem->tagsMatcher().isUpdated()) {
		logFmt(LogTrace, "Updated TagsMatcher of namespace '{}' on modify:\n{}", name_, ritem->tagsMatcher().Dump());
		if (!ctx.GetOriginLSN().isEmpty()) [[unlikely]] {
			throw Error(errLogic, "{}: Replicated item requires explicit tagsmatcher update: {}", name_, ritem->GetJSON());
		}
	}
	if (ritem->Type().get() != payloadType().get() ||
		(ritem->tagsMatcher().isUpdated() && !indexRegistry_.GetTagsMatcher().try_merge(ritem->tagsMatcher()))) {
		std::string jsonSliceBuf(ritem->GetJSON());
		logFmt(LogTrace, "Conflict TagsMatcher of namespace '{}' on modify: item:\n{}\ntm is\n{}\nnew tm is\n {}\n", name_, jsonSliceBuf,
			   tagsMatcher().Dump(), ritem->tagsMatcher().Dump());

		ItemImpl tmpItem(payloadType(), tagsMatcher());
		tmpItem.Value().SetLSN(ritem->Value().GetLSN());
		*ritem = std::move(tmpItem);

		auto err = ritem->FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) {
			throw err;
		}

		if (ritem->tagsMatcher().isUpdated() && !indexRegistry_.GetTagsMatcher().try_merge(ritem->tagsMatcher())) [[unlikely]] {
			throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		}
		ritem->tagsMatcher() = tagsMatcher();
		ritem->tagsMatcher().setUpdated();
	} else if (ritem->tagsMatcher().isUpdated()) {
		ritem->tagsMatcher() = tagsMatcher();
		ritem->tagsMatcher().setUpdated();
	}
}

void NamespaceImpl::modifyItem(Item& item, ItemModifyMode mode, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (mode == ModeDelete) {
		deleteItem(item, pendedRepl, ctx);
	} else {
		doModifyItem(item, mode, pendedRepl, ctx);
	}
}

void NamespaceImpl::deleteItem(Item& item, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	ItemImpl* ritem = item.impl_;
	const auto oldTmV = tagsMatcher().version();
	updateTagsMatcherFromItem(ritem, ctx);

	auto itItem = findByPK(ritem, ctx.IsInTransaction(), ctx.rdxContext);
	IdType id = itItem.first;

	item.setID(IdType::NotSet());
	if (itItem.second || ctx.IsWalSyncItem()) {
		item.setID(id);

		WrSerializer cjson;
		WALRecord wrec{WalItemModify, ritem->GetCJSON(cjson, WithTagsMatcher_False), ritem->tagsMatcher().version(), ModeDelete,
					   ctx.IsInTransaction()};

		if (itItem.second) {
			ritem->RealValue() = items_[id];
			if (!ctx.txCtx) {
				if (auto data = floatVectorsGetterFn(id)(payloadType(), items_[id], tagsMatcher()); !data.empty()) {
					ritem->RealValue().Clone();
					Payload pl(payloadType(), ritem->RealValue());
					VariantArray buf;
					for (auto& [fvIdx, values] : data) {
						buf.clear<false>();
						buf.reserve(values.size());
						for (auto& val : values) {
							if (ritem->floatVectorsHolder_.Add(FloatVector{std::move(val)})) {
								buf.emplace_back(ritem->floatVectorsHolder_.Back());
							} else {
								buf.emplace_back(ConstFloatVectorView{});
							}
						}
						pl.Set(fvIdx.ptField, buf);
					}
				}
			}
			doDelete(id, ctx);
		}

		replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);

		lsn_t itemLsn(item.GetLSN());
		processWalRecord(std::move(wrec), ctx, itemLsn, &item);
		pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::ItemDeleteTx : updates::URType::ItemDelete, name_, wal_.LastLSN(),
								repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
	}
}

void NamespaceImpl::doModifyItem(Item& item, ItemModifyMode mode, UpdatesContainer& pendedRepl, const NsContext& ctx, IdType suggestedId) {
	// Item to doUpsert
	assertrx(mode != ModeDelete);
	const auto oldTmV = tagsMatcher().version();
	ItemImpl* itemImpl = item.impl_;
	setFieldsBasedOnPrecepts(itemImpl, pendedRepl, ctx);
	updateTagsMatcherFromItem(itemImpl, ctx);
	auto newPl = itemImpl->GetPayload();

	auto realItem = findByPK(itemImpl, ctx.IsInTransaction(), ctx.rdxContext);
	const bool exists = realItem.second;

	if ((exists && mode == ModeInsert) || (!exists && mode == ModeUpdate)) {
		item.setID(IdType::NotSet());
		replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
		return;
	}

	// Validate strings encoding
	for (int field = 1, regularIndexes = indexes().firstCompositePos(); field < regularIndexes; ++field) {
		const Index& index = *indexes()[field];
		if (index.Opts().GetCollateMode() == CollateUTF8 && index.KeyType().Is<KeyValueType::String>()) {
			if (index.Opts().IsSparse()) {
				assertrx(index.Fields().getTagsPathsLength() > 0);
				newPl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, KeyValueType::String{});
			} else {
				newPl.Get(field, skrefs);
			}

			for (auto& key : skrefs) {
				key.EnsureUTF8();
			}
		}
	}
	// Build tuple if it does not exist
	itemImpl->BuildTupleIfEmpty();

	if (suggestedId.IsValid() && exists && suggestedId != realItem.first) [[unlikely]] {
		throw Error(errParams, "Suggested ID doesn't correspond to real ID: {} vs {}", suggestedId, realItem.first);
	}
	const IdType id = exists ? realItem.first : createItem(newPl.RealSize(), suggestedId, ctx);

	replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
	lsn_t lsn;
	if (ctx.IsForceSyncItem()) {
		lsn = ctx.GetOriginLSN();
	} else {
		lsn = wal_.Add(WALRecord(WalItemUpdate, id, ctx.IsInTransaction()), ctx.GetOriginLSN(), exists ? items_[id].GetLSN() : lsn_t());
	}
	assertrx(!lsn.isEmpty());

	item.setLSN(lsn);
	item.setID(id);

	doUpsert(*itemImpl, id, exists, ctx.txCtx);

	WrSerializer cjson;
	std::ignore = itemImpl->GetCJSON(cjson, WithTagsMatcher_False);

	saveTagsMatcherToStorage(true);
	if (storage_.IsValid()) {
		const FieldsSet* pk = pkFields();
		assertrx(pk);
		WrSerializer pkBuf, cjsonBuf;
		pkBuf << kRxStorageItemPrefix;
		newPl.SerializeFields(pkBuf, *pk);
		cjsonBuf.PutUInt64(int64_t(lsn));
		cjsonBuf.Write(cjson.Slice());
		storage_.Write(pkBuf.Slice(), cjsonBuf.Slice());
	}

	markUpdated(exists ? IndexOptimization::Partial : IndexOptimization::Full, ctx);

	auto type = updates::URType::None;
	switch (mode) {
		case ModeUpdate:
			type = ctx.IsInTransaction() ? updates::URType::ItemUpdateTx : updates::URType::ItemUpdate;
			break;
		case ModeInsert:
			type = ctx.IsInTransaction() ? updates::URType::ItemInsertTx : updates::URType::ItemInsert;
			break;
		case ModeUpsert:
			type = ctx.IsInTransaction() ? updates::URType::ItemUpsertTx : updates::URType::ItemUpsert;
			break;
		case ModeDelete:
			type = ctx.IsInTransaction() ? updates::URType::ItemDeleteTx : updates::URType::ItemDelete;
			break;
	}
	pendedRepl.emplace_back(type, name_, lsn, repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
}

PayloadType NamespaceImpl::GetPayloadType(const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	return payloadType();
}

RX_ALWAYS_INLINE VariantArray NamespaceImpl::getPkKeys(const ConstPayload& cpl, Index* pkIndex, int fieldNum) {
	// It is a faster alternative of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	VariantArray keys;
	if (IsComposite(pkIndex->Type())) {
		keys.emplace_back(*cpl.Value());
	} else {
		cpl.Get(fieldNum, keys);
	}
	return keys;
}

std::pair<Index*, int> NamespaceImpl::getPkIdx() const noexcept {
	int pkPos = 0;
	if (!tryGetIndexByName(kPKIndexName, pkPos)) {
		return std::pair<Index*, int>(nullptr, -1);
	}
	return std::make_pair(indexes()[pkPos].get(), pkPos);
}

RX_ALWAYS_INLINE SelectKeyResult NamespaceImpl::getPkDocs(const ConstPayload& cpl, bool inTransaction, const RdxContext& ctx) {
	auto [pkIndex, pkField] = getPkIdx();
	if (!pkIndex) [[unlikely]] {
		throwCannotModifyNsWithoutPK();
	}
	VariantArray keys = getPkKeys(cpl, pkIndex, pkField);
	assertf(keys.size() == 1, "Pkey field must contain 1 key, but there '{}' in '{}.{}'", keys.size(), name_, pkIndex->Name());
	Index::SelectContext selectContext;
	selectContext.opts.inTransaction = inTransaction;
	return pkIndex->SelectKey(keys, CondEq, 0, selectContext, ctx).Front();
}

// find id by PK. NOT THREAD SAFE!
std::pair<IdType, bool> NamespaceImpl::findByPK(ItemImpl* ritem, bool inTransaction, const RdxContext& ctx) {
	SelectKeyResult res = getPkDocs(ritem->GetConstPayload(), inTransaction, ctx);
	if (!res.empty() && !res[0].TryGetFlatIDSet().empty()) {
		assertrx_dbg(res[0].TryGetFlatIDSet().size() == 1);
		return {res[0].TryGetFlatIDSet()[0], true};
	}
	return {IdType::NotSet(), false};
}

void NamespaceImpl::throwDuplicatePK(const ConstPayload& cpl, IdType itemId, IdType conflictingItemId) {
	auto [pkIndex, pkPos] = getPkIdx();
	assertrx_throw(pkIndex);
	VariantArray keys = getPkKeys(cpl, pkIndex, pkPos);
	WrSerializer wrser;
	wrser << "Duplicate Primary Key {" << pkIndex->Name() << ": ";
	keys.Dump(wrser, PayloadType(cpl.Type()), pkIndex->Fields(), CheckIsStringPrintable::No);
	wrser << "} for rows [" << itemId.ToNumber() << ", " << conflictingItemId.ToNumber() << "]!";
	throw Error(errLogic, wrser.Slice());
}

void NamespaceImpl::throwCannotModifyNsWithoutPK() const {
	throw Error(errLogic, "Trying to modify namespace '{}', but it doesn't contain PK index", name_);
}

void NamespaceImpl::optimizeIndexes(const NsContext& ctx) {
	// Background FT cleanup is independent from sort-index optimization (own timeout / may run when
	// optimization_sort_workers=0 or OptimizationState::Completed). Sort optimization still gated below.
	Locker::RLockT rlck;
	if (!ctx.isCopiedNsRequest) {
		rlck = rLock(ctx.rdxContext);
	}
	if (isSystem() || isTemporary()) {
		return;
	}

	class [[nodiscard]] ConcurrentCancel final : public index::ICancelable {
	public:
		ConcurrentCancel(const NamespaceImpl& ns) noexcept : cancelCommitCnt_{ns.cancelCommitCnt_}, dbDestroyed_{ns.dbDestroyed_} {}

		virtual bool IsCanceled() const noexcept override {
			return cancelCommitCnt_.load(std::memory_order_relaxed) || dbDestroyed_.load(std::memory_order_relaxed);
		}

	private:
		const std::atomic_int32_t& cancelCommitCnt_;
		const std::atomic<bool>& dbDestroyed_;
	};

	const ConcurrentCancel cancelable(*this);
	tryCleanFulltextIndexes(ctx.isCopiedNsRequest, cancelable);

	// This is read lock only, atomics-based implementation of background indexes optimization.
	// If indexOptimizer_.State() == OptimizationState::Completed, then indexes are completely built.
	// If indexOptimizer_.State() == OptimizationState::Error, then indexes can not be optimized for some unexpected reason.
	if (!indexOptimizer_.IsOptimizationAvailable()) {
		return;
	}

	indexOptimizer_.TryOptimize(
		IndexOptimizer::Context{.nsName = name_,
								.enablePerfCounters = enablePerfCounters_.load(),
								.skipTimeCheck = ctx.isCopiedNsRequest,
								.lastUpdateTime = std::chrono::milliseconds(lastUpdateTime_.load(std::memory_order_acquire)),
								.indexes = indexes(),
								.items = items_},
		cancelable);
}

void NamespaceImpl::tryCleanFulltextIndexes(bool skipTimeCheck, const index::ICancelable& cancelable) {
	using namespace std::chrono;
	if (config_.ftCleanupTimeout <= 0) {
		return;
	}
	const auto lastUpdateTime = milliseconds(lastUpdateTime_.load(std::memory_order_acquire));
	if (!lastUpdateTime.count()) {
		return;
	}
	if (!skipTimeCheck) {
		const auto now = duration_cast<milliseconds>(system_clock_w::now().time_since_epoch());
		if ((now - lastUpdateTime) < milliseconds(config_.ftCleanupTimeout)) {
			return;
		}
	}

	const bool enablePerfCounters = enablePerfCounters_.load(std::memory_order_relaxed);
	for (auto& idx : indexes()) {
		if (cancelable.IsCanceled()) {
			return;
		}
		if (!idx->NeedsClean()) {
			continue;
		}
		idx->Clean(cancelable, enablePerfCounters);
	}
}

void NamespaceImpl::markUpdated(IndexOptimization requestedOptimization) {
	using namespace std::chrono;
	itemsCount_.store(items_.size(), std::memory_order_relaxed);
	itemsCapacity_.store(items_.capacity(), std::memory_order_relaxed);
	indexOptimizer_.ScheduleOptimization(requestedOptimization);
	clearNamespaceCaches();
	lastUpdateTime_.store(duration_cast<milliseconds>(system_clock_w::now().time_since_epoch()).count(), std::memory_order_release);
	if (!nsIsLoading_) {
		repl_.updatedUnixNano = getTimeNow(TimeUnit::nsec);
	}
}

void NamespaceImpl::markUpdated(IndexOptimization requestedOptimization, const NsContext& ctx) {
	if (auto* slot = ctx.DeferredMarkUpdated()) {
		if (!*slot || requestedOptimization == IndexOptimization::Full) {
			*slot = requestedOptimization;
		}
		return;
	}
	markUpdated(requestedOptimization);
}

Item NamespaceImpl::newItem() {
	const auto* pk = pkFields();
	auto impl_ = pool_.get(0, payloadType(), tagsMatcher(), pk ? *pk : FieldsSet{}, schema_);
	impl_->tagsMatcher() = tagsMatcher();
	impl_->tagsMatcher().clearUpdated();
	impl_->schema() = schema_;
#ifdef RX_WITH_STDLIB_DEBUG
	assertrx_dbg(pk ? impl_->PkFields() == *pk : impl_->PkFields() == FieldsSet{});
#endif	// RX_WITH_STDLIB_DEBUG
	return Item(impl_.release());
}

void NamespaceImpl::doUpdateTr(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
							   const functions::PrecomputedValues& precomputedValues) {
	NsSelecter selecter(this);
	MainSelectCtx selCtx(query, std::nullopt, nullptr);
	FtFunctionsHolder func;
	selCtx.functions = &func;
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.IsInTransaction();
	selCtx.explain = nullptr;  // No explain for tx updates
	selecter(result, selCtx, ctx.rdxContext);
	doUpdate(result, pendedRepl, query, ctx, precomputedValues);
}

void NamespaceImpl::doUpdate(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
							 const functions::PrecomputedValues& precomputedValues) {
	const FieldsSet* pk = pkFields();
	if (!pk) {
		throwCannotModifyNsWithoutPK();
	}

	ActiveQueryScope queryScope(query, QueryUpdate, indexOptimizer_.StateRef(), strHolder_.get());
	const auto tmStart = system_clock_w::now();

	bool updateWithJson = false;
	bool withExpressions = false;
	for (const UpdateEntry& ue : query.UpdateFields()) {
		if (!withExpressions && ue.IsExpression()) {
			withExpressions = true;
		}
		if (!updateWithJson && ue.Mode() == FieldModeSetJson) {
			updateWithJson = true;
		}
		if (withExpressions && updateWithJson) {
			break;
		}
	}

	if (!ctx.GetOriginLSN().isEmpty() && withExpressions) [[unlikely]] {
		throw Error(errLogic, "Can't apply update query with expression to follower's ns '{}'", name_);
	}

	if (!ctx.IsInTransaction()) {
		ThrowOnCancel(ctx.rdxContext);
	}

	// If update statement is expression and contains function calls then we use
	// row-based replication (to preserve data consistency), otherwise we update
	// it via 'WalUpdateQuery' (statement-based replication). If Update statement
	// contains update of entire object (via JSON) then statement replication is not possible.
	// bool statementReplication =
	// 	(!updateWithJson && !withExpressions && !query.HasLimit() && !query.HasOffset() && (result.Count() >= kWALStatementItemsThreshold));
	constexpr bool statementReplication = false;

	ItemModifier itemModifier(query.UpdateFields(), *this, pendedRepl, ctx, precomputedValues);
	pendedRepl.reserve(pendedRepl.size() + result.Count());	 // Required for item-based replication only
	const FieldsFilter pkFilter = FieldsFilter::FromFieldsSet(*pk, payloadType(), *this);
	for (auto& it : result) {
		ItemRef& item = it.GetItemRef();
		assertrx(items_.exists(item.Id()));
		const auto oldTmV = tagsMatcher().version();
		PayloadValue& pv(items_[item.Id()]);
		Payload pl(payloadType(), pv);

		const uint64_t oldItemHash = calculateItemChecksum(item.Id());
		size_t oldItemCapacity = pv.GetCapacity();
		const bool isPKModified = itemModifier.Modify(item.Id(), ctx, pendedRepl);
		std::optional<PKModifyRevertData> modifyData;
		if (isPKModified) {
			// statementReplication = false;
			modifyData.emplace(itemModifier.GetPayloadValueBackup(), item.Value().GetLSN());
		}

		replicateItem(item.Id(), ctx, statementReplication, oldItemHash, oldItemCapacity, oldTmV, std::move(modifyData), pendedRepl,
					  pkFilter);
		item.Value() = items_[item.Id()];
	}
	result.getTagsMatcher(0) = tagsMatcher();
	// Tx commit copies only item IDs from this QR and drops payloads, so holding vectors is wasted work.
	if (!ctx.IsInTransaction()) {
		result.GetFloatVectorsHolder().Add(*this, result.begin(), result.end(), FieldsFilter{query.SelectFilters(), *this});
	}
	assertrx(ctx.IsInTransaction() ? !result.IsNamespaceAdded(this) : result.IsNamespaceAdded(this));

	// Disabled due to statement base replication logic conflicts (#1771)
	// lsn_t lsn;
	//	if (statementReplication) {
	//		WrSerializer ser;
	//		const_cast<Query &>(query).type_ = QueryUpdate;
	//		WALRecord wrec(WalUpdateQuery, query.GetSQL(ser, QueryUpdate).Slice(), ctx.IsInTransaction());
	//		lsn = wal_.Add(wrec, ctx.GetOriginLSN());
	//		if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;
	//		for (ItemRef &item : result.Items()) {
	//			item.Value().SetLSN(lsn);
	//		}
	//		if (!isTemporary())
	//			observers_->OnWALUpdate(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_, wrec);
	//		if (!ctx.rdxContext.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
	//	}

	if (query.DebugLevel() >= LogInfo) {
		logFmt(LogInfo, "Updated {} items in {} µs", result.Count(), duration_cast<microseconds>(system_clock_w::now() - tmStart).count());
	}
	//	if (statementReplication) {
	//		assertrx(!lsn.isEmpty());
	//		pendedRepl.emplace_back(UpdateRecord::Type::UpdateQuery, name_, lsn, query);
	//	}
}

void NamespaceImpl::replicateItem(IdType itemId, const NsContext& ctx, bool statementReplication, uint64_t oldItemHash,
								  size_t oldItemCapacity, int oldTmVersion, std::optional<PKModifyRevertData>&& modifyData,
								  UpdatesContainer& pendedRepl, const FieldsFilter& pkFilter) {
	const FieldsSet* pk = pkFields();
	PayloadValue& pv(items_[itemId]);
	Payload pl(payloadType(), pv);

	if (!statementReplication) {
		replicateTmUpdateIfRequired(pendedRepl, oldTmVersion, ctx);
		auto sendWalUpdate = [this, itemId, &ctx, &pv, &pendedRepl](updates::URType mode) {
			lsn_t lsn;
			if (ctx.IsForceSyncItem()) {
				lsn = ctx.GetOriginLSN();
			} else {
				lsn = wal_.Add(WALRecord(WalItemUpdate, itemId, ctx.IsInTransaction()), lsn_t(), items_[itemId].GetLSN());
			}
			assertrx(!lsn.isEmpty());

			pv.SetLSN(lsn);
			ItemImpl item(payloadType(), pv, tagsMatcher());
			item.Unsafe(true);
			item.CopyIndexedVectorsValuesFrom(floatVectorsGetterFn(itemId));
			WrSerializer cjson;
			std::ignore = item.GetCJSON(cjson, WithTagsMatcher_False);
			pendedRepl.emplace_back(mode, name_, lsn, repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
		};

		if (modifyData.has_value()) {
			WrSerializer cjson;
			ConstPayload plSave(payloadType(), modifyData->pv);
			CJsonBuilder builder(cjson, ObjType::TypePlain);
			CJsonEncoder encoder(&tagsMatcher(), &pkFilter);
			encoder.Encode(plSave, builder);
			processWalRecord(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher().version(), ModeDelete, ctx.IsInTransaction()), ctx,
							 modifyData->lsn);
			pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::ItemDeleteTx : updates::URType::ItemDelete, name_,
									wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
			sendWalUpdate(ctx.IsInTransaction() ? updates::URType::ItemInsertTx : updates::URType::ItemInsert);
		} else {
			sendWalUpdate(ctx.IsInTransaction() ? updates::URType::ItemUpdateTx : updates::URType::ItemUpdate);
		}
	}

	repl_.checksum ^= oldItemHash;
	repl_.checksum ^= calculateItemChecksum(itemId);
	itemsDataSize_ -= oldItemCapacity;
	itemsDataSize_ += pl.Value()->GetCapacity();

	saveTagsMatcherToStorage(true);
	if (storage_.IsValid()) {
		assertrx(pk);
		WrSerializer pkBuf, itemBuf;
		if (modifyData.has_value()) {
			Payload plSave(payloadType(), modifyData->pv);
			pkBuf << kRxStorageItemPrefix;
			plSave.SerializeFields(pkBuf, *pk);
			storage_.Remove(pkBuf.Slice());
			pkBuf.Reset();
		}
		pkBuf << kRxStorageItemPrefix;
		pl.SerializeFields(pkBuf, *pk);
		itemBuf.PutUInt64(uint64_t(pv.GetLSN()));
		ItemImpl item(payloadType(), pv, tagsMatcher());
		item.Unsafe(true);
		item.CopyIndexedVectorsValuesFrom(floatVectorsGetterFn(itemId));
		storage_.Write(pkBuf.Slice(), item.GetCJSON(itemBuf));
	}
}

void NamespaceImpl::doDeleteTr(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
							   const functions::PrecomputedValues& precomputedValues) {
	NsSelecter selecter(this);
	// Tx commit copies only item IDs from this QR and drops payloads, so holding vectors is wasted work.
	MainSelectCtx selCtx(query, std::nullopt, nullptr);
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.IsInTransaction();
	selCtx.explain = nullptr;  // No explain for tx deletes
	FtFunctionsHolder func;
	selCtx.functions = &func;
	selecter(result, selCtx, ctx.rdxContext);
	doDelete(result, pendedRepl, query, ctx, precomputedValues);
}

void NamespaceImpl::doDelete(LocalQueryResults& result, UpdatesContainer& pendedRepl, ConstQueryImpl query, const NsContext& ctx,
							 const functions::PrecomputedValues&) {
	const FieldsSet* pk = pkFields();
	if (!pk) {
		throwCannotModifyNsWithoutPK();
	}
	const FieldsFilter pkFilter = FieldsFilter::FromFieldsSet(*pk, payloadType(), *this);

	ActiveQueryScope queryScope(query, QueryDelete, indexOptimizer_.StateRef(), strHolder_.get());
	const auto tmStart = system_clock_w::now();
	const auto oldTmV = tagsMatcher().version();
	for (const auto& it : result.Items()) {
		doDelete(it.GetItemRef().Id(), ctx);
	}

	// TODO disabled due to #1771
	// if (ctx.IsWalSyncItem() || (!q.HasLimit() && !q.HasOffset() && result.Count() >= kWALStatementItemsThreshold)) {
	// 	WrSerializer ser;
	// 	const_cast<Query&>(q).type_ = QueryDelete;
	// 	processWalRecord(WALRecord(WalUpdateQuery, q.GetSQL(ser, QueryDelete).Slice(), ctx.IsInTransaction()), ctx);
	// 	pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::DeleteQueryTx : updates::URType::DeleteQuery, name_,
	// wal_.LastLSN(), 							repl_.nsVersion, ctx.EmitterServerId(), std::string(ser.Slice())); } else {
	replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
	for (auto& it : result) {
		WrSerializer cjson;
		ConstPayload pl(payloadType(), it.GetItemRef().Value());
		CJsonBuilder builder(cjson, ObjType::TypePlain);
		CJsonEncoder encoder(&tagsMatcher(), &pkFilter);
		encoder.Encode(pl, builder);
		processWalRecord(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher().version(), ModeDelete, ctx.IsInTransaction()), ctx,
						 it.GetLSN());
		pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::ItemDeleteTx : updates::URType::ItemDelete, name_, wal_.LastLSN(),
								repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
	}
	// }
	if (query.DebugLevel() >= LogInfo) {
		logFmt(LogInfo, "Deleted {} items in {} µs", result.Count(), duration_cast<microseconds>(system_clock_w::now() - tmStart).count());
	}
	assertrx(ctx.IsInTransaction() ? !result.IsNamespaceAdded(this) : result.IsNamespaceAdded(this));
}

void NamespaceImpl::checkClusterRole(lsn_t originLsn) const {
	switch (repl_.clusterStatus.role) {
		case ClusterOperationStatus::Role::None:
			if (!originLsn.isEmpty()) [[unlikely]] {
				throw Error(errWrongReplicationData, "Can't modify ns '{}' with 'None' replication status from node {}", name_,
							originLsn.Server());
			}
			break;
		case ClusterOperationStatus::Role::SimpleReplica:
			if (originLsn.isEmpty()) [[unlikely]] {
				throw Error(errWrongReplicationData, "Can't modify replica's ns '{}' without origin LSN", name_, originLsn.Server());
			}
			break;
		case ClusterOperationStatus::Role::ClusterReplica:
			if (originLsn.isEmpty() || originLsn.Server() != repl_.clusterStatus.leaderId) [[unlikely]] {
				throw Error(errWrongReplicationData, "Can't modify cluster ns '{}' with incorrect origin LSN: ({}) (s1:{} s2:{})", name_,
							originLsn, originLsn.Server(), repl_.clusterStatus.leaderId);
			}
			break;
	}
}

void NamespaceImpl::checkClusterStatus(lsn_t originLsn) const {
	checkClusterRole(originLsn);
	if (!originLsn.isEmpty() && wal_.LSNCounter() != originLsn.Counter()) [[unlikely]] {
		throw Error(errWrongReplicationData, "Can't modify cluster ns '{}' with incorrect origin LSN: ({}). Expected counter value: ({})",
					name_, originLsn, wal_.LSNCounter());
	}
}

void NamespaceImpl::checkSnapshotLSN(lsn_t lsn) {
	// Just in case of some unexpected scenarios
	const static bool kDisableSnapshotCheck = std::getenv("REINDEXER_NO_SNAPSHOT_CHECK");
	if (!kDisableSnapshotCheck && wal_.LastLSN().Counter() > lsn.Counter()) [[unlikely]] {
		// Do not expect to get this error in tests scenarios
		assertrx_dbg(false);
		throw Error(errParams,
					"Target namespace has unexpected LSN counter: {}. First LSN in snapshot chunk is {}. Snapshot's data are incompatible",
					wal_.LastLSN(), lsn);
	}
}

// NOLINTNEXTLINE(bugprone-exception-escape) Termination here is better, than inconsistent state of the user's data
void NamespaceImpl::replicateTmUpdateIfRequired(UpdatesContainer& pendedRepl, int oldTmVersion, const NsContext& ctx) noexcept {
	if (oldTmVersion != tagsMatcher().version()) {
		assertrx(ctx.GetOriginLSN().isEmpty());
		const auto lsn = wal_.Add(WALRecord(WalEmpty, IdType::Zero(), ctx.IsInTransaction()), lsn_t());
		pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::SetTagsMatcherTx : updates::URType::SetTagsMatcher, name_, lsn,
								repl_.nsVersion, ctx.EmitterServerId(), tagsMatcher());
	}
}

template <typename SelectCtxT>
void NamespaceImpl::Select(LocalQueryResults& result, SelectCtxT& params, const RdxContext& ctx) {
	if (!params.query.IsWALQuery()) [[likely]] {
		NsSelecter selecter(this);
		selecter(result, params, ctx);
	} else {
		WALSelecter selecter(this, true);
		selecter(result, params);
	}
}
template void NamespaceImpl::Select(LocalQueryResults&, MainSelectCtx&, const RdxContext&);
template void NamespaceImpl::Select(LocalQueryResults&, JoinPreSelectCtx&, const RdxContext&);
template void NamespaceImpl::Select(LocalQueryResults&, JoinSelectCtx&, const RdxContext&);

IndexDef NamespaceImpl::getIndexDefinition(size_t i) const {
	assertrx(i < indexes().size());
	const Index& index = *indexes()[i];

	if (static_cast<int>(i) >= payloadType().NumFields()) {
		int fIdx = 0;
		JsonPaths jsonPaths;
		for (auto& f : index.Fields()) {
			if (f != IndexValueType::SetByJsonPath) {
				jsonPaths.push_back(indexes()[f]->Name());
			} else {
				jsonPaths.push_back(index.Fields().getJsonPath(fIdx++));
			}
		}
		return {index.Name(), std::move(jsonPaths), index.Type(), index.Opts(), index.GetTTLValue()};
	} else {
		return {index.Name(), payloadType().Field(i).JsonPaths(), index.Type(), index.Opts(), index.GetTTLValue()};
	}
}

NamespaceDef NamespaceImpl::getDefinition() const {
	NamespaceDef nsDef(std::string(name_), StorageOpts().Enabled(storage_.GetStatusCached().isEnabled));
	nsDef.indexes.reserve(indexes().size());
	for (size_t i = 1; i < indexes().size(); ++i) {
		nsDef.AddIndex(getIndexDefinition(i));
	}
	if (schema_) {
		WrSerializer ser;
		schema_->GetJSON(ser);
		nsDef.schemaJson = std::string(ser.Slice());
	}
	return nsDef;
}

NamespaceDef NamespaceImpl::GetDefinition(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return getDefinition();
}

NamespaceMemStat NamespaceImpl::GetMemStat(const RdxContext& ctx) {
	NamespaceMemStat ret;
	auto rlck = rLock(ctx);
	ret.name = name_;
	ret.type = NamespaceMemStat::kNamespaceStatType;
	ret.joinCache = joinCache_.GetMemStat();
	ret.queryCache = queryCountCache_.GetMemStat();

	ret.itemsCount = itemsCount();
	*(static_cast<ReplicationState*>(&ret.replication)) = getReplState();
	ret.replication.walCount = size_t(wal_.size());
	ret.replication.walSize = wal_.heap_size();
	if (!isSystem()) {
		ret.replication.serverId = wal_.GetServer();
	} else {
		ret.replication.serverId = repl_.nsVersion.Server();
	}

	ret.emptyItemsCount = free_.size();

	ret.Total.dataSize = itemsDataSize_ + items_.capacity() * sizeof(PayloadValue);
	ret.Total.cacheSize = ret.joinCache.totalSize + ret.queryCache.totalSize;
	ret.Total.indexOptimizerMemory = indexOptimizer_.UpdateSortedContextMemory();
	ret.Storage.proxySize = storage_.GetProxyMemStat();
	ret.Total.inmemoryStorageSize = ret.Storage.proxySize;
	ret.indexes.reserve(indexes().size());
	for (const auto& idx : indexes()) {
		ret.indexes.emplace_back(idx->GetMemStat(ctx));
		auto& istat = ret.indexes.back();
		istat.sortOrdersSize = idx->IsOrdered() ? (items_.size() * sizeof(IdType)) : 0;
		ret.Total.indexesSize += istat.GetFullIndexStructSize();
		ret.Total.dataSize += istat.dataSize;
		ret.Total.cacheSize += istat.idsetCache.totalSize;
	}

	const auto storageStatus = storage_.GetStatusCached();
	ret.storageOK = storageStatus.isEnabled && storageStatus.err.ok();
	ret.storageEnabled = storageStatus.isEnabled;
	if (storageStatus.isEnabled) {
		if (storageStatus.err.ok()) {
			ret.storageStatus = "OK"sv;
		} else if (checkIfEndsWith<CaseSensitive::Yes>("No space left on device"sv, storageStatus.err.what())) {
			ret.storageStatus = "NO SPACE LEFT"sv;
		} else {
			ret.storageStatus = storageStatus.err.what();
		}
	} else {
		ret.storageStatus = "DISABLED"sv;
	}
	ret.storagePath = storage_.GetPathCached();
	ret.optimizationCompleted = indexOptimizer_.IsOptimizationCompleted();

	ret.stringsWaitingToBeDeletedSize = strHolder_->MemStat();
	for (const auto& idx : strHolder_->Indexes()) {
		const auto& istat = idx->GetMemStat(ctx);
		ret.stringsWaitingToBeDeletedSize += istat.GetFullIndexStructSize() + istat.dataSize;
	}
	for (const auto& strHldr : strHoldersWaitingToBeDeleted_) {
		ret.stringsWaitingToBeDeletedSize += strHldr->MemStat();
		for (const auto& idx : strHldr->Indexes()) {
			const auto& istat = idx->GetMemStat(ctx);
			ret.stringsWaitingToBeDeletedSize += istat.GetFullIndexStructSize() + istat.dataSize;
		}
	}

	ret.tagsMatcher.tagsCount = tagsMatcher().size();
	ret.tagsMatcher.version = tagsMatcher().version();
	ret.tagsMatcher.stateToken = tagsMatcher().stateToken();

	logFmt(LogTrace, "[GetMemStat:{}]:{} replication (checksum={}  dataCount={}  lastLsn={})", ret.name, wal_.GetServer(),
		   ret.replication.checksum, ret.replication.dataCount, ret.replication.lastLsn);

	return ret;
}

NamespacePerfStat NamespaceImpl::GetPerfStat(const RdxContext& ctx) {
	NamespacePerfStat ret;

	auto rlck = rLock(ctx);

	ret.name = name_;
	ret.selects = selectPerfCounter_.Get<PerfStat>();
	ret.updates = updatePerfCounter_.Get<PerfStat>();
	ret.joinCache = joinCache_.GetPerfStat();
	ret.queryCountCache = queryCountCache_.GetPerfStat();
	ret.indexes.reserve(indexes().size() - 1);
	for (unsigned i = 1; i < indexes().size(); i++) {
		ret.indexes.emplace_back(indexes()[i]->GetIndexPerfStat());
	}
	return ret;
}

void NamespaceImpl::ResetPerfStat(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	selectPerfCounter_.Reset();
	updatePerfCounter_.Reset();
	for (auto& i : indexes()) {
		i->ResetIndexPerfStat();
	}
	queryCountCache_.ResetPerfStat();
	joinCache_.ResetPerfStat();
	if (embeddersCache_) {
		embeddersCache_->ResetPerfStat();
	}
}

Error NamespaceImpl::loadLatestSysRecord(std::string_view baseSysTag, uint64_t& version, std::string& content) {
	std::string key(baseSysTag);
	key.append(".");
	std::string latestContent;
	version = 0;
	Error err;
	for (int i = 0; i < kSysRecordsBackupCount; ++i) {
		content.clear();
		Error status = storage_.Read(StorageOpts().FillCache(), std::string_view(key + std::to_string(i)), content);
		if (!status.ok() && status.code() != errNotFound) [[unlikely]] {
			logFmt(LogTrace, "Error on namespace service info(tag: {}, id: {}) load '{}': {}", baseSysTag, i, name_, status.what());
			err = Error(errNotValid, "Error load namespace from storage '{}': {}", name_, status.what());
			continue;
		}

		if (status.ok() && !content.empty()) {
			Serializer ser(content.data(), content.size());
			auto curVersion = ser.GetUInt64();
			if (curVersion >= version) {
				version = curVersion;
				std::swap(latestContent, content);
				content.clear();
				err = Error();
			}
		}
	}

	if (latestContent.empty()) {
		content.clear();
		Error status = storage_.Read(StorageOpts().FillCache(), baseSysTag, content);
		if (!content.empty()) {
			logFmt(LogTrace, "Converting {} for {} to new format", baseSysTag, name_);
			WrSerializer ser;
			ser.PutUInt64(version);
			ser.Write(std::string_view(content));
			writeSysRecToStorage(ser.Slice(), baseSysTag, version, true);
		}
		if (!status.ok() && status.code() != errNotFound) [[unlikely]] {
			return Error(errNotValid, "Error load namespace from storage '{}': {}", name_, status.what());
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
	assertrx(indexes().size() == 1);
	assertrx(items_.empty());

	std::string def;
	Error status = loadLatestSysRecord(kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (!def.empty()) {
		Serializer ser(def.data(), def.size());
		indexRegistry_.GetTagsMatcher().deserialize(ser);
		indexRegistry_.GetTagsMatcher().clearUpdated();
		logFmt(LogInfo, "[tm:{}]:{}: TagsMatcher was loaded from storage. tm: {{ state_token: {:#08x}, version: {} }}", name_,
			   wal_.GetServer(), tagsMatcher().stateToken(), tagsMatcher().version());
		logFmt(LogTrace, "Loaded tags(version: {}) of namespace {}:\n{}",
			   sysRecordsVersions_.tagsVersion ? sysRecordsVersions_.tagsVersion - 1 : 0, name_, tagsMatcher().Dump());
	}

	def.clear();
	status = loadLatestSysRecord(kStorageSchemaPrefix, sysRecordsVersions_.schemaVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (!def.empty()) {
		schema_ = std::make_shared<Schema>();
		Serializer ser(def.data(), def.size());
		status = schema_->FromJSON(ser.GetSlice());
		if (!status.ok()) {
			throw status;
		}
		std::string_view schemaStr = schema_->GetJSON();
		// NOLINTNEXTLINE(bugprone-suspicious-stringview-data-usage)
		schemaStr = std::string_view(schemaStr.data(), std::min(schemaStr.size(), kMaxSchemaCharsToPrint));
		logFmt(LogInfo, "Loaded schema(version: {}) of the namespace '{}'. First {} symbols of the schema are: '{}'",
			   sysRecordsVersions_.schemaVersion ? sysRecordsVersions_.schemaVersion - 1 : 0, name_, schemaStr.size(), schemaStr);
	}

	def.clear();
	status = loadLatestSysRecord(kStorageIndexesPrefix, sysRecordsVersions_.idxVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}

	if (!def.empty()) {
		Serializer ser(def.data(), def.size());
		const uint32_t dbMagic = ser.GetUInt32();
		const uint32_t dbVer = ser.GetUInt32();
		if (dbMagic != kStorageMagic) {
			logFmt(LogError, "Storage magic mismatch. want {:#08x}, got {:#08x}", kStorageMagic, dbMagic);
			return false;
		}
		if (dbVer != kStorageVersion) {
			logFmt(LogError, "Storage version mismatch. want {:#08x}, got {:#08x}", kStorageVersion, dbVer);
			return false;
		}

		int count = int(ser.GetVarUInt());
		while (count--) {
			std::string_view indexData = ser.GetVString();
			auto indexDef = IndexDef::FromJSON(giftStr(indexData));
			Error err;
			if (indexDef) {
				try {
					verifyUpsertIndex("add", *indexDef);
					addIndex(*indexDef, false);
				} catch (const Error& e) {
					err = e;
				} catch (std::exception& e) {
					err = Error(errLogic, "Exception: '{}'", e.what());
				}
			} else {
				err = indexDef.error();
			}
			if (!err.ok()) {
				logFmt(LogError, "Error adding index '{}': {}", indexDef ? indexDef->Name() : "?", err.what());
			}
		}
	}

	if (schema_) {
		auto err = schema_->BuildProtobufSchema(indexRegistry_.GetTagsMatcher(), payloadType());
		if (!err.ok()) {
			logFmt(LogInfo, "Unable to build protobuf schema for the '{}' namespace: {}", name_, err.what());
		}
	}

	logFmt(LogTrace, "Loaded index structure(version {}) of namespace '{}'\n{}",
		   sysRecordsVersions_.idxVersion ? sysRecordsVersions_.idxVersion - 1 : 0, name_, payloadType()->ToString());

	return true;
}

void NamespaceImpl::loadReplStateFromStorage() {
	std::string json;
	Error status = loadLatestSysRecord(kStorageReplStatePrefix, sysRecordsVersions_.replVersion, json);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}

	if (!json.empty()) {
		logFmt(LogTrace, "[load_repl:{}]:{} Loading replication state(version {}) of namespace {}: {}", name_, wal_.GetServer(),
			   sysRecordsVersions_.replVersion ? sysRecordsVersions_.replVersion - 1 : 0, name_, json);
		repl_.FromJSON(giftStr(json));
	}
	{
		WrSerializer serLog;
		JsonBuilder builderLog(serLog, ObjType::TypePlain);
		repl_.GetJSON(builderLog);
		logFmt(LogTrace, "[load_repl:{}]:{} Loading replication state {}", name_, wal_.GetServer(), serLog.Slice());
	}
}

void NamespaceImpl::loadMetaFromStorage() {
	StorageOpts opts;
	opts.FillCache(false);
	auto dbIter = storage_.GetCursor(opts);
	size_t prefixLen = kStorageMetaPrefix.length();

	for (dbIter->Seek(kStorageMetaPrefix);
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), kStorageMetaPrefix + kFFFFFFFF) < 0; dbIter->Next()) {
		std::string_view keySlice = dbIter->Key();
		if (keySlice.length() >= prefixLen) {
			meta_.emplace(keySlice.substr(prefixLen), dbIter->Value());
		}
	}
}

void NamespaceImpl::saveIndexesToStorage() {
	// clear ItemImpl pool on payload change
	pool_.clear();

	if (!storage_.IsValid()) {
		return;
	}

	logFmt(LogTrace, "Namespace::saveIndexesToStorage ({})", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.idxVersion);
	ser.PutUInt32(kStorageMagic);
	ser.PutUInt32(kStorageVersion);

	ser.PutVarUint(indexes().size() - 1);
	NamespaceDef nsDef = getDefinition();

	WrSerializer wrser;
	for (const IndexDef& indexDef : nsDef.indexes) {
		wrser.Reset();
		indexDef.GetJSON(wrser);
		ser.PutVString(wrser.Slice());
	}

	writeSysRecToStorage(ser.Slice(), kStorageIndexesPrefix, sysRecordsVersions_.idxVersion, true);

	saveTagsMatcherToStorage(false);
	saveReplStateToStorage();
}

void NamespaceImpl::saveSchemaToStorage() {
	if (!storage_.IsValid()) {
		return;
	}

	logFmt(LogTrace, "Namespace::saveSchemaToStorage ({})", name_);

	if (!schema_) {
		return;
	}

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.schemaVersion);
	{
		auto sliceHelper = ser.StartSlice();
		schema_->GetJSON(ser);
	}

	writeSysRecToStorage(ser.Slice(), kStorageSchemaPrefix, sysRecordsVersions_.schemaVersion, true);

	saveTagsMatcherToStorage(false);
	saveReplStateToStorage();
}

void NamespaceImpl::saveReplStateToStorage(bool direct) {
	if (!storage_.IsValid()) {
		return;
	}

	if (direct) {
		replStateUpdates_.store(0, std::memory_order_release);
	}

	logFmt(LogTrace, "Namespace::saveReplStateToStorage ({})", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.replVersion);
	JsonBuilder builder(ser);
	ReplicationState st = getReplState();
	st.GetJSON(builder);
	builder.End();
	writeSysRecToStorage(ser.Slice(), kStorageReplStatePrefix, sysRecordsVersions_.replVersion, direct);
}

void NamespaceImpl::saveTagsMatcherToStorage(bool clearUpdate) {
	if (storage_.IsValid() && tagsMatcher().isUpdated()) {
		WrSerializer ser;
		ser.PutUInt64(sysRecordsVersions_.tagsVersion);
		tagsMatcher().serialize(ser);
		if (clearUpdate) {	// Update flags should be cleared after some items updates (to replicate tagsmatcher with WALItemModify record)
			indexRegistry_.GetTagsMatcher().clearUpdated();
		}
		writeSysRecToStorage(ser.Slice(), kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, false);
		logFmt(LogTrace, "Saving tags of namespace {}:\n{}", name_, tagsMatcher().Dump());
	}
}

void NamespaceImpl::EnableStorage(const std::string& path, StorageOpts opts, StorageType storageType, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	std::string dbpath = fs::JoinPath(path, name_);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	bool success = false;
	const bool storageDirExists = (fs::Stat(dbpath) == fs::StatDir);
	try {
		while (!success) {
			if (!opts.IsCreateIfMissing() && !storageDirExists) [[unlikely]] {
				throw Error(errNotFound,
							"Storage directory doesn't exist for namespace '{}' on path '{}' and CreateIfMissing option is not set", name_,
							path);
			}
			Error status = storage_.Open(storageType, name_, dbpath, opts);
			if (!status.ok()) {
				if (!opts.IsDropOnFileFormatError()) [[unlikely]] {
					storage_.Close();
					throw Error(errLogic, "Cannot enable storage for namespace '{}' on path '{}' - {}", name_, path, status.what());
				}
			} else {
				success = loadIndexesFromStorage();
				if (!success && !opts.IsDropOnFileFormatError()) [[unlikely]] {
					storage_.Close();
					throw Error(errLogic, "Cannot enable storage for namespace '{}' on path '{}': format error", name_, dbpath);
				}
				loadReplStateFromStorage();
				loadMetaFromStorage();
			}
			if (!success && opts.IsDropOnFileFormatError()) {
				logFmt(LogWarning, "Dropping storage for namespace '{}' on path '{}' due to format error", name_, dbpath);
				opts.DropOnFileFormatError(false);
				storage_.Destroy();
			}
		}
	} catch (...) {
		// if storage was created by this call
		if (!storageDirExists && (fs::Stat(dbpath) == fs::StatDir)) {
			logFmt(LogWarning, "Dropping storage (via {}), which was created with errors ('{}':'{}')",
				   storage_.IsValid() ? "storage interface" : "filesystem", name_, dbpath);
			if (storage_.IsValid()) {
				storage_.Destroy();
			} else if (fs::RmDirAll(dbpath) != 0) {
				logFmt(LogError, "Failed to remove directory '{}', error: '{}'", dbpath, strerror(errno));
			}
		}
		throw;
	}

	storageOpts_ = opts;
}

std::shared_ptr<const Schema> NamespaceImpl::GetSchemaPtr(const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	return schema_;
}

void NamespaceImpl::SetClusterOperationStatus(ClusterOperationStatus&& status, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	switch (status.role) {
		case ClusterOperationStatus::Role::None:
			if (isTemporary()) [[unlikely]] {
				throw Error{errParams, "Unable to set replication role 'none' to temporary namespace"};
			}
			wal_.OnNewLeaderSet(wal_.GetServer());
			break;
		case ClusterOperationStatus::Role::ClusterReplica:
		case ClusterOperationStatus::Role::SimpleReplica:
			wal_.OnNewLeaderSet(status.leaderId);
			break;
	}

	repl_.clusterStatus = std::move(status);
	saveReplStateToStorage(true);
}

void NamespaceImpl::ApplySnapshotChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	SnapshotHandler handler(*this);

	// Snapshot::addRawData appends WalResetLocalWal as the last raw record (last non-WAL chunk).
	// A misplaced Reset still works via WALTracker::Reset under dataWLock.
	if (!ch.IsWAL() && !ch.Records().empty() && ch.Records().back().Unpack().type == WalResetLocalWal) {
		storage_.Flush(StorageFlushOpts{});
	}

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx, true);
	cg.Reset();
	checkClusterRole(ctx);

	handler.ApplyChunk(ch, isInitialLeaderSync, pendedRepl);
	if (ch.IsLastChunk()) {
		replicateAsync({isInitialLeaderSync ? updates::URType::ResyncNamespaceLeaderInit : updates::URType::ResyncNamespaceGeneric, name_,
						lsn_t(0, 0), lsn_t(0, 0), ctx.EmitterServerId()},
					   ctx);
	}
}

void NamespaceImpl::GetSnapshot(Snapshot& snapshot, const SnapshotOpts& opts, const RdxContext& ctx) {
	SnapshotHandler handler(*this);
	auto rlck = rLock(ctx);
	snapshot = handler.CreateSnapshot(opts);
}

void NamespaceImpl::SetTagsMatcher(TagsMatcher&& tm, const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	// Intentionally do not set cancelCommit_ here - this method is called by system activities, not by user
	auto wlck = dataWLock(rdxCtx);

	setTagsMatcher(std::move(tm), pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, ctx);
}

FloatVectorsIndexes NamespaceImpl::getVectorIndexes() const {
	FloatVectorsIndexes result;
	for (size_t i = 0, count = size_t(payloadType().NumFields()); i < count; ++i) {
		const auto& field = payloadType().Field(i);
		if (!field.IsFloatVector()) {
			continue;
		}
		const auto indexId = getIndexByName(field.Name());
		if (auto idx = dynamic_cast<FloatVectorIndex*>(indexes()[indexId].get()); idx) [[likely]] {
			result.emplace_back(FloatVectorIndexData{.ptField = i, .ptr = idx});
		} else {
			throw Error(errParams, "Incorrect payload type for '{}', index '{}' must have vector type", name_, field.Name());
		}
	}
	return result;
}

FloatVectorsIndexes NamespaceImpl::getVectorIndexes(const PayloadType& pt) const {
	FloatVectorsIndexes result;
	if (!iequals(pt.Name(), payloadType().Name())) [[unlikely]] {
		throw Error(errParams, "Attempt to get vector indexes for incorrect payload type. Expected name is '{}', actual name is '{}'",
					payloadType().Name(), pt.Name());
	}
	for (size_t i = 0, total = size_t(pt.NumFields()); i < total; ++i) {
		auto& field = pt.Field(i);
		if (!field.IsFloatVector()) {
			continue;
		}
		const std::string& fieldName = field.Name();
		auto indexIt = std::ranges::find_if(indexes(), [&fieldName](const auto& idx) noexcept { return idx->Name() == fieldName; });
		if (indexIt == indexes().end()) [[unlikely]] {
			throw Error(errParams, "Index '{}' not found in '{}'", fieldName, name_);
		}
		if (auto idx = dynamic_cast<FloatVectorIndex*>(indexIt->get()); idx) {
			result.emplace_back(FloatVectorIndexData{.ptField = i, .ptr = idx});
		} else {
			throw Error(errParams, "Incorrect payload type for '{}', index '{}' must have vector type", name_, fieldName);
		}
	}
	return result;
}

void NamespaceImpl::RebuildFreeItemsStorage(const RdxContext& ctx) {
	std::vector<IdType> newFree;
	auto wlck = simpleWLock(ctx);

	if (!isTemporary()) [[unlikely]] {
		assertrx_dbg(false);
		throw Error(errLogic, "Unexpected manual free items rebuild on non-temporary namespace");
	}
	for (size_t i = 0, sz = items_.size(); i < sz; ++i) {
		const auto rowId = IdType::FromNumber(i);
		if (items_[rowId].IsFree()) {
			newFree.emplace_back(rowId);
		}
	}
	free_ = std::move(newFree);
}

void NamespaceImpl::LoadFromStorage(unsigned threadsCount, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	const auto storedChecksum = repl_.checksum;
	repl_.checksum = 0;

	migrations::PKMigrationService pkMigrationService{*this};
	pkMigrationService.RemoveItemsWithObsoletePK();

	const auto t1 = system_clock_w::now_coarse();
	loadHashMapStats();
	const auto t2 = system_clock_w::now_coarse();

	ItemsLoader loader(threadsCount, *this);
	auto ldata = loader.Load();

	initWAL(ldata.minLSN, ldata.maxLSN);
	if (!isSystem()) {
		repl_.lastLsn.SetServer(wal_.GetServer());
	}

	std::string errorString(ldata.lastErr.what());
	std::string logErrorPart = errorString.empty() ? "" : " (" + std::to_string(ldata.errCount) + " errors " + errorString + ")";
	logFmt(LogInfo,
		   "[{}] Done loading storage. {} items loaded{}, lsn #{}, total size={}M, checksum={}, hash maps stats loading time="
		   "{}ms",
		   name_, items_.size(), logErrorPart, repl_.lastLsn, ldata.ldcount / (1024 * 1024), repl_.checksum,
		   std::chrono::duration_cast<milliseconds>((t2 - t1)).count());
	if (storedChecksum != repl_.checksum) {
		logFmt(LogError, "[{}] Warning checksum mismatch {} != {}", name_, storedChecksum, repl_.checksum);
		replStateUpdates_.fetch_add(1, std::memory_order_release);
	}

	markUpdated(IndexOptimization::Full);
}

void NamespaceImpl::initWAL(int64_t minLSN, int64_t maxLSN) {
	wal_.Init(getWalSize(config_), minLSN, maxLSN, storage_);
	// Fill existing records
	for (size_t id = 0; id < items_.size(); ++id) {
		const auto rowId = IdType::FromNumber(id);
		if (!items_[rowId].IsFree()) {
			std::ignore = wal_.Set(WALRecord(WalItemUpdate, rowId), items_[rowId].GetLSN(), true);
		}
	}
	repl_.lastLsn = wal_.LastLSN();
	logFmt(LogInfo, "[{}] WAL has been initialized lsn #{}, max size {}", name_, repl_.lastLsn, wal_.Capacity());
}

void NamespaceImpl::removeExpiredItems(RdxActivityContext* ctx) {
	const RdxContext rdxCtx{ctx};
	const NsContext nsCtx{rdxCtx};
	{
		auto rlck = rLock(rdxCtx);
		if (repl_.clusterStatus.role != ClusterOperationStatus::Role::None) {
			return;
		}
	}
	UpdatesContainer pendedRepl;

	// Intentionally do not set cancelCommit_ here - this method is called in background thread, not by user
	auto wlck = dataWLock(rdxCtx);

	const auto now = std::chrono::duration_cast<std::chrono::seconds>(system_clock_w::now().time_since_epoch());
	if (now == lastExpirationCheckTs_) {
		return;
	}
	lastExpirationCheckTs_ = now;
	for (const std::unique_ptr<Index>& index : indexes()) {
		if ((index->Type() != IndexTtl) || (index->Size() == 0)) {
			continue;
		}
		const auto tmDelete = system_clock_w::now();

		if (!pkFields()) {
			logFmt(LogTrace, "Cannot remove expired (TTL) items for namespace '{}': it doesn't contain PK index", name_);
			return;
		}
		const int64_t expirationThreshold =
			std::chrono::duration_cast<std::chrono::seconds>(system_clock_w::now_coarse().time_since_epoch()).count() -
			index->GetTTLValue();
		LocalQueryResults qr;

		if (intrusive_ptr_ref_count(this) == 0) {
			assertrx_dbg(false);  // This should never happen
			logFmt(LogError, "Unable to call removeExpiredItems() on non-intrusive namespace pointer: {}", name_);
			return;
		}
		qr.AddNamespace(Ptr(this), true);
		const Query q = Query(name_).Where(index->Name(), CondLt, expirationThreshold);
		doDeleteTr(qr, pendedRepl, Impl(q), nsCtx, functions::PrecomputedValues{});
		if (qr.Count()) {
			logFmt(LogInfo, "[{}] {} items were removed: TTL({}) has expired in {} us", name_, qr.Count(), index->Name(),
				   duration_cast<microseconds>(system_clock_w::now() - tmDelete).count());
		}
	}
	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, nsCtx);
}

void NamespaceImpl::removeExpiredStrings(RdxActivityContext* ctx) {
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

void NamespaceImpl::optimizeFloatVectorKeeper(RdxActivityContext* ctx) {
	const RdxContext rdxCtx{ctx};
	auto rlck = rLock(rdxCtx);

	for (const auto& index : indexes()) {
		if (index->IsFloatVector()) {
			auto idx = dynamic_cast<FloatVectorIndex*>(index.get());
			assertrx_dbg(idx != nullptr);
			idx->GetKeeper().RemoveUnused();
		}
	}
}

void NamespaceImpl::setSchema(std::string_view schema, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	// NOLINTNEXTLINE (bugprone-suspicious-stringview-data-usage)
	std::string_view schemaPrint(schema.data(), std::min(schema.size(), kMaxSchemaCharsToPrint));
	std::string_view source = "user"sv;
	if (ctx.IsInSnapshot()) {
		source = "snapshot"sv;
	} else if (!ctx.GetOriginLSN().isEmpty()) {
		source = "replication"sv;
	}
	logFmt(LogInfo, "[{}]:{} Setting new schema from {}. First {} symbols are: '{}'", name_, wal_.GetServer(), source, schemaPrint.size(),
		   schemaPrint);
	schema_ = std::make_shared<Schema>(schema);
	const auto oldTmV = tagsMatcher().version();
	auto fields = schema_->GetPaths();
	for (auto& field : fields) {
		[[maybe_unused]] auto _ = indexRegistry_.GetTagsMatcher().path2tag(field, CanAddField_True);
	}
	if (oldTmV != tagsMatcher().version()) {
		logFmt(LogInfo,
			   "[tm:{}]:{}: TagsMatcher was updated from schema. Old tm: {{ state_token: {:#08x}, version: {} }}, new tm: {{ state_token: "
			   "{:#08x}, version: {} }}",
			   name_, wal_.GetServer(), tagsMatcher().stateToken(), oldTmV, tagsMatcher().stateToken(), tagsMatcher().version());
	}

	auto err = schema_->BuildProtobufSchema(indexRegistry_.GetTagsMatcher(), payloadType());
	if (!err.ok()) {
		logFmt(LogInfo, "Unable to build protobuf schema for the '{}' namespace: {}", name_, err.what());
	}

	replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
	addToWAL(schema, WalSetSchema, ctx);
	pendedRepl.emplace_back(updates::URType::SetSchema, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), std::string(schema));
}

void NamespaceImpl::setTagsMatcher(TagsMatcher&& tm, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (ctx.GetOriginLSN().isEmpty()) [[unlikely]] {
		throw Error(errLogic, "Tagsmatcher may be set by replication only");
	}
	if (tm.stateToken() != tagsMatcher().stateToken()) [[unlikely]] {
		throw Error(errParams, "Tagsmatcher have different statetokens: {:#08x} vs {:#08x}", tagsMatcher().stateToken(), tm.stateToken());
	}
	logFmt(
		LogInfo,
		"[tm:{}]:{} Set new TagsMatcher (replicated): {{ state_token: {:#08x}, version: {} }} -> {{ state_token: {:#08x}, version: {} }}",
		name_, wal_.GetServer(), tagsMatcher().stateToken(), tagsMatcher().version(), tm.stateToken(), tm.version());
	indexRegistry_.ReplaceTagsMatcher(TagsMatcher{tm});

	const auto lsn = wal_.Add(WALRecord(WalEmpty, IdType::Zero(), ctx.IsInTransaction()), ctx.GetOriginLSN());
	pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::SetTagsMatcherTx : updates::URType::SetTagsMatcher, name_, lsn,
							repl_.nsVersion, ctx.EmitterServerId(), std::move(tm));

	saveTagsMatcherToStorage(true);
}

void NamespaceImpl::BackgroundRoutine(RdxActivityContext* ctx) {
	const RdxContext rdxCtx(ctx);
	const NsContext nsCtx(rdxCtx);
	auto replStateUpdates = replStateUpdates_.load(std::memory_order_acquire);
	if (replStateUpdates) {
		auto wlck = simpleWLock(nsCtx.rdxContext);
		if (replStateUpdates_.load(std::memory_order_relaxed)) {
			saveReplStateToStorage(false);
			replStateUpdates_.store(0, std::memory_order_relaxed);
		}
	}
	optimizeIndexes(nsCtx);
	try {
		removeExpiredItems(ctx);
	} catch (Error& e) {
		// Catch exception from AwaitInitialSync in WLock for cluster replica in follower mode
		if (e.code() != errWrongReplicationData) {
			throw e;
		}
	}
	removeExpiredStrings(ctx);
	optimizeFloatVectorKeeper(ctx);
	backgroundHNSWIndexesQuantization(ctx);
}

void NamespaceImpl::StorageFlushingRoutine() { storage_.Flush(StorageFlushOpts()); }

void NamespaceImpl::ANNCachingRoutine() {
	const bool skipTimeCheck = false;
	UpdateANNStorageCache(skipTimeCheck, RdxContext());
}

void NamespaceImpl::loadHashMapStats() noexcept {
	try {
		std::vector<IndexHashMapStats> indexesStats;
		std::string statsKey = std::string(kStorageHashTablesStatsPrefix) + "." + std::string(name_);
		Error err = LoadIndexesHashMapStats(storage_, statsKey, indexesStats);
		if (!err.ok()) {
			logFmt(LogInfo, "[{}] Can not load hash tables stats - {}", name_, err.whatStr());
			return;
		}

		for (auto& index : indexes()) {
			for (auto& st : indexesStats) {
				if (iequals(index->Name(), st.indexName)) {
					index->ReserveHashTables(st.stats);
					break;
				}
			}
		}
	} catch (const std::exception& exc) {
		logFmt(LogError, "[{}] Error while reserving hash tables from stats: {}", name_, exc.what());
	} catch (...) {
		logFmt(LogError, "[{}] Unknown error while reserving hash tables from stats", name_);
	}
}

void NamespaceImpl::UpdateANNStorageCache(bool skipTimeCheck, const RdxContext& ctx) {
	auto rlck = tryRLock(ctx);
	if (!rlck.owns_lock()) {
		return;
	}

	if (isSystem() || isTemporary()) {
		return;
	}

	ann_storage_cache::Writer cacheWriter(*this);
	for (;;) {
		const auto lastUpdate = lastUpdateTimeNano();
		if (!lastUpdate || config_.annStorageCacheBuildTimeout <= 0 || locker_.IsInvalidated()) {
			return;
		}
		const auto lastUpdateDiff = duration_cast<milliseconds>(nanoseconds(getTimeNow(TimeUnit::nsec) - lastUpdate));
		if (!skipTimeCheck && lastUpdateDiff < milliseconds(config_.annStorageCacheBuildTimeout)) {
			return;
		}

		if (cacheWriter.TryUpdateNextPart(std::move(rlck), storage_, annStorageCacheState_, cancelCommitCnt_)) {
			rlck = tryRLock(ctx);
			if (!rlck.owns_lock()) {
				return;
			}
		} else {
			return;
		}
	}
}

void NamespaceImpl::DropANNStorageCache(std::string_view index, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);

	if (index.empty()) {
		for (auto& idx : indexes()) {
			if (idx->IsFloatVector()) {
				storage_.Remove(ann_storage_cache::GetStorageKey(idx->Name()));
				annStorageCacheState_.Remove(idx->Name());
			}
		}
	} else {
		storage_.Remove(ann_storage_cache::GetStorageKey(index));
		annStorageCacheState_.Remove(index);
	}
}

void NamespaceImpl::RebuildIVFIndex(std::string_view index, float dataPart, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);

	auto rebuildSingleIndex = [this, dataPart](Index* idx) {
		if (idx->Type() != IndexIvf) {
			return;
		}
		auto vecIdx = dynamic_cast<FloatVectorIndex*>(idx);
		vecIdx->RebuildCentroids(dataPart);

		storage_.Remove(ann_storage_cache::GetStorageKey(idx->Name()));
		annStorageCacheState_.Remove(idx->Name());
	};

	if (index.empty()) {
		for (auto& idx : indexes()) {
			rebuildSingleIndex(idx.get());
		}
	} else {
		int indexId = -1;
		if (tryGetIndexByName(index, indexId)) {
			rebuildSingleIndex(indexes()[indexId].get());
		}
	}
}

void NamespaceImpl::DeleteStorage(const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	if (!isTemporary()) {
		// Should not be able to delete non-temporary follower's storage with user's request
		checkClusterRole(ctx.GetOriginLSN());
	}

	storage_.Destroy();
}

void NamespaceImpl::CloseStorage(const RdxContext& ctx) {
	constexpr bool skipTimeCheck = true;
	// Update storage cache even if not enough time is passed
	UpdateANNStorageCache(skipTimeCheck, RdxContext());

	storage_.Flush(StorageFlushOpts().WithImmediateReopen());

	auto wlck = simpleWLock(ctx);

	saveReplStateToStorage(true);
	replStateUpdates_.store(0, std::memory_order_relaxed);
	storage_.Close();
}

std::string NamespaceImpl::sysRecordName(std::string_view sysTag, uint64_t version) {
	std::string backupRecord(sysTag);
	static_assert(kSysRecordsBackupCount && ((kSysRecordsBackupCount & (kSysRecordsBackupCount - 1)) == 0),
				  "kBackupsCount has to be power of 2");
	backupRecord.append(".").append(std::to_string(version & (kSysRecordsBackupCount - 1)));
	return backupRecord;
}

void NamespaceImpl::writeSysRecToStorage(std::string_view data, std::string_view sysTag, uint64_t& version, bool direct) {
	size_t iterCount = (version > 0) ? 1 : kSysRecordsFirstWriteCopies;
	for (size_t i = 0; i < iterCount; ++i, ++version) {
		unaligned::write<uint64_t>(const_cast<char*>(data.data()), version);
		if (direct) {
			storage_.WriteSync(StorageOpts().FillCache().Sync(0 == version), sysRecordName(sysTag, version), data);
		} else {
			storage_.Write(sysRecordName(sysTag, version), data);
		}
	}
}

Item NamespaceImpl::NewItem(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return newItem();
}

void NamespaceImpl::ToPool(ItemImpl* item) {
	item->Clear();
	pool_.put(std::unique_ptr<ItemImpl>{item});
}

// Get metadata from storage by key
std::string NamespaceImpl::GetMeta(const std::string& key, const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return getMeta(key);
}

std::string NamespaceImpl::getMeta(const std::string& key) const {
	if (key.empty()) [[unlikely]] {
		throw Error(errParams, "Empty key is not supported");
	}

	auto it = meta_.find(key);
	return it == meta_.end() ? std::string() : it->second;
}

// Put metadata to storage by key
void NamespaceImpl::PutMeta(const std::string& key, std::string_view data, const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	// This method does not affects index data, but it's called directly be user's API, and can not be delayed by background indexes commit
	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();

	putMeta(key, data, pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::putMeta(const std::string& key, std::string_view data, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (key.empty()) [[unlikely]] {
		throw Error(errParams, "Empty key is not supported");
	}

	meta_[key] = std::string(data);

	storage_.Write(kStorageMetaPrefix + key, data);

	processWalRecord(WALRecord(WalPutMeta, key, data, ctx.IsInTransaction()), ctx);
	pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::PutMetaTx : updates::URType::PutMeta, name_, wal_.LastLSN(),
							repl_.nsVersion, ctx.EmitterServerId(), key, std::string(data));
}

std::vector<std::string> NamespaceImpl::EnumMeta(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return enumMeta();
}

std::vector<std::string> NamespaceImpl::enumMeta() const {
	std::vector<std::string> keys(meta_.size());
	transform(meta_.begin(), meta_.end(), keys.begin(), [](const auto& pair) { return pair.first; });
	return keys;
}

// Delete metadata from storage by key
void NamespaceImpl::DeleteMeta(const std::string& key, const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	// This method does not affects index data, but it's called directly be user's API, and can not be delayed by background indexes commit
	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();

	deleteMeta(key, pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::deleteMeta(const std::string& key, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (key.empty()) [[unlikely]] {
		throw Error(errParams, "Empty key is not supported");
	}
	if (ctx.IsInTransaction()) [[unlikely]] {
		throw Error(errParams, "DeleteMeta command not supported in transaction mode");
	}

	meta_.erase(key);

	storage_.Remove(kStorageMetaPrefix + key);

	processWalRecord(WALRecord(WalDeleteMeta, key, ctx.IsInTransaction()), ctx);
	pendedRepl.emplace_back(updates::URType::DeleteMeta, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), key, std::string());
}

void NamespaceImpl::warmupFtIndexes() {
	for (auto& idx : indexes()) {
		if (idx->IsFulltext()) {
			idx->CommitFulltext();
		}
	}
}

IdType NamespaceImpl::createItem(size_t realSize, IdType suggestedId, const NsContext& ctx) {
	IdType id = IdType::Zero();
	if (!suggestedId.IsValid()) {
		if (!free_.empty()) {
			id = free_.back();
			free_.pop_back();
			assertrx(id < IdType::FromNumber(items_.size()));
			assertrx(items_[id].IsFree());
			items_[id] = PayloadValue(realSize);
		} else {
			id = IdType::FromNumber(items_.size());
			if (id == IdType::Max()) [[unlikely]] {
				throw Error(errParams, "Max item ID value is reached. Unable to store more than {} items in '{}' namespace", id, name_);
			}
			items_.emplace_back(PayloadValue(realSize));
		}
	} else {
		if (!ctx.IsForceSyncItem()) [[unlikely]] {
			throw Error(errParams, "Suggested ID should only be used during force-sync replication: {}", suggestedId);
		}
		id = suggestedId;
		if (id < IdType::FromNumber(items_.size())) {
			if (!items_[id].IsFree()) [[unlikely]] {
				throw Error(errParams, "Suggested ID {} is not empty", id);
			}
		} else {
			items_.resize(id.ToNumber() + 1);
			items_[id] = PayloadValue(realSize);
		}
	}
	return id;
}

void NamespaceImpl::setFieldsBasedOnPrecepts(ItemImpl* ritem, UpdatesContainer& replUpdates, const NsContext& ctx) {
	functions::PrecomputedValues precomputedValues;
	for (auto& precept : ritem->GetPrecepts()) {
		auto sqlFunc = functions::FunctionParser::Parse(precept);

		skrefs.clear<false>();
		if (sqlFunc.isFunction) {
			functions::FunctionInvoker funcInvoker{precomputedValues, *this, payloadType(), tagsMatcher(), replUpdates};
			skrefs.emplace_back(funcInvoker.Invoke(functions::Create(functions::ParsedFunction(sqlFunc)), ctx, ritem->Value()));
		} else {
			skrefs.emplace_back(make_key_string(sqlFunc.value));
		}
		int index = 0;
		bool unsafe = ritem->IsUnsafe();
		ritem->Unsafe(false);
		auto unsafeGuard = reindexer::MakeScopeGuard([unsafe, ritem]() { ritem->Unsafe(unsafe); });

		auto checkAndConvert = [&]() {
			const auto& indexOpts = indexes()[index]->Opts();
			if (indexOpts.IsArray()) [[unlikely]] {
				throw Error(errLogic, "Precepts are not allowed for array fields ('{}')", sqlFunc.field);
			}
			IndexType indexType = indexes()[index]->Type();
			if (IsComposite(indexType)) [[unlikely]] {
				throw Error(errLogic, "Precepts are not allowed for composite indexes ('{}')", sqlFunc.field);
			}
			KeyValueType itp = indexes()[index]->KeyType();
			std::ignore = skrefs.back().convert(itp);
		};

		if (tryGetIndexByNameOrJsonPath(sqlFunc.field, index, EnableMultiJsonPath_True)) {
			checkAndConvert();
			const FieldsSet& fields = indexes()[index]->Fields();
			assertrx_throw(fields.size());
			if (index >= indexes().firstSparsePos()) {
				ritem->ModifyField(fields.getJsonPath(0), skrefs, FieldModeSet);
			} else {
				if (!ritem->IsIndexFieldSet(index)) {
					const auto& fieldType = payloadType().Field(*fields.begin());
					const auto& jps = fieldType.JsonPaths();
					assertrx_throw(jps.size());
					ritem->ModifyField(jps[0], skrefs, FieldModeSet);
				}
				ritem->SetField(index, std::move(skrefs));
			}
		} else {
			ritem->ModifyField(sqlFunc.field, skrefs, FieldModeSet);
		}
	}
}

int64_t NamespaceImpl::GetSerial(std::string_view field, UpdatesContainer& replUpdates, const NsContext& ctx) {
	int64_t counter = kStorageSerialInitial;

	std::string key(kSerialPrefix);
	key.append(field);
	auto ser = getMeta(key);
	if (ser != "") {
		counter = reindexer::stoll(ser) + 1;
	}

	std::string s = std::to_string(counter);
	putMeta(key, std::string_view(s), replUpdates, ctx);

	return counter;
}

void NamespaceImpl::FillResult(LocalQueryResults& result, const IdSetPlain& ids) const {
	for (auto id : ids) {
		result.AddItemRef(id, items_[id]);
	}
}

void NamespaceImpl::getFromJoinCache(ConstQueryImpl q, const JoinedQuery& jq, joins::CacheRes& out) const {
	if (config_.cacheMode == CacheModeOff || !indexOptimizer_.IsOptimizationCompleted() || q.HasVolatileExpressions() ||
		Impl(jq).HasVolatileExpressions()) {
		return;
	}
	out.key.SetData(Impl(jq), q);
	getFromJoinCacheImpl(out);
}

void NamespaceImpl::getFromJoinCache(ConstQueryImpl q, joins::CacheRes& out) const {
	if (config_.cacheMode == CacheModeOff || !indexOptimizer_.IsOptimizationCompleted() || q.HasVolatileExpressions()) {
		return;
	}
	out.key.SetData(q);
	getFromJoinCacheImpl(out);
}

void NamespaceImpl::getFromJoinCacheImpl(joins::CacheRes& ctx) const {
	auto it = joinCache_.Get(ctx.key);
	ctx.needPut = false;
	ctx.haveData = false;
	if (it.valid) {
		if (!it.val.IsInitialized()) {
			ctx.needPut = true;
		} else {
			ctx.haveData = true;
			ctx.it = std::move(it);
		}
	}
}

void NamespaceImpl::getInsideFromJoinCache(joins::CacheRes& ctx) const {
	if (config_.cacheMode != CacheModeAggressive || !indexOptimizer_.IsOptimizationCompleted() || ctx.key.buf_.empty()) {
		return;
	}
	getFromJoinCacheImpl(ctx);
}

void NamespaceImpl::putToJoinCache(joins::CacheRes& res, joins::PreSelect::CPtr preSelect) const {
	joins::CacheVal CacheVal;
	res.needPut = false;
	CacheVal.inited = true;
	CacheVal.preSelect = std::move(preSelect);
	joinCache_.Put(res.key, std::move(CacheVal));
}
void NamespaceImpl::putToJoinCache(joins::CacheRes& res, joins::CacheVal&& val) const {
	val.inited = true;
	joinCache_.Put(res.key, std::move(val));
}

const FieldsSet* NamespaceImpl::pkFields() const noexcept {
	if (int pkPos = 0; tryGetIndexByName(kPKIndexName, pkPos)) {
		return &indexes()[pkPos]->Fields();
	}
	return nullptr;
}

void NamespaceImpl::processWalRecord(WALRecord&& wrec, const NsContext& ctx, lsn_t itemLsn, Item* item) {
	lsn_t lsn;
	if (item && ctx.IsForceSyncItem()) {
		lsn = ctx.GetOriginLSN();
	} else {
		lsn = wal_.Add(wrec, ctx.GetOriginLSN(), itemLsn);
	}

	if (item) {
		assertrx(!lsn.isEmpty());
		// Cloning is required to avoid LSN modification in the QueryResult's/Snapshot's item
		if (!item->impl_->RealValue().IsFree()) {
			item->impl_->RealValue().Clone();
			item->impl_->RealValue().SetLSN(lsn);
		} else if (!item->impl_->Value().IsFree()) {
			item->impl_->Value().Clone();
			item->impl_->Value().SetLSN(lsn);
		}
	}
}

void NamespaceImpl::replicateAsync(updates::UpdateRecord&& rec, const RdxContext& ctx) {
	if (!isTemporary()) {
		auto err = observers_.SendAsyncUpdate(std::move(rec), ctx);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.whatStr());
		}
	}
}

void NamespaceImpl::replicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) {
	if (!isTemporary()) {
		auto err = observers_.SendAsyncUpdates(std::move(recs), ctx);
		if (!err.ok()) [[unlikely]] {
			throw Error(errUpdateReplication, err.whatStr());
		}
	}
}

bool NamespaceImpl::IsFulltextOrVector(std::string_view indexName, const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	for (const auto& idx : indexes()) {
		if (indexName == idx->Name()) {
			return idx->IsFloatVector() || idx->IsFulltext();
		}
	}
	return false;
}

std::shared_ptr<const reindexer::QueryEmbedder> NamespaceImpl::QueryEmbedder(std::string_view fieldName, const RdxContext& ctx) const {
	auto rlck = rLock(ctx);

	int idxNo = NotSet;
	if (!tryGetIndexByNameOrJsonPath(fieldName, idxNo)) [[unlikely]] {
		throw Error(errParams, "Can't find field by name or json path: '{}'", fieldName);
	}
	if (idxNo >= payloadType().NumFields()) [[unlikely]] {
		throw Error(errParams, "Can't use embedding with sparse/composite indexes ('{}')", fieldName);
	}
	const auto& type = payloadType().Field(idxNo);
	const auto& embedder = type.QueryEmbedder();
	if (embedder) {
		return embedder;
	}

	throw Error(errNotValid, "Trying to find knn by string. No Embedder configured for index '{}'", fieldName);
}

void NamespaceImpl::IndexesCacheCleaner::Add(Index& idx) noexcept {
	// Each ordered index may affect SortOrders of the other indexes
	requiresCleanup_ = requiresCleanup_ || idx.IsOrdered();
}

NamespaceImpl::IndexesCacheCleaner::~IndexesCacheCleaner() {
	if (requiresCleanup_) {
		for (auto& idx : ns_.indexes()) {
			if (idx->IsSupportSortedIdsBuild()) {
				idx->ClearCache();
			}
		}
	}
}

void NamespaceImpl::throwIndexUpsertErrorWithPKInfo(const ConstPayload& pl, const std::exception& err) {
	const auto errPtr = dynamic_cast<const Error*>(&err);
	const auto errCode = errPtr ? errPtr->code() : errParams;
	auto [pkIndex, pkField] = getPkIdx();
	if (pkIndex) {
		VariantArray keys = getPkKeys(pl, pkIndex, pkField);
		throw Error{errCode, fmt::format("Error during processing item with primary key `{}`={}: {}", pkIndex->Name(),
										 keys.Dump(payloadType(), pkIndex->Fields()), err.what())};
	} else {
		throw Error{errCode, fmt::format("Error during processing item with unknown primary key (PK index is missing): {}", err.what())};
	}
}

void NamespaceImpl::backgroundHNSWIndexesQuantization(RdxActivityContext* ctx) {
	std::vector<std::string> indexNames;

	RdxContext rdxCtx{ctx};
	{
		auto rlock = rLock(rdxCtx);

		auto vectorIndexes = getVectorIndexes();
		indexNames.reserve(vectorIndexes.size());
		for (auto [idx, ptr] : vectorIndexes) {
			indexNames.emplace_back(indexes()[idx]->Name());
		}
	}

	for (const auto& indexName : indexNames) {
		quantize(indexName, rdxCtx);
	}

	{
		auto rlock = rLock(rdxCtx);
		if (auto indexes = getVectorIndexes();
			storage_.WithProxy() && std::find_if(indexes.begin(), indexes.end(), [](const FloatVectorIndexData& indexData) {
										return indexData.ptr->IsQuantized();
									}) == indexes.end()) {
			storage_.WithProxy(false);
		}
	}
}

void NamespaceImpl::quantize(std::string_view indexName, const RdxContext& ctx) {
	auto rlock = rLock(ctx);

	auto getFloatVectorIndex = [this, indexName](int& idx) {
		return tryGetIndexByName(indexName, idx) ? dynamic_cast<FloatVectorIndex*>(indexes()[idx].get()) : nullptr;
	};

	int idx = -1;
	auto index = getFloatVectorIndex(idx);
	if (!index || !index->QuantizationAvailable()) {
		return;
	}

	if (!storageOpts_.IsEnabled()) {
		logFmt(LogWarning, "NamespaceImpl::backgroundHNSWIndexesQuantization ({}): Quantization available only with enabled storage.",
			   name_);
		return;
	}

	if (!storage_.WithProxy()) {
		storage_.WithProxy(true);
		storage_.Flush(StorageFlushOpts{});
	}

	index->Quantize();
	logFmt(LogInfo, "[{}] Index '{}' was quantized in background", name_, index->Name());

	rlock.unlock();
	auto wlock = simpleWLock(ctx);
	// This is necessary in order to avoid accessing invalid memory, if this index is deleted between lock captures.
	index = getFloatVectorIndex(idx);
	if (!index) {
		throw Error(errLogic,
					"NamespaceImpl::backgroundHNSWIndexesQuantization ({}): Quantization was interrupted due to index '{}' deletion", name_,
					indexName);
	}
	index->SwitchMapOnQuantized();
}

void NamespaceImpl::reloadNonQuantizedIndex(int idx) {
	assertrx_throw(indexes()[idx]->IsFloatVector());
	auto& index = static_cast<FloatVectorIndex&>(*indexes()[idx]);

	if (!index.IsQuantized()) {
		return;
	}

	WrSerializer wrSer;
	auto err = index.WriteIndexCache(wrSer, [](IdType id) { return VariantArray{Variant{id.ToNumber()}}; }, false, 0).err;
	if (!err.ok()) {
		throw err;
	}

	auto newIndexPtr = Index::New(getIndexDefinition(idx), PayloadType{payloadType()}, FieldsSet{index.Fields()}, config_.cacheConfig,
								  itemsCount(), LogCreation_True);
	auto& newIndex = static_cast<FloatVectorIndex&>(*newIndexPtr);
	newIndex.CopyEmptyValues(index);

	const FieldsSet* pk = pkFields();
	assertrx_throw(pk);

	const auto vecSizeBytes = sizeof(float) * index.Opts().FloatVector().Dimension();
	auto vectorsData = FloatVectorExtractor(storage_, index, *pk, payloadType(), tagsMatcher()).LoadVectorDataFromStorage(items_);
	err =
		newIndex.LoadIndexCache(wrSer.Slice(), false, FloatVectorIndexRawDataInserter{vectorsData, vecSizeBytes}, LoadWithQuantizer_False);

	if (!err.ok()) {
		throw err;
	}

	std::ignore = indexRegistry_.ReplaceIndex(idx, std::move(newIndexPtr));
}

}  // namespace reindexer
