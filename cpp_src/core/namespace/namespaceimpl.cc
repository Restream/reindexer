#include "core/namespace/namespaceimpl.h"

#include <algorithm>
#include <ctime>
#include <memory>
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/uuid_recoders.h"
#include "core/index/index.h"
#include "core/index/ttlindex.h"
#include "core/itemimpl.h"
#include "core/itemmodifier.h"
#include "core/nsselecter/crashqueryreporter.h"
#include "core/nsselecter/nsselecter.h"
#include "core/payload/payloadiface.h"
#include "core/querystat.h"
#include "core/rdxcontext.h"
#include "core/selectfunc/functionexecutor.h"
#include "itemsloader.h"
#include "namespace.h"
#include "snapshot/snapshothandler.h"
#include "threadtaskqueueimpl.h"
#include "tools/errors.h"
#include "tools/flagguard.h"
#include "tools/fsops.h"
#include "tools/hardware_concurrency.h"
#include "tools/logger.h"
#include "tools/timetools.h"
#include "wal/walselecter.h"

#ifdef REINDEX_WITH_V3_FOLLOWERS
#include "replv3/updatesobserver.h"
#endif	// REINDEX_WITH_V3_FOLLOWERS

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::microseconds;
using reindexer::cluster::UpdateRecord;

#define kStorageIndexesPrefix "indexes"
#define kStorageSchemaPrefix "schema"
#define kStorageReplStatePrefix "repl"
#define kStorageTagsPrefix "tags"
#define kStorageMetaPrefix "meta"
#define kStorageCachePrefix "cache"
#define kTupleName "-tuple"

static const std::string kPKIndexName = "#pk";
constexpr int kWALStatementItemsThreshold = 5;

#define kStorageMagic 0x1234FEDC
#define kStorageVersion 0x8

namespace reindexer {

std::atomic<bool> rxAllowNamespaceLeak = {false};

constexpr int64_t kStorageSerialInitial = 1;
constexpr uint8_t kSysRecordsBackupCount = 8;
constexpr uint8_t kSysRecordsFirstWriteCopies = 3;
constexpr size_t kMaxMemorySizeOfStringsHolder = 1ull << 24;

NamespaceImpl::IndexesStorage::IndexesStorage(const NamespaceImpl& ns) : ns_(ns) {}

void NamespaceImpl::IndexesStorage::MoveBase(IndexesStorage&& src) { Base::operator=(std::move(src)); }

// private implementation and NOT THREADSAFE of copy CTOR
NamespaceImpl::NamespaceImpl(const NamespaceImpl& src, AsyncStorage::FullLockT& storageLock)
	: intrusive_atomic_rc_base(),
	  indexes_{*this},
	  indexesNames_{src.indexesNames_},
	  indexesToComposites_{src.indexesToComposites_},
	  items_{src.items_},
	  free_{src.free_},
	  name_{src.name_},
	  payloadType_{src.payloadType_},
	  tagsMatcher_{src.tagsMatcher_},
	  storage_{src.storage_, storageLock},
	  replStateUpdates_{src.replStateUpdates_.load()},
	  meta_{src.meta_},
	  sparseIndexesCount_{src.sparseIndexesCount_},
	  krefs(src.krefs),
	  skrefs(src.skrefs),
	  sysRecordsVersions_{src.sysRecordsVersions_},
	  locker_(src.clusterizator_, *this),
	  schema_(src.schema_),
	  enablePerfCounters_{src.enablePerfCounters_.load()},
	  config_{src.config_},
	  queryCountCache_{
		  std::make_unique<QueryCountCache>(config_.cacheConfig.queryCountCacheSize, config_.cacheConfig.queryCountHitsToCache)},
	  joinCache_{std::make_unique<JoinCache>(config_.cacheConfig.joinCacheSize, config_.cacheConfig.joinHitsToCache)},
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
	  optimizationState_{NotOptimized},
	  strHolder_{makeStringsHolder()},
	  nsUpdateSortedContextMemory_{0},
	  clusterizator_{src.clusterizator_},
	  dbDestroyed_(false),
	  incarnationTag_(src.incarnationTag_)
#ifdef REINDEX_WITH_V3_FOLLOWERS
	  ,
	  observers_(src.observers_)
#endif	// REINDEX_WITH_V3_FOLLOWERS
{
	for (auto& idxIt : src.indexes_) indexes_.push_back(idxIt->Clone());

	markUpdated(true);
	logPrintf(LogInfo, "Namespace::CopyContentsFrom (%s).Workers: %d, timeout: %d, tm: { state_token: 0x%08X, version: %d }", name_,
			  config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher_.stateToken(), tagsMatcher_.version());
}

static int64_t GetCurrentTimeUS() noexcept {
	using std::chrono::duration_cast;
	using std::chrono::microseconds;
	using std::chrono::system_clock;
	return duration_cast<microseconds>(system_clock::now().time_since_epoch()).count();
}

NamespaceImpl::NamespaceImpl(const std::string& name, std::optional<int32_t> stateToken, cluster::INsDataReplicator& clusterizator
#ifdef REINDEX_WITH_V3_FOLLOWERS
							 ,
							 UpdatesObservers& observers
#endif	// REINDEX_WITH_V3_FOLLOWERS
							 )
	: intrusive_atomic_rc_base(),
	  indexes_(*this),
	  name_(name),
	  payloadType_(name),
	  tagsMatcher_(payloadType_, stateToken.has_value() ? stateToken.value() : tools::RandomGenerator::gets32()),
	  locker_(clusterizator, *this),
	  enablePerfCounters_(false),
	  queryCountCache_(
		  std::make_unique<QueryCountCache>(config_.cacheConfig.queryCountCacheSize, config_.cacheConfig.queryCountHitsToCache)),
	  joinCache_(std::make_unique<JoinCache>(config_.cacheConfig.joinCacheSize, config_.cacheConfig.joinHitsToCache)),
	  wal_(getWalSize(config_)),
	  lastSelectTime_{0},
	  cancelCommitCnt_{0},
	  lastUpdateTime_{0},
	  nsIsLoading_(false),
	  strHolder_{makeStringsHolder()},
	  clusterizator_(clusterizator),
	  dbDestroyed_(false),
	  incarnationTag_(GetCurrentTimeUS() % lsn_t::kDefaultCounter, 0)
#ifdef REINDEX_WITH_V3_FOLLOWERS
	  ,
	  observers_(observers)
#endif	// REINDEX_WITH_V3_FOLLOWERS
{
	logPrintf(LogTrace, "NamespaceImpl::NamespaceImpl (%s)", name_);
	FlagGuardT nsLoadingGuard(nsIsLoading_);
	items_.reserve(10000);
	itemsCapacity_.store(items_.capacity());
	optimizationState_.store(NotOptimized);

	// Add index and payload field for tuple of non indexed fields
	IndexDef tupleIndexDef(kTupleName, {}, IndexStrStore, IndexOpts());
	addIndex(tupleIndexDef, false);
	updateSelectTime();

	logPrintf(LogInfo, "Namespace::Construct (%s).Workers: %d, timeout: %d, tm: { state_token: 0x%08X (%s), version: %d }", name_,
			  config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher_.stateToken(),
			  stateToken.has_value() ? "preset" : "rand", tagsMatcher_.version());
}

NamespaceImpl::~NamespaceImpl() {
	const unsigned int kMaxItemCountNoThread = 1'000'000;
	const bool allowLeak = rxAllowNamespaceLeak.load(std::memory_order::memory_order_relaxed);
	unsigned int threadsCount = 0;

	ThreadTaskQueueImpl tasks;
	if (!allowLeak && items_.size() > kMaxItemCountNoThread) {
		static constexpr double kDeleteRxDestroy = 0.5;
		static constexpr double kDeleteNs = 0.25;

		const double k = dbDestroyed_.load(std::memory_order_relaxed) ? kDeleteRxDestroy : kDeleteNs;
		threadsCount = k * hardware_concurrency();
		if (threadsCount > indexes_.size() + 1) {
			threadsCount = indexes_.size() + 1;
		}
	}
	const bool multithreadingMode = (threadsCount > 1);

	logPrintf(LogTrace, "NamespaceImpl::~NamespaceImpl name=%s allowLeak=%d threadCount=%d", name_, allowLeak, threadsCount);

	auto flushStorage = [this]() {
		try {
			if (locker_.IsValid()) {
				saveReplStateToStorage(false);
				storage_.Flush(StorageFlushOpts().WithImmediateReopen());
			}
		} catch (Error& e) {
			logPrintf(LogWarning, "Namespace::~Namespace (%s), flushStorage() error: %s", name_, e.what());
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

#endif
	if (multithreadingMode) {
		tasks.AddTask(flushStorage);
#ifndef NDEBUG
		tasks.AddTask(checkStrHoldersWaitingToBeDeleted);
#endif
	} else {
		flushStorage();
#ifndef NDEBUG
		checkStrHoldersWaitingToBeDeleted();
#endif
	}

	if (allowLeak) {
		logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items. Leak mode", name_, items_.size());
		for (auto& indx : indexes_) {
			indx.release();	 // NOLINT(bugprone-unused-return-value)
		}
		return;
	}

	if (multithreadingMode) {
		logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items. Multithread mode. Deletion threads: %d", name_, items_.size(),
				  threadsCount);
		for (size_t i = 0; i < indexes_.size(); i++) {
			if (indexes_[i]->IsDestroyPartSupported()) {
				indexes_[i]->AddDestroyTask(tasks);
			} else {
				tasks.AddTask([i, this]() { indexes_[i].reset(); });
			}
		}
		std::vector<std::thread> threadPool;
		threadPool.reserve(threadsCount);
		for (size_t i = 0; i < threadsCount; ++i) {
			threadPool.emplace_back(std::thread([&tasks]() {
				while (auto task = tasks.GetTask()) {
					task();
				}
			}));
		}
		for (size_t i = 0; i < threadsCount; i++) {
			threadPool[i].join();
		}
		logPrintf(LogTrace, "Namespace::~Namespace (%s)", name_);
	} else {
		logPrintf(LogTrace, "Namespace::~Namespace (%s), %d items. Simple mode", name_, items_.size());
	}
}

void NamespaceImpl::SetNsVersion(lsn_t version, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	repl_.nsVersion = version;
	saveReplStateToStorage();
}

void NamespaceImpl::OnConfigUpdated(DBConfigProvider& configProvider, const RdxContext& ctx) {
	NamespaceConfigData configData;
	configProvider.GetNamespaceConfig(GetName(ctx), configData);
	const int serverId = configProvider.GetReplicationConfig().serverID;

	enablePerfCounters_ = configProvider.PerfStatsEnabled();

	// ! Updating storage under write lock
	auto wlck = simpleWLock(ctx);

	const bool needReoptimizeIndexes = (config_.optimizationSortWorkers == 0) != (configData.optimizationSortWorkers == 0);
	if (config_.optimizationSortWorkers != configData.optimizationSortWorkers ||
		config_.optimizationTimeout != configData.optimizationTimeout) {
		logPrintf(LogInfo, "[%s] Setting new index optimization config. Workers: %d->%d, timeout: %d->%d", name_,
				  config_.optimizationSortWorkers, configData.optimizationSortWorkers, config_.optimizationTimeout,
				  configData.optimizationTimeout);
	}
	const bool needReconfigureIdxCache = !config_.cacheConfig.IsIndexesCacheEqual(configData.cacheConfig);
	const bool needReconfigureJoinCache = !config_.cacheConfig.IsJoinCacheEqual(configData.cacheConfig);
	const bool needReconfigureQueryCountCache = !config_.cacheConfig.IsQueryCountCacheEqual(configData.cacheConfig);
	config_ = configData;
	storageOpts_.LazyLoad(configData.lazyLoad);
	storageOpts_.noQueryIdleThresholdSec = configData.noQueryIdleThreshold;
	storage_.SetForceFlushLimit(config_.syncStorageFlushLimit);

	for (auto& idx : indexes_) {
		idx->EnableUpdatesCountingMode(configData.idxUpdatesCountingMode);
	}
	if (needReconfigureIdxCache) {
		for (auto& idx : indexes_) {
			idx->ReconfigureCache(config_.cacheConfig);
		}
		logPrintf(LogTrace,
				  "[%s] Indexes cache has been reconfigured. IdSets cache (for each index): { max_size %lu KB; hits: %u }. FullTextIdSets "
				  "cache (for each ft-index): { max_size %lu KB; hits: %u }",
				  name_, config_.cacheConfig.idxIdsetCacheSize / 1024, config_.cacheConfig.idxIdsetHitsToCache,
				  config_.cacheConfig.ftIdxCacheSize / 1024, config_.cacheConfig.ftIdxHitsToCache);
	}
	if (needReconfigureJoinCache) {
		joinCache_ = std::make_unique<JoinCache>(config_.cacheConfig.joinCacheSize, config_.cacheConfig.joinHitsToCache);
		logPrintf(LogTrace, "[%s] Join cache has been reconfigured: { max_size %lu KB; hits: %u }", name_,
				  config_.cacheConfig.joinCacheSize / 1024, config_.cacheConfig.joinHitsToCache);
	}
	if (needReconfigureQueryCountCache) {
		queryCountCache_ =
			std::make_unique<QueryCountCache>(config_.cacheConfig.queryCountCacheSize, config_.cacheConfig.queryCountHitsToCache);
		logPrintf(LogTrace, "[%s] Queries count cache has been reconfigured: { max_size %lu KB; hits: %u }", name_,
				  config_.cacheConfig.queryCountCacheSize / 1024, config_.cacheConfig.queryCountHitsToCache);
	}

	if (needReoptimizeIndexes) {
		updateSortedIdxCount();
	}

	if (wal_.Resize(getWalSize(config_))) {
		logPrintf(LogInfo, "[%s] WAL has been resized lsn #%s, max size %ld", name_, repl_.lastLsn, wal_.Capacity());
	}

	if (isSystem()) {
		try {
			repl_.nsVersion.SetServer(serverId);
		} catch (const Error& err) {
			logPrintf(LogError, "[repl:%s]:%d Failed to change serverId to %d: %s.", name_, repl_.nsVersion.Server(), serverId, err.what());
		}
		return;
	}

	if (wal_.GetServer() != serverId) {
		if (itemsCount_ != 0) {
			repl_.clusterStatus.role = ClusterizationStatus::Role::None;  // TODO: Maybe we have to add separate role for this case
			logPrintf(LogWarning, "Changing serverId on NON EMPTY ns [%s]. Cluster role will be reset to None", name_);
		}
		logPrintf(LogWarning, "[repl:%s]:%d Changing serverId to %d. Tm_statetoken: %08X", name_, wal_.GetServer(), serverId,
				  tagsMatcher_.stateToken());
		const auto oldServerId = wal_.GetServer();
		try {
			wal_.SetServer(serverId);
			incarnationTag_.SetServer(serverId);
		} catch (const Error& err) {
			logPrintf(LogError, "[repl:%s]:%d Failed to change serverId to %d: %s. Resetting to 0(zero).", name_, wal_.GetServer(),
					  serverId, err.what());
			wal_.SetServer(oldServerId);
			incarnationTag_.SetServer(oldServerId);
		}
		replStateUpdates_.fetch_add(1, std::memory_order_release);
	}
}

template <NeedRollBack needRollBack>
class NamespaceImpl::RollBack_recreateCompositeIndexes : private RollBackBase {
public:
	RollBack_recreateCompositeIndexes(NamespaceImpl& ns, size_t startIdx, size_t count) : ns_{ns}, startIdx_{startIdx} {
		indexes_.reserve(count);
		std::swap(ns_.indexesToComposites_, indexesToComposites_);
	}
	~RollBack_recreateCompositeIndexes() {
		RollBack();
		for (auto& idx : indexes_) {
			try {
				if (idx->HoldsStrings()) {
					ns_.strHolder_->Add(std::move(idx));
				}
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
	}
	void RollBack() noexcept {
		if (IsDisabled()) return;
		for (size_t i = 0, s = indexes_.size(); i < s; ++i) {
			std::swap(ns_.indexes_[i + startIdx_], indexes_[i]);
		}
		std::swap(ns_.indexesToComposites_, indexesToComposites_);
		Disable();
	}
	void SaveIndex(std::unique_ptr<Index>&& idx) { indexes_.emplace_back(std::move(idx)); }
	using RollBackBase::Disable;

	// NOLINTNEXTLINE (performance-noexcept-move-constructor)
	RollBack_recreateCompositeIndexes(RollBack_recreateCompositeIndexes&&) = default;
	RollBack_recreateCompositeIndexes(const RollBack_recreateCompositeIndexes&) = delete;
	RollBack_recreateCompositeIndexes& operator=(const RollBack_recreateCompositeIndexes&) = delete;
	RollBack_recreateCompositeIndexes& operator=(RollBack_recreateCompositeIndexes&&) = delete;

private:
	NamespaceImpl& ns_;
	std::vector<std::unique_ptr<Index>> indexes_;
	fast_hash_map<int, std::vector<int>> indexesToComposites_;
	size_t startIdx_;
};

template <>
class NamespaceImpl::RollBack_recreateCompositeIndexes<NeedRollBack::No> {
public:
	RollBack_recreateCompositeIndexes(NamespaceImpl&, size_t, size_t) noexcept {}
	RollBack_recreateCompositeIndexes(RollBack_recreateCompositeIndexes&&) noexcept = default;
	~RollBack_recreateCompositeIndexes() = default;
	void RollBack() noexcept {}
	void Disable() noexcept {}
	void SaveIndex(std::unique_ptr<Index>&&) {}

	RollBack_recreateCompositeIndexes(const RollBack_recreateCompositeIndexes&) = delete;
	RollBack_recreateCompositeIndexes& operator=(const RollBack_recreateCompositeIndexes&) = delete;
	RollBack_recreateCompositeIndexes& operator=(RollBack_recreateCompositeIndexes&&) = delete;
};

template <NeedRollBack needRollBack>
NamespaceImpl::RollBack_recreateCompositeIndexes<needRollBack> NamespaceImpl::recreateCompositeIndexes(size_t startIdx, size_t endIdx) {
	FieldsSet fields;
	RollBack_recreateCompositeIndexes<needRollBack> rollbacker{*this, startIdx, endIdx - startIdx};
	for (size_t i = startIdx; i < endIdx; ++i) {
		std::unique_ptr<Index>& index(indexes_[i]);
		if (IsComposite(index->Type())) {
			IndexDef indexDef;
			indexDef.name_ = index->Name();
			indexDef.opts_ = index->Opts();
			indexDef.FromType(index->Type());

			createCompositeFieldsSet<FieldsSet, h_vector<std::string, 1>>(indexDef.name_, index->Fields(), fields);
			auto newIndex{Index::New(indexDef, PayloadType{payloadType_}, FieldsSet{fields}, config_.cacheConfig)};
			rollbacker.SaveIndex(std::move(index));
			std::swap(index, newIndex);

			for (auto field : fields) {
				indexesToComposites_[field].emplace_back(i);
			}
		}
	}
	return rollbacker;
}

template <NeedRollBack needRollBack>
class NamespaceImpl::RollBack_updateItems : private RollBackBase {
public:
	RollBack_updateItems(NamespaceImpl& ns, RollBack_recreateCompositeIndexes<needRollBack>&& rb, uint64_t dh, size_t ds) noexcept
		: ns_{ns}, rollbacker_recreateCompositeIndexes_{std::move(rb)}, dataHash_{dh}, itemsDataSize_{ds} {
		items_.reserve(ns_.items_.size());
	}
	~RollBack_updateItems() { RollBack(); }
	void Disable() noexcept {
		rollbacker_recreateCompositeIndexes_.Disable();
		RollBackBase::Disable();
	}
	void RollBack() noexcept {
		if (IsDisabled()) return;
		if (!items_.empty()) {
			ns_.repl_.dataHash = dataHash_;
			ns_.itemsDataSize_ = itemsDataSize_;
		}
		if (tuple_) {
			std::swap(ns_.indexes_[0], tuple_);
		}
		for (auto& [rowId, pv] : items_) {
			ns_.items_[rowId] = std::move(pv);
		}
		rollbacker_recreateCompositeIndexes_.RollBack();
		for (auto& idx : ns_.indexes_) {
			idx->UpdatePayloadType(PayloadType{ns_.payloadType_});
		}
		Disable();
	}
	void SaveItem(size_t rowId, PayloadValue&& pv) { items_.emplace_back(rowId, std::move(pv)); }
	void SaveTuple() { tuple_ = ns_.indexes_[0]->Clone(); }

	// NOLINTNEXTLINE (performance-noexcept-move-constructor)
	RollBack_updateItems(RollBack_updateItems&&) = default;
	RollBack_updateItems(const RollBack_updateItems&) = delete;
	RollBack_updateItems& operator=(const RollBack_updateItems&) = delete;
	RollBack_updateItems& operator=(RollBack_updateItems&&) = delete;

private:
	NamespaceImpl& ns_;
	RollBack_recreateCompositeIndexes<needRollBack> rollbacker_recreateCompositeIndexes_;
	std::vector<std::pair<size_t, PayloadValue>> items_;
	uint64_t dataHash_;
	size_t itemsDataSize_;
	std::unique_ptr<Index> tuple_;
};

template <>
class NamespaceImpl::RollBack_updateItems<NeedRollBack::No> {
public:
	RollBack_updateItems(NamespaceImpl&, RollBack_recreateCompositeIndexes<NeedRollBack::No>&&, uint64_t, size_t) noexcept {}
	~RollBack_updateItems() = default;
	void Disable() noexcept {}
	void RollBack() noexcept {}
	void SaveItem(size_t, const PayloadValue&) noexcept {}
	void SaveTuple() noexcept {}

	RollBack_updateItems(RollBack_updateItems&&) noexcept = default;
	RollBack_updateItems(const RollBack_updateItems&) = delete;
	RollBack_updateItems& operator=(const RollBack_updateItems&) = delete;
	RollBack_updateItems& operator=(RollBack_updateItems&&) = delete;
};

template <NeedRollBack needRollBack>
NamespaceImpl::RollBack_updateItems<needRollBack> NamespaceImpl::updateItems(const PayloadType& oldPlType, const FieldsSet& changedFields,
																			 int deltaFields) {
	logPrintf(LogTrace, "Namespace::updateItems(%s) delta=%d", name_, deltaFields);

	assertrx(oldPlType->NumFields() + deltaFields == payloadType_->NumFields());

	const int compositeStartIdx =
		(deltaFields >= 0) ? indexes_.firstCompositePos() : indexes_.firstCompositePos(oldPlType, sparseIndexesCount_);
	const int compositeEndIdx = indexes_.totalSize();
	// All the composite indexes must be recreated, beacuse those indexes are holding pointers to the old Payloads
	RollBack_updateItems<needRollBack> rollbacker{*this, recreateCompositeIndexes<needRollBack>(compositeStartIdx, compositeEndIdx),
												  repl_.dataHash, itemsDataSize_};

	for (auto& idx : indexes_) {
		idx->UpdatePayloadType(PayloadType{payloadType_});
	}

	VariantArray skrefsDel, skrefsUps;
	ItemImpl newItem(payloadType_, tagsMatcher_);
	newItem.Unsafe(true);
	int errCount = 0;
	Error lastErr = errOK;
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	std::unique_ptr<Recoder> recoder;
	if (deltaFields < 0) {
		assertrx(deltaFields == -1);
		for (auto fieldIdx : changedFields) {
			const auto& fld = oldPlType.Field(fieldIdx);
			if (fieldIdx != 0 && fld.Type().Is<KeyValueType::Uuid>()) {
				const auto& jsonPaths = fld.JsonPaths();
				assertrx(jsonPaths.size() == 1);
				if (fld.IsArray()) {
					recoder.reset(new RecoderUuidToString<true>{tagsMatcher_.path2tag(jsonPaths[0])});
				} else {
					recoder.reset(new RecoderUuidToString<false>{tagsMatcher_.path2tag(jsonPaths[0])});
				}
			}
		}
	} else if (deltaFields > 0) {
		assertrx(deltaFields == 1);
		for (auto fieldIdx : changedFields) {
			const auto& fld = payloadType_.Field(fieldIdx);
			if (fieldIdx != 0 && fld.Type().Is<KeyValueType::Uuid>()) {
				if (fld.IsArray()) {
					recoder.reset(new RecoderStringToUuidArray{fieldIdx});
				} else {
					recoder.reset(new RecoderStringToUuid{fieldIdx});
				}
			}
		}
	}
	if (!items_.empty()) {
		rollbacker.SaveTuple();
	}
	for (size_t rowId = 0; rowId < items_.size(); rowId++) {
		if (items_[rowId].IsFree()) {
			continue;
		}
		PayloadValue& plCurr = items_[rowId];
		Payload oldValue(oldPlType, plCurr);
		ItemImpl oldItem(oldPlType, plCurr, tagsMatcher_);
		oldItem.Unsafe(true);
		newItem.FromCJSON(&oldItem, recoder.get());

		PayloadValue plNew = oldValue.CopyTo(payloadType_, deltaFields >= 0);
		plNew.SetLSN(plCurr.GetLSN());
		Payload newValue(payloadType_, plNew);

		for (auto fieldIdx : changedFields) {
			auto& index = *indexes_[fieldIdx];
			if ((fieldIdx == 0) || deltaFields <= 0) {
				oldValue.Get(fieldIdx, skrefsDel, Variant::hold_t{});
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

		rollbacker.SaveItem(rowId, std::move(plCurr));
		plCurr = std::move(plNew);
		repl_.dataHash ^= Payload(payloadType_, plCurr).GetHash();
		itemsDataSize_ += plCurr.GetCapacity() + sizeof(PayloadValue::dataHeader);
	}
	markUpdated(false);
	if (errCount != 0) {
		logPrintf(LogError, "Can't update indexes of %d items in namespace %s: %s", errCount, name_, lastErr.what());
	}
	return rollbacker;
}

void NamespaceImpl::addToWAL(const IndexDef& indexDef, WALRecType type, const NsContext& ctx) {
	WrSerializer ser;
	indexDef.GetJSON(ser);
	processWalRecord(WALRecord(type, ser.Slice()), ctx);
}

void NamespaceImpl::addToWAL(std::string_view json, WALRecType type, const NsContext& ctx) { processWalRecord(WALRecord(type, json), ctx); }

void NamespaceImpl::AddIndex(const IndexDef& indexDef, const RdxContext& ctx) {
	verifyAddIndex(indexDef, [this, &ctx]() { return GetName(ctx); });

	UpdatesContainer pendedRepl;

	auto wlck = dataWLock(ctx, true);

	bool checkIdxEqualityNow = ctx.GetOriginLSN().isEmpty();
	// Check index existance before cluster role check, to allow followers "add" their indexes locally
	// FT indexes may have different config, it will be ignored during comparison
	bool requireTtlUpdate = false;
	if (checkIdxEqualityNow && checkIfSameIndexExists(indexDef, &requireTtlUpdate)) {
		if (requireTtlUpdate && repl_.clusterStatus.role == ClusterizationStatus::Role::None) {
			checkIdxEqualityNow = false;
		} else {
			if (ctx.HasEmmiterServer()) {
				// Make sure, that index was already replicated to emmiter
				pendedRepl.emplace_back(UpdateRecord::Type::EmptyUpdate, name_, ctx.EmmiterServerId());
				replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
			}
			return;
		}
	}

	checkClusterStatus(ctx);

	doAddIndex(indexDef, checkIdxEqualityNow, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::DumpIndex(std::ostream& os, std::string_view index, const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	dumpIndex(os, index);
}

void NamespaceImpl::UpdateIndex(const IndexDef& indexDef, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);

	if (doUpdateIndex(indexDef, pendedRepl, ctx)) {
		saveIndexesToStorage();
		replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
	}
}

void NamespaceImpl::DropIndex(const IndexDef& indexDef, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);
	doDropIndex(indexDef, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::SetSchema(std::string_view schema, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;

	auto wlck = dataWLock(ctx, true);

	if (ctx.GetOriginLSN().isEmpty()) {
		if (schema_ && schema_->GetJSON() == Schema::AppendProtobufNumber(schema, schema_->GetProtobufNsNumber())) {
			if (repl_.clusterStatus.role != ClusterizationStatus::Role::None) {
				logPrintf(LogWarning,
						  "[repl:%s]:%d Attempt to set new JSON-schema for the replicated namespace via user interface, which does not "
						  "correspond to the current schema. New schema was ignored to avoid force syncs",
						  name_, wal_.GetServer());
				return;
			}
			if (ctx.HasEmmiterServer()) {
				// Make sure, that schema was already replicated to emmiter
				pendedRepl.emplace_back(UpdateRecord::Type::EmptyUpdate, name_, ctx.EmmiterServerId());
				replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
			}
			return;
		}
	}
	checkClusterStatus(ctx);
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
			if (!err.ok()) throw err;
		} else {
			throw Error(errParams, "Unknown schema type: %d", format);
		}
	}
	return std::string(ser.Slice());
}

void NamespaceImpl::dumpIndex(std::ostream& os, std::string_view index) const {
	auto itIdxName = indexesNames_.find(index);
	if (itIdxName == indexesNames_.end()) {
		constexpr char errMsg[] = "Cannot dump index %s: doesn't exist";
		logPrintf(LogError, errMsg, index);
		throw Error(errParams, errMsg, index);
	}
	indexes_[itIdxName->second]->Dump(os);
}

void NamespaceImpl::dropIndex(const IndexDef& index, bool disableTmVersionInc) {
	auto itIdxName = indexesNames_.find(index.name_);
	if (itIdxName == indexesNames_.end()) {
		const char* errMsg = "Cannot remove index %s: doesn't exist";
		logPrintf(LogError, errMsg, index.name_);
		throw Error(errParams, errMsg, index.name_);
	}

	int fieldIdx = itIdxName->second;
	std::unique_ptr<Index>& indexToRemove = indexes_[fieldIdx];
	if (!IsComposite(indexToRemove->Type()) && indexToRemove->Opts().IsSparse()) --sparseIndexesCount_;

	// Check, that index to remove is not a part of composite index
	for (int i = indexes_.firstCompositePos(); i < indexes_.totalSize(); ++i) {
		if (indexes_[i]->Fields().contains(fieldIdx))
			throw Error(errLogic, "Cannot remove index %s : it's a part of a composite index %s", index.name_, indexes_[i]->Name());
	}
	for (auto& namePair : indexesNames_) {
		if (namePair.second >= fieldIdx) {
			namePair.second--;
		}
	}

	if (indexToRemove->Opts().IsPK()) {
		indexesNames_.erase(kPKIndexName);
	}

	// Update indexes fields refs
	for (const std::unique_ptr<Index>& idx : indexes_) {
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

	const bool isComposite = IsComposite(indexToRemove->Type());
	if (isComposite) {
		for (auto& v : indexesToComposites_) {
			const auto f = std::find(v.second.begin(), v.second.end(), fieldIdx);
			if (f != v.second.end()) {
				v.second.erase(f);
			}
		}
	} else if (!indexToRemove->Opts().IsSparse()) {
		PayloadType oldPlType = payloadType_;
		payloadType_.Drop(index.name_);
		tagsMatcher_.UpdatePayloadType(payloadType_, disableTmVersionInc ? NeedChangeTmVersion::No : NeedChangeTmVersion::Increment);
		FieldsSet changedFields{0, fieldIdx};
		auto rollbacker{updateItems<NeedRollBack::No>(oldPlType, changedFields, -1)};
		rollbacker.Disable();
	}

	removeIndex(indexToRemove);
	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
	updateSortedIdxCount();
}

void NamespaceImpl::doDropIndex(const IndexDef& index, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	dropIndex(index, ctx.inSnapshot);

	addToWAL(index, WalIndexDrop, ctx);
	pendedRepl.emplace_back(UpdateRecord::Type::IndexDrop, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), index);
}

static void verifyConvertTypes(KeyValueType from, KeyValueType to, const PayloadType& payloadType, const FieldsSet& fields) {
	if (!from.IsSame(to) && (((from.Is<KeyValueType::String>() || from.Is<KeyValueType::Uuid>()) &&
							  !(to.Is<KeyValueType::String>() || to.Is<KeyValueType::Uuid>())) ||
							 ((to.Is<KeyValueType::String>() || to.Is<KeyValueType::Uuid>()) &&
							  !(from.Is<KeyValueType::String>() || from.Is<KeyValueType::Uuid>())))) {
		throw Error(errParams, "Cannot convert key from type %s to %s", from.Name(), to.Name());
	}
	static const std::string defaultStringValue;
	static const std::string nilUuidStringValue{Uuid{}};
	Variant value;
	from.EvaluateOneOf(
		[&](KeyValueType::Int64) noexcept { value = Variant(int64_t(0)); }, [&](KeyValueType::Double) noexcept { value = Variant(0.0); },
		[&](KeyValueType::String) { value = Variant{to.Is<KeyValueType::Uuid>() ? nilUuidStringValue : defaultStringValue}; },
		[&](KeyValueType::Bool) noexcept { value = Variant(false); }, [](KeyValueType::Null) noexcept {},
		[&](KeyValueType::Int) noexcept { value = Variant(0); }, [&](KeyValueType::Uuid) noexcept { value = Variant{Uuid{}}; },
		[&](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined>) {
			if (!to.IsSame(from)) throw Error(errParams, "Cannot convert key from type %s to %s", from.Name(), to.Name());
		});
	value.convert(to, &payloadType, &fields);
}

void NamespaceImpl::verifyCompositeIndex(const IndexDef& indexDef) const {
	const auto type = indexDef.Type();
	if (indexDef.opts_.IsSparse()) {
		throw Error{errParams, "Composite index cannot be sparse. Use non-sparse composite instead"};
	}
	for (const auto& jp : indexDef.jsonPaths_) {
		int idx;
		if (!tryGetIndexByName(jp, idx)) {
			if (!IsFullText(indexDef.Type())) {
				throw Error(errParams,
							"Composite indexes over non-indexed field ('%s') are not supported yet (except for full-text indexes). Create "
							"at least column index('-') over each field inside the composite index",
							jp);
			}
		} else {
			const auto& index = *indexes_[idx];
			if (index.Opts().IsSparse()) {
				throw Error(errParams, "Composite indexes over sparse indexed field ('%s') are not supported yet", jp);
			}
			if (type != IndexCompositeHash && index.IsUuid()) {
				throw Error{errParams, "Only hash index allowed on UUID field"};
			}
			if (index.Opts().IsArray() && !IsFullText(type)) {
				throw Error(errParams, "Cannot add array subindex '%s' to not fulltext composite index '%s'", jp, indexDef.name_);
			}
			if (IsComposite(index.Type())) {
				throw Error(errParams, "Cannot create composite index '%s' over the other composite '%s'", indexDef.name_, index.Name());
			}
		}
	}
}

template <typename GetNameF>
void NamespaceImpl::verifyAddIndex(const IndexDef& indexDef, GetNameF&& getNameF) const {
	const auto idxType = indexDef.Type();
	if (!validateIndexName(indexDef.name_, idxType)) {
		throw Error(errParams,
					"Cannot add index '%s' in namespace '%s'. Index name contains invalid characters. Only alphas, digits, '+' (for "
					"composite indexes only), '.', '_' and '-' are allowed",
					indexDef.name_, getNameF());
	}
	if (indexDef.opts_.IsPK()) {
		if (indexDef.opts_.IsArray()) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't be array", indexDef.name_, getNameF());
		} else if (indexDef.opts_.IsSparse()) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't be sparse", indexDef.name_, getNameF());
		} else if (isStore(idxType)) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't have '-' type", indexDef.name_, getNameF());
		} else if (IsFullText(idxType)) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. PK field can't be fulltext index", indexDef.name_, getNameF());
		}
	}
	if ((idxType == IndexUuidHash || idxType == IndexUuidStore) && indexDef.opts_.IsSparse()) {
		throw Error(errParams, "Cannot add index '%s' in namespace '%s'. UUID field can't be sparse", indexDef.name_, getNameF());
	}
	if (indexDef.jsonPaths_.size() > 1 && !IsComposite(idxType) && !indexDef.opts_.IsArray()) {
		throw Error(errParams,
					"Cannot add index '%s' in namespace '%s'. Scalar (non-array and non-composite) index can not have multiple JSON-paths. "
					"Use array index instead",
					indexDef.name_, getNameF());
	}
	if (indexDef.jsonPaths_.empty()) {
		throw Error(errParams, "Cannot add index '%s' in namespace '%s'. JSON paths array can not be empty", indexDef.name_, getNameF());
	}
	for (const auto& jp : indexDef.jsonPaths_) {
		if (jp.empty()) {
			throw Error(errParams, "Cannot add index '%s' in namespace '%s'. JSON path can not be empty", indexDef.name_, getNameF());
		}
	}
}

void NamespaceImpl::verifyUpdateIndex(const IndexDef& indexDef) const {
	const auto idxNameIt = indexesNames_.find(indexDef.name_);
	const auto currentPKIt = indexesNames_.find(kPKIndexName);

	if (idxNameIt == indexesNames_.end()) {
		throw Error(errParams, "Cannot update index %s: doesn't exist", indexDef.name_);
	}
	const auto& oldIndex = indexes_[idxNameIt->second];
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
	if (indexDef.jsonPaths_.size() > 1 && !IsComposite(indexDef.Type()) && !indexDef.opts_.IsArray()) {
		throw Error(
			errParams,
			"Cannot update index '%s' in namespace '%s'. Scalar (non-array and non-composite) index can not have multiple JSON-paths",
			indexDef.name_, name_);
	}
	if (indexDef.jsonPaths_.empty()) {
		throw Error(errParams, "Cannot update index '%s' in namespace '%s'. JSON paths array can not be empty", indexDef.name_, name_);
	}
	for (const auto& jp : indexDef.jsonPaths_) {
		if (jp.empty()) {
			throw Error(errParams, "Cannot update index '%s' in namespace '%s'. JSON path can not be empty", indexDef.name_, name_);
		}
	}

	if (IsComposite(indexDef.Type())) {
		verifyUpdateCompositeIndex(indexDef);
		return;
	}

	const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet(), config_.cacheConfig));
	if (indexDef.opts_.IsSparse()) {
		if (indexDef.jsonPaths_.size() != 1) {
			throw Error(errParams, "Sparse index must have exactly 1 JSON-path, but %d paths found for '%s'", indexDef.jsonPaths_.size(),
						indexDef.name_);
		}
		const auto newSparseIndex = std::unique_ptr<Index>(Index::New(indexDef, PayloadType{payloadType_}, {}, config_.cacheConfig));
	} else {
		FieldsSet changedFields{idxNameIt->second};
		PayloadType newPlType = payloadType_;
		newPlType.Drop(indexDef.name_);
		newPlType.Add(PayloadFieldType(newIndex->KeyType(), indexDef.name_, indexDef.jsonPaths_, indexDef.opts_.IsArray()));
		verifyConvertTypes(oldIndex->KeyType(), newIndex->KeyType(), newPlType, changedFields);
	}
}

class NamespaceImpl::RollBack_insertIndex : private RollBackBase {
	using IndexesNamesIt = decltype(NamespaceImpl::indexesNames_)::iterator;

public:
	RollBack_insertIndex(NamespaceImpl& ns, NamespaceImpl::IndexesStorage::iterator idxIt, int idxNo) noexcept
		: ns_{ns}, insertedIndex_{idxIt}, insertedIdxNo_{idxNo} {}
	RollBack_insertIndex(RollBack_insertIndex&&) noexcept = default;
	~RollBack_insertIndex() { RollBack(); }
	void RollBack() noexcept {
		if (IsDisabled()) return;
		if (insertedIdxName_) {
			try {
				ns_.indexesNames_.erase(*insertedIdxName_);
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
		if (pkIndexNameInserted_) {
			try {
				ns_.indexesNames_.erase(kPKIndexName);
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
		for (auto& n : ns_.indexesNames_) {
			if (n.second > insertedIdxNo_) {
				--n.second;
			}
		}
		try {
			ns_.indexes_.erase(insertedIndex_);
			// NOLINTBEGIN(bugprone-empty-catch)
		} catch (...) {
		}
		// NOLINTEND(bugprone-empty-catch)
		Disable();
	}
	void PkIndexNameInserted() noexcept { pkIndexNameInserted_ = true; }
	void InsertedIndexName(const IndexesNamesIt& it) noexcept { insertedIdxName_.emplace(it); }
	using RollBackBase::Disable;

	RollBack_insertIndex(const RollBack_insertIndex&) = delete;
	RollBack_insertIndex operator=(const RollBack_insertIndex&) = delete;
	RollBack_insertIndex operator=(RollBack_insertIndex&&) = delete;

private:
	NamespaceImpl& ns_;
	NamespaceImpl::IndexesStorage::iterator insertedIndex_;
	std::optional<IndexesNamesIt> insertedIdxName_;
	int insertedIdxNo_;
	bool pkIndexNameInserted_{false};
};

NamespaceImpl::RollBack_insertIndex NamespaceImpl::insertIndex(std::unique_ptr<Index> newIndex, int idxNo, const std::string& realName) {
	const bool isPK = newIndex->Opts().IsPK();
	const auto it = indexes_.insert(indexes_.begin() + idxNo, std::move(newIndex));
	RollBack_insertIndex rollbacker{*this, it, idxNo};

	for (auto& n : indexesNames_) {
		if (n.second >= idxNo) {
			++n.second;
		}
	}

	if (isPK) {
		if (const auto [it, ok] = indexesNames_.insert({kPKIndexName, idxNo}); ok) {
			(void)it;
			rollbacker.PkIndexNameInserted();
		}
	}
	if (const auto [it, ok] = indexesNames_.insert({realName, idxNo}); ok) {
		rollbacker.InsertedIndexName(it);
	}
	return rollbacker;
}

class NamespaceImpl::RollBack_addIndex : private RollBackBase {
public:
	RollBack_addIndex(NamespaceImpl& ns) : ns_{ns} {}
	RollBack_addIndex(const RollBack_addIndex&) = delete;
	~RollBack_addIndex() { RollBack(); }
	void RollBack() noexcept {
		if (IsDisabled()) return;
		if (oldPayloadType_) ns_.payloadType_ = std::move(*oldPayloadType_);
		if (needDecreaseSparseIndexCount_) --ns_.sparseIndexesCount_;
		if (rollbacker_updateItems_) rollbacker_updateItems_->RollBack();
		if (rollbacker_insertIndex_) rollbacker_insertIndex_->RollBack();
		if (needResetPayloadTypeInTagsMatcher_) {
			ns_.tagsMatcher_.UpdatePayloadType(ns_.payloadType_,
											   disableTmVersionInc_ ? NeedChangeTmVersion::No : NeedChangeTmVersion::Decrement);
		}
		Disable();
	}
	void RollBacker_insertIndex(RollBack_insertIndex&& rb) noexcept { rollbacker_insertIndex_.emplace(std::move(rb)); }
	void RollBacker_updateItems(RollBack_updateItems<NeedRollBack::Yes>&& rb) noexcept { rollbacker_updateItems_.emplace(std::move(rb)); }
	void Disable() noexcept {
		if (rollbacker_insertIndex_) rollbacker_insertIndex_->Disable();
		if (rollbacker_updateItems_) rollbacker_updateItems_->Disable();
		RollBackBase::Disable();
	}
	void NeedDecreaseSparseIndexCount() noexcept { needDecreaseSparseIndexCount_ = true; }
	void SetOldPayloadType(PayloadType&& oldPt) noexcept { oldPayloadType_.emplace(std::move(oldPt)); }
	const PayloadType& GetOldPayloadType() const noexcept {
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		return *oldPayloadType_;
	}
	void NeedResetPayloadTypeInTagsMatcher(bool disableTmVersionInc) noexcept {
		needResetPayloadTypeInTagsMatcher_ = true;
		disableTmVersionInc_ = disableTmVersionInc;
	}

private:
	std::optional<RollBack_insertIndex> rollbacker_insertIndex_;
	std::optional<RollBack_updateItems<NeedRollBack::Yes>> rollbacker_updateItems_;
	NamespaceImpl& ns_;
	std::optional<PayloadType> oldPayloadType_;
	bool needDecreaseSparseIndexCount_{false};
	bool needResetPayloadTypeInTagsMatcher_{false};
	bool disableTmVersionInc_{false};
};

void NamespaceImpl::addIndex(const IndexDef& indexDef, bool disableTmVersionInc, bool skipEqualityCheck) {
	if (bool requireTtlUpdate = false; !skipEqualityCheck && checkIfSameIndexExists(indexDef, &requireTtlUpdate)) {
		if (requireTtlUpdate) {
			auto idxNameIt = indexesNames_.find(indexDef.name_);
			assertrx(idxNameIt != indexesNames_.end());
			UpdateExpireAfter(indexes_[idxNameIt->second].get(), indexDef.expireAfter_);
		}
		return;
	}

	const auto currentPKIndex = indexesNames_.find(kPKIndexName);
	const auto& indexName = indexDef.name_;
	// New index case. Just add
	if (currentPKIndex != indexesNames_.end() && indexDef.opts_.IsPK()) {
		throw Error(errConflict, "Cannot add PK index '%s.%s'. Already exists another PK index - '%s'", name_, indexName,
					indexes_[currentPKIndex->second]->Name());
	}

	if (IsComposite(indexDef.Type())) {
		verifyCompositeIndex(indexDef);
		addCompositeIndex(indexDef);
		return;
	}

	const int idxNo = payloadType_->NumFields();
	if (idxNo >= kMaxIndexes) {
		throw Error(errConflict, "Cannot add index '%s.%s'. Too many non-composite indexes. %d non-composite indexes are allowed only",
					name_, indexName, kMaxIndexes - 1);
	}
	const JsonPaths& jsonPaths = indexDef.jsonPaths_;
	RollBack_addIndex rollbacker{*this};
	if (indexDef.opts_.IsSparse()) {
		if (jsonPaths.size() != 1) {
			throw Error(errParams, "Sparse index must have exactly 1 JSON-path, but %d paths found for '%s':'%s'", jsonPaths.size(), name_,
						indexDef.name_);
		}
		FieldsSet fields;
		fields.push_back(jsonPaths[0]);
		TagsPath tagsPath = tagsMatcher_.path2tag(jsonPaths[0], true);
		assertrx(tagsPath.size() > 0);
		fields.push_back(std::move(tagsPath));
		auto newIndex = Index::New(indexDef, PayloadType{payloadType_}, std::move(fields), config_.cacheConfig);
		rollbacker.RollBacker_insertIndex(insertIndex(std::move(newIndex), idxNo, indexName));
		++sparseIndexesCount_;
		rollbacker.NeedDecreaseSparseIndexCount();
		fillSparseIndex(*indexes_[idxNo], jsonPaths[0]);
	} else {
		PayloadType oldPlType = payloadType_;
		auto newIndex = Index::New(indexDef, PayloadType(), FieldsSet(), config_.cacheConfig);
		payloadType_.Add(PayloadFieldType{newIndex->KeyType(), indexName, jsonPaths, newIndex->Opts().IsArray()});
		rollbacker.SetOldPayloadType(std::move(oldPlType));
		tagsMatcher_.UpdatePayloadType(payloadType_, disableTmVersionInc ? NeedChangeTmVersion::No : NeedChangeTmVersion::Increment);
		rollbacker.NeedResetPayloadTypeInTagsMatcher(disableTmVersionInc);
		newIndex->SetFields(FieldsSet{idxNo});
		newIndex->UpdatePayloadType(PayloadType(payloadType_));

		FieldsSet changedFields{0, idxNo};
		rollbacker.RollBacker_insertIndex(insertIndex(std::move(newIndex), idxNo, indexName));
		rollbacker.RollBacker_updateItems(updateItems<NeedRollBack::Yes>(rollbacker.GetOldPayloadType(), changedFields, 1));
	}
	updateSortedIdxCount();
	rollbacker.Disable();
}

void NamespaceImpl::fillSparseIndex(Index& index, std::string_view jsonPath) {
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (size_t rowId = 0; rowId < items_.size(); rowId++) {
		if (items_[rowId].IsFree()) {
			continue;
		}
		Payload{payloadType_, items_[rowId]}.GetByJsonPath(jsonPath, tagsMatcher_, skrefs, index.KeyType());
		krefs.resize(0);
		bool needClearCache{false};
		index.Upsert(krefs, skrefs, rowId, needClearCache);
		if (needClearCache && index.IsOrdered()) indexesCacheCleaner.Add(index.SortId());
	}
	markUpdated(false);
}

void NamespaceImpl::doAddIndex(const IndexDef& indexDef, bool skipEqualityCheck, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	addIndex(indexDef, ctx.inSnapshot, skipEqualityCheck);

	addToWAL(indexDef, WalIndexAdd, ctx);
	pendedRepl.emplace_back(UpdateRecord::Type::IndexAdd, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(),
							indexDef);
}

bool NamespaceImpl::updateIndex(const IndexDef& indexDef, bool disableTmVersionInc) {
	const std::string& indexName = indexDef.name_;

	IndexDef foundIndex = getIndexDefinition(indexName);

	if (indexDef.IsEqual(foundIndex, IndexComparison::SkipConfig)) {
		// Index has not been changed
		if (!indexDef.IsEqual(foundIndex, IndexComparison::WithConfig)) {
			// Only index config changed
			// Just call SetOpts
			indexes_[getIndexByName(indexName)]->SetOpts(indexDef.opts_);
			return true;
		}
		return false;
	}

	verifyUpdateIndex(indexDef);
	dropIndex(indexDef, disableTmVersionInc);
	addIndex(indexDef, disableTmVersionInc);
	return true;
}

bool NamespaceImpl::doUpdateIndex(const IndexDef& indexDef, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (updateIndex(indexDef, ctx.inSnapshot) || !ctx.GetOriginLSN().isEmpty()) {
		addToWAL(indexDef, WalIndexUpdate, ctx.rdxContext);
		pendedRepl.emplace_back(UpdateRecord::Type::IndexUpdate, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(),
								indexDef);
		return true;
	}
	return false;
}

IndexDef NamespaceImpl::getIndexDefinition(const std::string& indexName) const {
	for (unsigned i = 0; i < indexes_.size(); ++i) {
		if (indexes_[i]->Name() == indexName) {
			return getIndexDefinition(i);
		}
	}
	throw Error(errParams, "Index '%s' not found in '%s'", indexName, name_);
}

void NamespaceImpl::verifyUpdateCompositeIndex(const IndexDef& indexDef) const {
	verifyCompositeIndex(indexDef);
	const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, PayloadType{payloadType_}, {}, config_.cacheConfig));
}

void NamespaceImpl::addCompositeIndex(const IndexDef& indexDef) {
	const auto& indexName = indexDef.name_;

	FieldsSet fields;
	createCompositeFieldsSet<JsonPaths, JsonPaths>(indexName, indexDef.jsonPaths_, fields);
	assertrx(indexesNames_.find(indexName) == indexesNames_.end());

	const int idxPos = indexes_.size();
	auto insertIndex_rollbacker{
		insertIndex(Index::New(indexDef, PayloadType{payloadType_}, FieldsSet{fields}, config_.cacheConfig), idxPos, indexName)};

	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (IdType rowId = 0; rowId < int(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			bool needClearCache{false};
			indexes_[idxPos]->Upsert(Variant(items_[rowId]), rowId, needClearCache);
			if (needClearCache && indexes_[idxPos]->IsOrdered()) indexesCacheCleaner.Add(indexes_[idxPos]->SortId());
		}
	}

	for (auto field : fields) {
		indexesToComposites_[field].emplace_back(idxPos);
	}

	updateSortedIdxCount();
	insertIndex_rollbacker.Disable();
}

template <typename PathsT, typename JsonPathsContainerT>
void NamespaceImpl::createCompositeFieldsSet(const std::string& idxName, const PathsT& paths, FieldsSet& fields) {
	fields.clear();

	const JsonPathsContainerT* jsonPaths = nullptr;
	if constexpr (std::is_same_v<PathsT, FieldsSet>) {
		jsonPaths = &paths.getJsonPaths();
		for (int field : paths) {
			// Moving index fields
			fields.push_back(field);
		}
	} else {
		jsonPaths = &paths;
	}

	for (const auto& jsonPathOrSubIdx : *jsonPaths) {
		int idx;
		if (!getScalarIndexByName(jsonPathOrSubIdx, idx) /* || idxName == jsonPathOrSubIdx*/) {	 // TODO may be uncomment
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPathOrSubIdx, true);
			if (tagsPath.empty()) {
				throw Error(errLogic, "Unable to get or create json-path '%s' for composite index '%s'", jsonPathOrSubIdx, idxName);
			}
			fields.push_back(tagsPath);
			fields.push_back(jsonPathOrSubIdx);
		} else {
			const auto& idxFields = indexes_[idx]->Fields();
			assertrx_throw(idxFields.size() == 1);
			assertrx_throw(idxFields[0] >= 0);
			fields.push_back(idxFields[0]);
		}
	}

	assertrx(fields.getJsonPathsLength() == fields.getTagsPathsLength());
}

bool NamespaceImpl::checkIfSameIndexExists(const IndexDef& indexDef, bool* requireTtlUpdate) {
	auto idxNameIt = indexesNames_.find(indexDef.name_);
	if (idxNameIt != indexesNames_.end()) {
		IndexDef oldIndexDef = getIndexDefinition(indexDef.name_);
		if (oldIndexDef.Type() == IndexTtl && indexDef.Type() == IndexTtl) {
			if (requireTtlUpdate && oldIndexDef.expireAfter_ != indexDef.expireAfter_) {
				*requireTtlUpdate = true;
			}
			oldIndexDef.expireAfter_ = indexDef.expireAfter_;
		}
		if (indexDef.IsEqual(oldIndexDef, IndexComparison::SkipConfig)) {
			return true;
		}
		throw Error(errConflict, "Index '%s.%s' already exists with different settings", name_, indexDef.name_);
	}
	return false;
}

int NamespaceImpl::getIndexByName(std::string_view index) const {
	const auto idxIt = indexesNames_.find(index);
	if (idxIt == indexesNames_.end()) {
		throw Error(errParams, "Index '%s' not found in '%s'", index, name_);
	}
	return idxIt->second;
}

int NamespaceImpl::getIndexByNameOrJsonPath(std::string_view index) const {
	int idx;
	if (tryGetIndexByName(index, idx)) {
		return idx;
	}
	idx = payloadType_.FieldByJsonPath(index);
	if (idx > 0) {
		return idx;
	} else {
		throw Error(errParams, "Index '%s' not found in '%s'", index, name_);
	}
}

int NamespaceImpl::getScalarIndexByName(std::string_view index) const {
	int idx;
	if (tryGetIndexByName(index, idx)) {
		if (idx < indexes_.firstCompositePos()) {
			return idx;
		}
	}
	throw Error(errParams, "Index '%s' not found in '%s'", index, name_);
}

bool NamespaceImpl::tryGetIndexByName(std::string_view name, int& index) const {
	auto it = indexesNames_.find(name);
	if (it == indexesNames_.end()) return false;
	index = it->second;
	return true;
}

bool NamespaceImpl::getIndexByNameOrJsonPath(std::string_view name, int& index) const {
	if (tryGetIndexByName(name, index)) {
		return true;
	}
	const auto idx = payloadType_.FieldByJsonPath(name);
	if (idx > 0) {
		index = idx;
		return true;
	}
	return false;
}

bool NamespaceImpl::getScalarIndexByName(std::string_view name, int& index) const {
	int idx;
	if (tryGetIndexByName(name, idx)) {
		if (idx < indexes_.firstCompositePos()) {
			index = idx;
			return true;
		}
	}
	return false;
}

bool NamespaceImpl::getSparseIndexByJsonPath(std::string_view jsonPath, int& index) const {
	// FIXME: Try to merge getIndexByNameOrJsonPath and getSparseIndexByJsonPath if it's possible
	for (int i = indexes_.firstSparsePos(), end = indexes_.firstSparsePos() + indexes_.sparseIndexesSize(); i < end; ++i) {
		if (indexes_[i]->Fields().contains(jsonPath)) {
			index = i;
			return true;
		}
	}
	return false;
}

void NamespaceImpl::Insert(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeInsert, ctx); }

void NamespaceImpl::Update(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeUpdate, ctx); }

void NamespaceImpl::Upsert(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeUpsert, ctx); }

void NamespaceImpl::Delete(Item& item, const RdxContext& ctx) { ModifyItem(item, ModeDelete, ctx); }

void NamespaceImpl::doDelete(IdType id) {
	assertrx(items_.exists(id));

	Payload pl(payloadType_, items_[id]);

	WrSerializer pk;
	pk << kRxStorageItemPrefix;
	pl.SerializeFields(pk, pkFields());

	repl_.dataHash ^= pl.GetHash();
	wal_.Set(WALRecord(), items_[id].GetLSN(), false);

	storage_.Remove(pk.Slice());

	// erase last item
	int field;

	// erase from composite indexes
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		bool needClearCache{false};
		indexes_[field]->Delete(Variant(items_[id]), id, *strHolder_, needClearCache);
		if (needClearCache && indexes_[field]->IsOrdered()) indexesCacheCleaner.Add(indexes_[field]->SortId());
	}

	// Holder for tuple. It is required for sparse indexes will be valid
	VariantArray tupleHolder;
	pl.Get(0, tupleHolder);

	// Deleteing fields from dense and sparse indexes:
	// we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire
	// payload value (and not only 0 item).
	assertrx(indexes_.firstCompositePos() != 0);
	const int borderIdx = indexes_.totalSize() > 1 ? 1 : 0;
	field = borderIdx;
	do {
		field %= indexes_.firstCompositePos();

		Index& index = *indexes_[field];
		if (index.Opts().IsSparse()) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			pl.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
		} else if (index.Opts().IsArray()) {
			pl.Get(field, skrefs, Variant::hold_t{});
		} else {
			pl.Get(field, skrefs);
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

void NamespaceImpl::removeIndex(std::unique_ptr<Index>& idx) {
	if (idx->HoldsStrings() && !(strHoldersWaitingToBeDeleted_.empty() && strHolder_.unique())) {
		strHolder_->Add(std::move(idx));
	}
}

void NamespaceImpl::doTruncate(UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (storage_.IsValid()) {
		for (PayloadValue& pv : items_) {
			if (pv.IsFree()) continue;
			Payload pl(payloadType_, pv);
			WrSerializer pk;
			pk << kRxStorageItemPrefix;
			pl.SerializeFields(pk, pkFields());
			storage_.Remove(pk.Slice());
		}
	}
	items_.clear();
	free_.clear();
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	for (size_t i = 0; i < indexes_.size(); ++i) {
		const IndexOpts opts = indexes_[i]->Opts();
		std::unique_ptr<Index> newIdx{Index::New(getIndexDefinition(i), PayloadType{indexes_[i]->GetPayloadType()},
												 FieldsSet{indexes_[i]->Fields()}, config_.cacheConfig)};
		newIdx->SetOpts(opts);
		std::swap(indexes_[i], newIdx);
		removeIndex(newIdx);
	}

	WrSerializer ser;
	WALRecord wrec(WalUpdateQuery, (ser << "TRUNCATE " << name_).Slice());

	const auto lsn = wal_.Add(wrec, ctx.GetOriginLSN());
	markUpdated(true);

#ifdef REINDEX_WITH_V3_FOLLOWERS
	if (!repl_.temporary && !ctx.inSnapshot) {
		observers_.OnWALUpdate(LSNPair(lsn, lsn), name_, wrec);
	}
#endif	// REINDEX_WITH_V3_FOLLOWERS

	pendedRepl.emplace_back(UpdateRecord::Type::Truncate, name_, lsn, repl_.nsVersion, ctx.rdxContext.EmmiterServerId());
}

void NamespaceImpl::ModifyItem(Item& item, ItemModifyMode mode, const RdxContext& ctx) {
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
	UpdatesContainer pendedRepl;

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();
	calc.LockHit();
	if (mode == ModeDelete && rx_unlikely(item.PkFields() != pkFields())) {
		throw Error(errNotValid, "Item has outdated PK metadata (probably PK has been change during the Delete-call)");
	}
	modifyItem(item, mode, pendedRepl, NsContext(ctx));

	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, ctx);
}

void NamespaceImpl::Truncate(const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();

	calc.LockHit();

	doTruncate(pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, ctx);
}

void NamespaceImpl::Refill(std::vector<Item>& items, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx);
	cg.Reset();

	assertrx(isSystem());  // TODO: Refill currently will not be replicated, so it's available for system ns only

	doTruncate(pendedRepl, ctx);
	NsContext nsCtx(ctx);
	for (Item& i : items) {
		doModifyItem(i, ModeUpsert, pendedRepl, nsCtx);
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
	state.dataHash = repl_.dataHash;
	state.dataCount = ItemsCount();
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

LocalTransaction NamespaceImpl::NewTransaction(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return LocalTransaction(name_, payloadType_, tagsMatcher_, pkFields(), schema_, ctx.GetOriginLSN());
}

void NamespaceImpl::CommitTransaction(LocalTransaction& tx, LocalQueryResults& result, const NsContext& ctx,
									  QueryStatCalculator<LocalTransaction, long_actions::Logger>& queryStatCalculator) {
	Locker::WLockT wlck;
	if (!ctx.isCopiedNsRequest) {
		PerfStatCalculatorMT calc(updatePerfCounter_, enablePerfCounters_);
		CounterGuardAIR32 cg(cancelCommitCnt_);
		wlck = queryStatCalculator.CreateLock(*this, &NamespaceImpl::dataWLock, ctx.rdxContext, true);
		cg.Reset();
		calc.LockHit();
		tx.ValidatePK(pkFields());
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
#ifdef REINDEX_WITH_V3_FOLLOWERS
			if (!repl_.temporary) {
				observers_.OnWALUpdate(LSNPair(lsn, lsn), name_, initWrec);
			}
#endif	// REINDEX_WITH_V3_FOLLOWERS
		}
	}

	AsyncStorage::AdviceGuardT storageAdvice;
	if (tx.GetSteps().size() >= AsyncStorage::kLimitToAdviceBatching) {
		storageAdvice = storage_.AdviceBatching();
	}

	for (auto&& step : tx.GetSteps()) {
		UpdatesContainer pendedRepl;
		switch (step.type_) {
			case TransactionStep::Type::ModifyItem: {
				const auto mode = std::get<TransactionItemStep>(step.data_).mode;
				const auto lsn = step.lsn_;
				Item item = tx.GetItem(std::move(step));
				modifyItem(item, mode, pendedRepl, NsContext(ctx).InTransaction(lsn));
				result.AddItem(item);
				break;
			}
			case TransactionStep::Type::Query: {
				LocalQueryResults qr;
				qr.AddNamespace(this, true);
				auto& data = std::get<TransactionQueryStep>(step.data_);
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
				auto& data = std::get<TransactionMetaStep>(step.data_);
				putMeta(data.key, data.value, pendedRepl, NsContext(ctx).InTransaction(step.lsn_));
				break;
			}
			case TransactionStep::Type::SetTM: {
				auto& data = std::get<TransactionTmStep>(step.data_);
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
		replicate(std::move(pendedRepl), std::move(wlck), true, queryStatCalculator, ctx);
		return;
	} else if (ctx.inSnapshot && ctx.isRequireResync) {
		replicateAsync(
			{ctx.isInitialLeaderSync ? UpdateRecord::Type::ResyncNamespaceLeaderInit : UpdateRecord::Type::ResyncNamespaceGeneric, name_,
			 lsn_t(0, 0), lsn_t(0, 0), ctx.rdxContext.EmmiterServerId()},
			ctx.rdxContext);
	}
	if (!ctx.isCopiedNsRequest) {
		queryStatCalculator.LogFlushDuration(*this, &NamespaceImpl::tryForceFlush, std::move(wlck));
	}
}

void NamespaceImpl::doUpsert(ItemImpl* ritem, IdType id, bool doUpdate) {
	// Upsert fields to indexes
	assertrx(items_.exists(id));
	auto& plData = items_[id];

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
			const auto& fields = indexes_[field + indexes_.firstCompositePos()]->Fields();
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
				const auto& tp = fields.getTagsPath(i);
				pl.GetByJsonPath(tp, skrefs, KeyValueType::Undefined{});
				plNew.GetByJsonPath(tp, krefs, KeyValueType::Undefined{});
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
		Index& index = *indexes_[field];
		bool isIndexSparse = index.Opts().IsSparse();
		if (isIndexSparse) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			try {
				plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
			} catch (const Error&) {
				skrefs.resize(0);
			}
		} else {
			plNew.Get(field, skrefs);
		}

		if (index.Opts().GetCollateMode() == CollateUTF8) {
			for (auto& key : skrefs) key.EnsureUTF8();
		}

		// Check for update
		if (doUpdate) {
			if (isIndexSparse) {
				try {
					pl.GetByJsonPath(index.Fields().getTagsPath(0), krefs, index.KeyType());
				} catch (const Error&) {
					krefs.resize(0);
				}
			} else if (index.Opts().IsArray()) {
				pl.Get(field, krefs, Variant::hold_t{});
			} else {
				pl.Get(field, krefs);
			}
			if ((krefs.ArrayType().Is<KeyValueType::Null>() && skrefs.ArrayType().Is<KeyValueType::Null>()) || krefs == skrefs) continue;
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

void NamespaceImpl::updateTagsMatcherFromItem(ItemImpl* ritem, const NsContext& ctx) {
	if (ritem->tagsMatcher().isUpdated()) {
		logPrintf(LogTrace, "Updated TagsMatcher of namespace '%s' on modify:\n%s", name_, ritem->tagsMatcher().dump());
		if (!ctx.GetOriginLSN().isEmpty()) {
			throw Error(errLogic, "%s: Replicated item requires explicit tagsmatcher update: %s", name_, ritem->GetJSON());
		}
	}
	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		std::string jsonSliceBuf(ritem->GetJSON());
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

void NamespaceImpl::modifyItem(Item& item, ItemModifyMode mode, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (mode == ModeDelete) {
		deleteItem(item, pendedRepl, ctx);
	} else {
		doModifyItem(item, mode, pendedRepl, ctx);
	}
}

void NamespaceImpl::deleteItem(Item& item, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	ItemImpl* ritem = item.impl_;
	const auto oldTmV = tagsMatcher_.version();
	updateTagsMatcherFromItem(ritem, ctx);

	auto itItem = findByPK(ritem, ctx.inTransaction, ctx.rdxContext);
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

void NamespaceImpl::doModifyItem(Item& item, ItemModifyMode mode, UpdatesContainer& pendedRepl, const NsContext& ctx, IdType suggestedId) {
	// Item to doUpsert
	assertrx(mode != ModeDelete);
	const auto oldTmV = tagsMatcher_.version();
	ItemImpl* itemImpl = item.impl_;
	setFieldsBasedOnPrecepts(itemImpl, pendedRepl, ctx);
	updateTagsMatcherFromItem(itemImpl, ctx);
	auto newPl = itemImpl->GetPayload();

	auto realItem = findByPK(itemImpl, ctx.inTransaction, ctx.rdxContext);
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

	saveTagsMatcherToStorage(true);
	if (storage_.IsValid()) {
		WrSerializer pk, data;
		pk << kRxStorageItemPrefix;
		newPl.SerializeFields(pk, pkFields());
		data.PutUInt64(int64_t(lsn));
		data.Write(cjson.Slice());
		storage_.Write(pk.Slice(), data.Slice());
	}

	markUpdated(!exists);

#ifdef REINDEX_WITH_V3_FOLLOWERS
	if (!repl_.temporary && !ctx.inSnapshot) {
		observers_.OnWALUpdate(LSNPair(lsn, lsn), name_,
							   WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), mode, ctx.inTransaction));
	}
#endif	// REINDEX_WITH_V3_FOLLOWERS

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

PayloadType NamespaceImpl::getPayloadType(const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	return payloadType_;
}

RX_ALWAYS_INLINE VariantArray NamespaceImpl::getPkKeys(const ConstPayload& cpl, Index* pkIndex, int fieldNum) {
	// It is a faster alternative of "select ID from namespace where pk1 = 'item.pk1' and pk2 = 'item.pk2' "
	// Get pkey values from pk fields
	VariantArray keys;
	if (IsComposite(pkIndex->Type())) {
		keys.emplace_back(*cpl.Value());
	} else if (pkIndex->Opts().IsSparse()) {
		cpl.GetByJsonPath(pkIndex->Fields().getTagsPath(0), keys, pkIndex->KeyType());
	} else
		cpl.Get(fieldNum, keys);
	return keys;
}

RX_ALWAYS_INLINE SelectKeyResult NamespaceImpl::getPkDocs(const ConstPayload& cpl, bool inTransaction, const RdxContext& ctx) {
	auto pkIndexIt = indexesNames_.find(kPKIndexName);
	if (pkIndexIt == indexesNames_.end()) {
		throw Error(errLogic, "Trying to modify namespace '%s', but it doesn't contain PK index", name_);
	}
	Index* pkIndex = indexes_[pkIndexIt->second].get();
	VariantArray keys = getPkKeys(cpl, pkIndex, pkIndexIt->second);
	assertf(keys.size() == 1, "Pkey field must contain 1 key, but there '%d' in '%s.%s'", keys.size(), name_, pkIndex->Name());
	Index::SelectOpts selectOpts;
	selectOpts.inTransaction = inTransaction;
	return pkIndex->SelectKey(keys, CondEq, 0, selectOpts, nullptr, ctx)[0];
}

// find id by PK. NOT THREAD SAFE!
std::pair<IdType, bool> NamespaceImpl::findByPK(ItemImpl* ritem, bool inTransaction, const RdxContext& ctx) {
	SelectKeyResult res = getPkDocs(ritem->GetConstPayload(), inTransaction, ctx);
	if (res.size() && res[0].ids_.size()) return {res[0].ids_[0], true};
	return {-1, false};
}

void NamespaceImpl::checkUniquePK(const ConstPayload& cpl, bool inTransaction, const RdxContext& ctx) {
	SelectKeyResult res = getPkDocs(cpl, inTransaction, ctx);
	if (res.size() && res[0].ids_.size() > 1) {
		auto pkIndexIt = indexesNames_.find(kPKIndexName);
		Index* pkIndex = indexes_[pkIndexIt->second].get();
		VariantArray keys = getPkKeys(cpl, pkIndex, pkIndexIt->second);

		WrSerializer wrser;
		wrser << "Duplicate Primary Key {" << pkIndex->Name() << ": ";
		keys.Dump(wrser, CheckIsStringPrintable::No);
		wrser << "} for rows [";
		for (size_t i = 0; i < res[0].ids_.size(); i++) {
			if (i != 0) {
				wrser << ", ";
			}
			wrser << res[0].ids_[i];
		}
		wrser << "]!";
		throw Error(errLogic, wrser.Slice());
	}
}

void NamespaceImpl::optimizeIndexes(const NsContext& ctx) {
	static const auto kHardwareConcurrency = hardware_concurrency();
	// This is read lock only atomics based implementation of rebuild indexes
	// If optimizationState_ == OptimizationCompleted is true, then indexes are completely built.
	// In this case reset optimizationState_ and/or any idset's and sort orders builds are allowed only protected by write lock
	if (optimizationState_.load(std::memory_order_relaxed) == OptimizationCompleted) return;
	auto lastUpdateTime = lastUpdateTime_.load(std::memory_order_acquire);

	Locker::RLockT rlck;
	if (!ctx.isCopiedNsRequest) {
		rlck = rLock(ctx.rdxContext);
	}

	if (isSystem() || repl_.temporary || !indexes_.size() || !lastUpdateTime || !config_.optimizationTimeout) {
		return;
	}
	const auto optState{optimizationState_.load(std::memory_order_acquire)};
	if (optState == OptimizationCompleted || cancelCommitCnt_.load(std::memory_order_relaxed)) {
		return;
	}

	using namespace std::chrono;
	const int64_t now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
	if (!ctx.isCopiedNsRequest && now - lastUpdateTime < config_.optimizationTimeout) {
		return;
	}

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
	const size_t maxIndexWorkers = std::min<size_t>(kHardwareConcurrency, config_.optimizationSortWorkers);
	for (auto& idxIt : indexes_) {
		if (idxIt->IsOrdered() && maxIndexWorkers != 0) {
			NSUpdateSortedContext sortCtx(*this, currentSortId++);
			const bool forceBuildAll = forceBuildAllIndexes || idxIt->IsBuilt() || idxIt->SortId() != currentSortId;
			idxIt->MakeSortOrders(sortCtx);
			// Build in multiple threads
			std::unique_ptr<std::thread[]> thrs(new std::thread[maxIndexWorkers]);
			for (size_t i = 0; i < maxIndexWorkers; i++) {
				thrs[i] = std::thread([&, i]() {
					for (size_t j = i; j < this->indexes_.size() && !cancelCommitCnt_.load(std::memory_order_relaxed) &&
									   !dbDestroyed_.load(std::memory_order_relaxed);
						 j += maxIndexWorkers) {
						auto& idx = this->indexes_[j];
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

	if (dbDestroyed_.load(std::memory_order_relaxed)) return;

	if (maxIndexWorkers && !cancelCommitCnt_.load(std::memory_order_relaxed)) {
		optimizationState_.store(OptimizationCompleted, std::memory_order_release);
		for (auto& idxIt : indexes_) {
			if (!idxIt->IsFulltext()) {
				idxIt->MarkBuilt();
			}
		}
	}
	if (cancelCommitCnt_.load(std::memory_order_relaxed)) {
		logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) done", name_);
	} else {
		logPrintf(LogTrace, "Namespace::optimizeIndexes(%s) was cancelled by concurent update", name_);
	}
}

void NamespaceImpl::markUpdated(bool forceOptimizeAllIndexes) {
	using namespace std::string_view_literals;
	using namespace std::chrono;
	itemsCount_.store(items_.size(), std::memory_order_relaxed);
	itemsCapacity_.store(items_.capacity(), std::memory_order_relaxed);
	if (forceOptimizeAllIndexes) {
		optimizationState_.store(NotOptimized);
	} else {
		int expected{OptimizationCompleted};
		optimizationState_.compare_exchange_strong(expected, OptimizedPartially);
	}
	queryCountCache_->Clear();
	joinCache_->Clear();
	lastUpdateTime_.store(duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count(), std::memory_order_release);
	if (!nsIsLoading_) {
		repl_.updatedUnixNano = getTimeNow("nsec"sv);
	}
}

Item NamespaceImpl::newItem() {
	auto impl_ = pool_.get(0, payloadType_, tagsMatcher_, pkFields(), schema_);
	impl_->tagsMatcher() = tagsMatcher_;
	impl_->tagsMatcher().clearUpdated();
	impl_->schema() = schema_;
#ifdef RX_WITH_STDLIB_DEBUG
	assertrx_dbg(impl_->PkFields() == pkFields());
#endif	// RX_WITH_STDLIB_DEBUG
	return Item(impl_.release());
}

void NamespaceImpl::doUpdate(const Query& query, LocalQueryResults& result, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	NsSelecter selecter(this);
	SelectCtx selCtx(query, nullptr);
	SelectFunctionsHolder func;
	selCtx.functions = &func;
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.inTransaction;
	selecter(result, selCtx, ctx.rdxContext);

	ActiveQueryScope queryScope(query, QueryUpdate, optimizationState_, strHolder_.get());
	const auto tmStart = high_resolution_clock::now();

	bool updateWithJson = false;
	bool withExpressions = false;
	for (const UpdateEntry& ue : query.UpdateFields()) {
		if (!withExpressions && ue.IsExpression()) withExpressions = true;
		if (!updateWithJson && ue.Mode() == FieldModeSetJson) updateWithJson = true;
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
	for (ItemRef& item : result.Items()) {
		assertrx(items_.exists(item.Id()));
		const auto oldTmV = tagsMatcher_.version();
		PayloadValue& pv(items_[item.Id()]);
		Payload pl(payloadType_, pv);
		uint64_t oldPlHash = pl.GetHash();
		size_t oldItemCapacity = pv.GetCapacity();
		const bool isPKModified = itemModifier.Modify(item.Id(), ctx, pendedRepl);
		std::optional<PKModifyRevertData> modifyData;
		if (isPKModified) {
			// statementReplication = false;
			modifyData.emplace(itemModifier.GetPayloadValueBackup(), lsn_t(item.Value().GetLSN()));
		}

		replicateItem(item.Id(), ctx, statementReplication, oldPlHash, oldItemCapacity, oldTmV, std::move(modifyData), pendedRepl);
		item.Value() = items_[item.Id()];
	}
	result.getTagsMatcher(0) = tagsMatcher_;
	assertrx(result.IsNamespaceAdded(this));

	// Disabled due to statement base replication logic conflicts
	// lsn_t lsn;
	//	if (statementReplication) {
	//		WrSerializer ser;
	//		const_cast<Query &>(query).type_ = QueryUpdate;
	//		WALRecord wrec(WalUpdateQuery, query.GetSQL(ser, QueryUpdate).Slice(), ctx.inTransaction);
	//		lsn = wal_.Add(wrec, ctx.GetOriginLSN());
	//		if (!ctx.rdxContext.fromReplication_) repl_.lastSelfLSN = lsn;
	//		for (ItemRef &item : result.Items()) {
	//			item.Value().SetLSN(lsn);
	//		}
	//		if (!repl_.temporary)
	//			observers_->OnWALUpdate(LSNPair(lsn, ctx.rdxContext.fromReplication_ ? ctx.rdxContext.LSNs_.originLSN_ : lsn), name_, wrec);
	//		if (!ctx.rdxContext.fromReplication_) setReplLSNs(LSNPair(lsn_t(), lsn));
	//	}

	if (query.GetDebugLevel() >= LogInfo) {
		logPrintf(LogInfo, "Updated %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
	//	if (statementReplication) {
	//		assertrx(!lsn.isEmpty());
	//		pendedRepl.emplace_back(UpdateRecord::Type::UpdateQuery, name_, lsn, query);
	//	}
}

void NamespaceImpl::replicateItem(IdType itemId, const NsContext& ctx, bool statementReplication, uint64_t oldPlHash,
								  size_t oldItemCapacity, int oldTmVersion, std::optional<PKModifyRevertData>&& modifyData,
								  UpdatesContainer& pendedRepl) {
	PayloadValue& pv(items_[itemId]);
	Payload pl(payloadType_, pv);

	if (!statementReplication) {
		replicateTmUpdateIfRequired(pendedRepl, oldTmVersion, ctx);
		auto sendWalUpdate = [this, itemId, &ctx, &pv, &pendedRepl](UpdateRecord::Type mode) {
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
#ifdef REINDEX_WITH_V3_FOLLOWERS
			if (!repl_.temporary && !ctx.inSnapshot) {
				observers_.OnWALUpdate(LSNPair(lsn, lsn), name_,
									   WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeUpdate, ctx.inTransaction));
			}
#endif	// REINDEX_WITH_V3_FOLLOWERS
			pendedRepl.emplace_back(mode, name_, lsn, repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
		};

		if (modifyData.has_value()) {
			ItemImpl itemSave(payloadType_, modifyData->pv, tagsMatcher_);
			WrSerializer cjson;
			itemSave.GetCJSON(cjson, false);
			processWalRecord(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete, ctx.inTransaction), ctx.rdxContext,
							 modifyData->lsn);
			pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemDeleteTx : UpdateRecord::Type::ItemDelete, name_,
									wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
			sendWalUpdate(ctx.inTransaction ? UpdateRecord::Type::ItemInsertTx : UpdateRecord::Type::ItemInsert);
		} else {
			sendWalUpdate(ctx.inTransaction ? UpdateRecord::Type::ItemUpdateTx : UpdateRecord::Type::ItemUpdate);
		}
	}

	repl_.dataHash ^= oldPlHash;
	repl_.dataHash ^= pl.GetHash();
	itemsDataSize_ -= oldItemCapacity;
	itemsDataSize_ += pl.Value()->GetCapacity();

	saveTagsMatcherToStorage(true);
	if (storage_.IsValid()) {
		WrSerializer pk, data;
		if (modifyData.has_value()) {
			Payload plSave(payloadType_, modifyData->pv);
			pk << kRxStorageItemPrefix;
			plSave.SerializeFields(pk, pkFields());
			storage_.Remove(pk.Slice());
			pk.Reset();
		}
		pk << kRxStorageItemPrefix;
		pl.SerializeFields(pk, pkFields());
		data.PutUInt64(uint64_t(pv.GetLSN()));
		ItemImpl item(payloadType_, pv, tagsMatcher_);
		item.GetCJSON(data);
		storage_.Write(pk.Slice(), data.Slice());
	}
}

void NamespaceImpl::doDelete(const Query& q, LocalQueryResults& result, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	NsSelecter selecter(this);
	SelectCtx selCtx(q, nullptr);
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.inTransaction;
	SelectFunctionsHolder func;
	selCtx.functions = &func;
	selecter(result, selCtx, ctx.rdxContext);
	assertrx(result.IsNamespaceAdded(this));

	ActiveQueryScope queryScope(q, QueryDelete, optimizationState_, strHolder_.get());
	const auto tmStart = high_resolution_clock::now();
	const auto oldTmV = tagsMatcher_.version();
	for (auto& r : result.Items()) {
		doDelete(r.Id());
	}

	if (ctx.IsWalSyncItem() || (!q.HasLimit() && !q.HasOffset() && result.Count() >= kWALStatementItemsThreshold)) {
		WrSerializer ser;
		const_cast<Query&>(q).type_ = QueryDelete;
		processWalRecord(WALRecord(WalUpdateQuery, q.GetSQL(ser, QueryDelete).Slice(), ctx.inTransaction), ctx);
		pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::DeleteQueryTx : UpdateRecord::Type::DeleteQuery, name_,
								wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::string(ser.Slice()));
	} else {
		replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
		for (auto it : result) {
			WrSerializer cjson;
			it.GetCJSON(cjson, false);
			processWalRecord(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete, ctx.inTransaction), ctx, lsn_t());
			pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::ItemDeleteTx : UpdateRecord::Type::ItemDelete, name_,
									wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(cjson));
		}
	}
	if (q.GetDebugLevel() >= LogInfo) {
		logPrintf(LogInfo, "Deleted %d items in %d µs", result.Count(),
				  duration_cast<microseconds>(high_resolution_clock::now() - tmStart).count());
	}
}

void NamespaceImpl::updateSelectTime() {
	using namespace std::chrono;
	lastSelectTime_ = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
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

void NamespaceImpl::replicateTmUpdateIfRequired(UpdatesContainer& pendedRepl, int oldTmVersion, const NsContext& ctx) noexcept {
	if (oldTmVersion != tagsMatcher_.version()) {
		assertrx(ctx.GetOriginLSN().isEmpty());
		const auto lsn = wal_.Add(WALRecord(WalEmpty, 0, ctx.inTransaction), lsn_t());
#ifdef REINDEX_WITH_V3_FOLLOWERS
		if (!repl_.temporary && !ctx.inSnapshot) {
			WrSerializer ser;
			ser.PutVarint(tagsMatcher_.version());
			ser.PutVarint(tagsMatcher_.stateToken());
			tagsMatcher_.serialize(ser);
			WALRecord tmRec(WalTagsMatcher, ser.Slice(), ctx.inTransaction);
			observers_.OnWALUpdate(LSNPair(lsn, lsn), name_, tmRec);
		}
#endif	// REINDEX_WITH_V3_FOLLOWERS
		pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::SetTagsMatcherTx : UpdateRecord::Type::SetTagsMatcher, name_, lsn,
								repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), tagsMatcher_);
	}
}

void NamespaceImpl::Select(LocalQueryResults& result, SelectCtx& params, const RdxContext& ctx) {
	if (!params.query.IsWALQuery()) {
		NsSelecter selecter(this);
		selecter(result, params, ctx);
	} else {
		WALSelecter selecter(this, true);
		selecter(result, params);
	}
}

IndexDef NamespaceImpl::getIndexDefinition(size_t i) const {
	assertrx(i < indexes_.size());
	IndexDef indexDef;
	const Index& index = *indexes_[i];
	indexDef.name_ = index.Name();
	indexDef.opts_ = index.Opts();
	indexDef.FromType(index.Type());
	indexDef.expireAfter_ = index.GetTTLValue();

	if (index.Opts().IsSparse() || static_cast<int>(i) >= payloadType_.NumFields()) {
		int fIdx = 0;
		for (auto& f : index.Fields()) {
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
	NamespaceDef nsDef(name_, StorageOpts().Enabled(storage_.GetStatusCached().isEnabled));
	nsDef.indexes.reserve(indexes_.size());
	for (size_t i = 1; i < indexes_.size(); ++i) {
		nsDef.AddIndex(getIndexDefinition(i));
	}
	nsDef.isTemporary = repl_.temporary;
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
	using namespace std::string_view_literals;
	NamespaceMemStat ret;
	auto rlck = rLock(ctx);
	ret.name = name_;
	ret.joinCache = joinCache_->GetMemStat();
	ret.queryCache = queryCountCache_->GetMemStat();

	ret.itemsCount = ItemsCount();
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
	ret.Total.indexOptimizerMemory = nsUpdateSortedContextMemory_.load(std::memory_order_relaxed);
	ret.indexes.reserve(indexes_.size());
	for (const auto& idx : indexes_) {
		ret.indexes.emplace_back(idx->GetMemStat(ctx));
		auto& istat = ret.indexes.back();
		istat.sortOrdersSize = idx->IsOrdered() ? (items_.size() * sizeof(IdType)) : 0;
		ret.Total.indexesSize += istat.GetIndexStructSize();
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
	ret.optimizationCompleted = (optimizationState_ == OptimizationCompleted);

	ret.stringsWaitingToBeDeletedSize = strHolder_->MemStat();
	for (const auto& idx : strHolder_->Indexes()) {
		const auto& istat = idx->GetMemStat(ctx);
		ret.stringsWaitingToBeDeletedSize += istat.GetIndexStructSize() + istat.dataSize;
	}
	for (const auto& strHldr : strHoldersWaitingToBeDeleted_) {
		ret.stringsWaitingToBeDeletedSize += strHldr->MemStat();
		for (const auto& idx : strHldr->Indexes()) {
			const auto& istat = idx->GetMemStat(ctx);
			ret.stringsWaitingToBeDeletedSize += istat.GetIndexStructSize() + istat.dataSize;
		}
	}

	logPrintf(LogTrace, "[GetMemStat:%s]:%d replication (dataHash=%ld  dataCount=%d  lastLsn=%s)", ret.name, wal_.GetServer(),
			  ret.replication.dataHash, ret.replication.dataCount, ret.replication.lastLsn);

	return ret;
}

NamespacePerfStat NamespaceImpl::GetPerfStat(const RdxContext& ctx) {
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

void NamespaceImpl::ResetPerfStat(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	selectPerfCounter_.Reset();
	updatePerfCounter_.Reset();
	for (auto& i : indexes_) i->ResetIndexPerfStat();
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
		if (!status.ok() && status.code() != errNotFound) {
			logPrintf(LogTrace, "Error on namespace service info(tag: %s, id: %u) load '%s': %s", baseSysTag, i, name_, status.what());
			err = Error(errNotValid, "Error load namespace from storage '%s': %s", name_, status.what());
			continue;
		}

		if (status.ok() && content.size()) {
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

	std::string def;
	Error status = loadLatestSysRecord(kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (def.size()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logPrintf(LogInfo, "[tm:%s]:%d: TagsMatcher was loaded from storage. tm: { state_token: 0x%08X, version: %d }", name_,
				  wal_.GetServer(), tagsMatcher_.stateToken(), tagsMatcher_.version());
		logPrintf(LogTrace, "Loaded tags(version: %lld) of namespace %s:\n%s",
				  sysRecordsVersions_.tagsVersion ? sysRecordsVersions_.tagsVersion - 1 : 0, name_, tagsMatcher_.dump());
	}

	def.clear();
	status = loadLatestSysRecord(kStorageSchemaPrefix, sysRecordsVersions_.schemaVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (def.size()) {
		schema_ = std::make_shared<Schema>();
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
					verifyAddIndex(indexDef, [this]() { return this->name_; });
					addIndex(indexDef, false);
				} catch (const Error& e) {
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
	std::string json;
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

	if (!storage_.IsValid()) return;

	logPrintf(LogTrace, "Namespace::saveIndexesToStorage (%s)", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.idxVersion);
	ser.PutUInt32(kStorageMagic);
	ser.PutUInt32(kStorageVersion);

	ser.PutVarUint(indexes_.size() - 1);
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
	if (!storage_.IsValid()) return;

	logPrintf(LogTrace, "Namespace::saveSchemaToStorage (%s)", name_);

	if (!schema_) return;

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
	if (!storage_.IsValid()) return;

	if (direct) {
		replStateUpdates_.store(0, std::memory_order_release);
	}

	logPrintf(LogTrace, "Namespace::saveReplStateToStorage (%s)", name_);

	WrSerializer ser;
	ser.PutUInt64(sysRecordsVersions_.replVersion);
	JsonBuilder builder(ser);
	ReplicationState st = getReplState();
	st.GetJSON(builder);
	builder.End();
	writeSysRecToStorage(ser.Slice(), kStorageReplStatePrefix, sysRecordsVersions_.replVersion, direct);
}

void NamespaceImpl::saveTagsMatcherToStorage(bool clearUpdate) {
	if (storage_.IsValid() && tagsMatcher_.isUpdated()) {
		WrSerializer ser;
		ser.PutUInt64(sysRecordsVersions_.tagsVersion);
		tagsMatcher_.serialize(ser);
		if (clearUpdate) {	// Update flags should be cleared after some items updates (to replicate tagsmatcher with WALItemModify record)
			tagsMatcher_.clearUpdated();
		}
		writeSysRecToStorage(ser.Slice(), kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, false);
		logPrintf(LogTrace, "Saving tags of namespace %s:\n%s", name_, tagsMatcher_.dump());
	}
}

void NamespaceImpl::EnableStorage(const std::string& path, StorageOpts opts, StorageType storageType, const RdxContext& ctx) {
	std::string dbpath = fs::JoinPath(path, name_);

	auto wlck = simpleWLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	bool success = false;
	const bool storageDirExists = (fs::Stat(dbpath) == fs::StatDir);
	try {
		while (!success) {
			if (!opts.IsCreateIfMissing() && !storageDirExists) {
				throw Error(errNotFound,
							"Storage directory doesn't exist for namespace '%s' on path '%s' and CreateIfMissing option is not set", name_,
							path);
			}
			Error status = storage_.Open(storageType, name_, dbpath, opts);
			if (!status.ok()) {
				if (!opts.IsDropOnFileFormatError()) {
					storage_.Close();
					throw Error(errLogic, "Cannot enable storage for namespace '%s' on path '%s' - %s", name_, path, status.what());
				}
			} else {
				success = loadIndexesFromStorage();
				if (!success && !opts.IsDropOnFileFormatError()) {
					storage_.Close();
					throw Error(errLogic, "Cannot enable storage for namespace '%s' on path '%s': format error", name_, dbpath);
				}
				loadReplStateFromStorage();
			}
			if (!success && opts.IsDropOnFileFormatError()) {
				logPrintf(LogWarning, "Dropping storage for namespace '%s' on path '%s' due to format error", name_, dbpath);
				opts.DropOnFileFormatError(false);
				storage_.Destroy();
			}
		}
	} catch (...) {
		// if storage was created by this call
		if (!storageDirExists && (fs::Stat(dbpath) == fs::StatDir)) {
			logPrintf(LogWarning, "Dropping storage (via %s), which was created with errors ('%s':'%s')",
					  storage_.IsValid() ? "storage interface" : "filesystem", name_, dbpath);
			if (storage_.IsValid()) {
				storage_.Destroy();
			} else {
				fs::RmDirAll(dbpath);
			}
		}
		throw;
	}

	storageOpts_ = opts;
}

StorageOpts NamespaceImpl::GetStorageOpts(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return storageOpts_;
}

std::shared_ptr<const Schema> NamespaceImpl::GetSchemaPtr(const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	return schema_;
}

Error NamespaceImpl::SetClusterizationStatus(ClusterizationStatus&& status, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	if (repl_.temporary && status.role == ClusterizationStatus::Role::None) {
		return Error(errParams, "Unable to set replication role 'none' to temporary namespace");
	}
	repl_.clusterStatus = std::move(status);
	saveReplStateToStorage(true);
	return Error();
}

void NamespaceImpl::ApplySnapshotChunk(const SnapshotChunk& ch, bool isInitialLeaderSync, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	SnapshotHandler handler(*this);
	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(ctx, true);
	cg.Reset();
	checkClusterRole(ctx);

	handler.ApplyChunk(ch, isInitialLeaderSync, pendedRepl);
	if (ch.IsLastChunk()) {
#ifdef REINDEX_WITH_V3_FOLLOWERS
		// Actually we are not supposing to use this mechanism
		if (!repl_.temporary) {
			WrSerializer ser;
			auto nsDef = getDefinition();
			nsDef.GetJSON(ser);
			ser.PutBool(true);
			observers_.OnWALUpdate(LSNPair(), name_, WALRecord(WalWALSync, ser.Slice()));
		}
#endif	// REINDEX_WITH_V3_FOLLOWERS
		replicateAsync({isInitialLeaderSync ? UpdateRecord::Type::ResyncNamespaceLeaderInit : UpdateRecord::Type::ResyncNamespaceGeneric,
						name_, lsn_t(0, 0), lsn_t(0, 0), ctx.EmmiterServerId()},
					   ctx);
	}
}

void NamespaceImpl::GetSnapshot(Snapshot& snapshot, const SnapshotOpts& opts, const RdxContext& ctx) {
	SnapshotHandler handler(*this);
	auto rlck = rLock(ctx);
	snapshot = handler.CreateSnapshot(opts);
}

void NamespaceImpl::SetTagsMatcher(TagsMatcher&& tm, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);
	setTagsMatcher(std::move(tm), pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, ctx);
}

void NamespaceImpl::LoadFromStorage(unsigned threadsCount, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	uint64_t dataHash = repl_.dataHash;
	repl_.dataHash = 0;

	ItemsLoader loader(threadsCount, *this);
	auto ldata = loader.Load();

	initWAL(ldata.minLSN, ldata.maxLSN);
	if (!isSystem()) {
		repl_.lastLsn.SetServer(wal_.GetServer());
	}

	logPrintf(LogInfo, "[%s] Done loading storage. %d items loaded (%d errors %s), lsn #%s, total size=%dM, dataHash=%ld", name_,
			  items_.size(), ldata.errCount, ldata.lastErr.what(), repl_.lastLsn, ldata.ldcount / (1024 * 1024), repl_.dataHash);
	if (dataHash != repl_.dataHash) {
		logPrintf(LogError, "[%s] Warning dataHash mismatch %lu != %lu", name_, dataHash, repl_.dataHash);
		replStateUpdates_.fetch_add(1, std::memory_order_release);
	}

	markUpdated(true);
}

void NamespaceImpl::initWAL(int64_t minLSN, int64_t maxLSN) {
	wal_.Init(getWalSize(config_), minLSN, maxLSN, storage_);
	// Fill existing records
	for (IdType rowId = 0; rowId < IdType(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			wal_.Set(WALRecord(WalItemUpdate, rowId), items_[rowId].GetLSN(), true);
		}
	}
	repl_.lastLsn = wal_.LastLSN();
	logPrintf(LogInfo, "[%s] WAL has been initalized lsn #%s, max size %ld", name_, repl_.lastLsn, wal_.Capacity());
}

void NamespaceImpl::removeExpiredItems(RdxActivityContext* ctx) {
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
	for (const std::unique_ptr<Index>& index : indexes_) {
		if ((index->Type() != IndexTtl) || (index->Size() == 0)) continue;
		const int64_t expirationthreshold =
			std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count() -
			index->GetTTLValue();
		LocalQueryResults qr;
		qr.AddNamespace(this, true);
		doDelete(Query(name_).Where(index->Name(), CondLt, expirationthreshold), qr, pendedRepl, NsContext(rdxCtx));
	}
	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, RdxContext(ctx));
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

void NamespaceImpl::setSchema(std::string_view schema, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	schema_ = std::make_shared<Schema>(schema);
	auto fields = schema_->GetPaths();
	for (auto& field : fields) {
		tagsMatcher_.path2tag(field, true);
	}

	schema_->BuildProtobufSchema(tagsMatcher_, payloadType_);

	addToWAL(schema, WalSetSchema, ctx);
	pendedRepl.emplace_back(UpdateRecord::Type::SetSchema, name_, wal_.LastLSN(), repl_.nsVersion, ctx.rdxContext.EmmiterServerId(),
							std::string(schema));
}

void NamespaceImpl::setTagsMatcher(TagsMatcher&& tm, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (ctx.GetOriginLSN().isEmpty()) {
		throw Error(errLogic, "Tagsmatcher may be set by replication only");
	}
	if (tm.stateToken() != tagsMatcher_.stateToken()) {
		throw Error(errParams, "Tagsmatcher have different statetokens: %08X vs %08X", tagsMatcher_.stateToken(), tm.stateToken());
	}
	tagsMatcher_ = tm;
	tagsMatcher_.UpdatePayloadType(payloadType_, NeedChangeTmVersion::No);
	tagsMatcher_.setUpdated();

	const auto lsn = wal_.Add(WALRecord(WalEmpty, 0, ctx.inTransaction), ctx.GetOriginLSN());
#ifdef REINDEX_WITH_V3_FOLLOWERS
	if (!repl_.temporary && !ctx.inSnapshot) {
		WrSerializer ser;
		ser.PutVarint(tagsMatcher_.version());
		ser.PutVarint(tagsMatcher_.stateToken());
		tagsMatcher_.serialize(ser);
		observers_.OnWALUpdate(LSNPair(lsn, lsn), name_, WALRecord(WalTagsMatcher, ser.Slice(), ctx.inTransaction));
	}
#endif	// REINDEX_WITH_V3_FOLLOWERS
	pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::SetTagsMatcherTx : UpdateRecord::Type::SetTagsMatcher, name_, lsn,
							repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), std::move(tm));

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
}

void NamespaceImpl::StorageFlushingRoutine() { storage_.Flush(StorageFlushOpts()); }

void NamespaceImpl::DeleteStorage(const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	storage_.Destroy();
}

void NamespaceImpl::CloseStorage(const RdxContext& ctx) {
	storage_.Flush(StorageFlushOpts().WithImmediateReopen());
	auto wlck = simpleWLock(ctx);
	if (replStateUpdates_.load(std::memory_order_relaxed)) {
		saveReplStateToStorage(true);
		replStateUpdates_.store(0, std::memory_order_relaxed);
	}
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
		*(reinterpret_cast<uint64_t*>(const_cast<char*>(data.data()))) = version;
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

// Get meta data from storage by key
std::string NamespaceImpl::GetMeta(const std::string& key, const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return getMeta(key);
}

std::string NamespaceImpl::getMeta(const std::string& key) const {
	auto it = meta_.find(key);
	if (it != meta_.end()) {
		return it->second;
	}

	std::string data;
	Error status = storage_.Read(StorageOpts().FillCache(), std::string_view(kStorageMetaPrefix + key), data);
	if (status.ok()) {
		return data;
	}

	return std::string();
}

// Put meta data to storage by key
void NamespaceImpl::PutMeta(const std::string& key, std::string_view data, const RdxContext& ctx) {
	UpdatesContainer pendedRepl;
	auto wlck = dataWLock(ctx);

	putMeta(key, data, pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

// Put meta data to storage by key
void NamespaceImpl::putMeta(const std::string& key, std::string_view data, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	meta_[key] = std::string(data);

	storage_.WriteSync(StorageOpts().FillCache(), kStorageMetaPrefix + key, data);

	processWalRecord(WALRecord(WalPutMeta, key, data, ctx.inTransaction), ctx);
	pendedRepl.emplace_back(ctx.inTransaction ? UpdateRecord::Type::PutMetaTx : UpdateRecord::Type::PutMeta, name_, wal_.LastLSN(),
							repl_.nsVersion, ctx.rdxContext.EmmiterServerId(), key, std::string(data));
}

std::vector<std::string> NamespaceImpl::EnumMeta(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return enumMeta();
}

std::vector<std::string> NamespaceImpl::enumMeta() const {
	std::vector<std::string> ret;
	ret.reserve(meta_.size());
	for (auto& m : meta_) {
		ret.push_back(m.first);
	}
	if (!storage_.IsValid()) return ret;

	StorageOpts opts;
	opts.FillCache(false);
	auto dbIter = storage_.GetCursor(opts);
	size_t prefixLen = strlen(kStorageMetaPrefix);

	for (dbIter->Seek(std::string_view(kStorageMetaPrefix));
		 dbIter->Valid() && dbIter->GetComparator().Compare(dbIter->Key(), std::string_view(kStorageMetaPrefix "\xFF\xFF\xFF\xFF")) < 0;
		 dbIter->Next()) {
		std::string_view keySlice = dbIter->Key();
		if (keySlice.size() > prefixLen) {
			std::string key(keySlice.substr(prefixLen));
			if (meta_.find(key) == meta_.end()) {
				ret.push_back(key);
			}
		}
	}
	return ret;
}

void NamespaceImpl::warmupFtIndexes() {
	h_vector<std::thread, 8> warmupThreads;
	h_vector<Index*, 8> warmupIndexes;
	for (auto& idx : indexes_) {
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
	for (auto& th : warmupThreads) {
		th.join();
	}
}

int NamespaceImpl::getSortedIdxCount() const noexcept {
	if (!config_.optimizationSortWorkers) return 0;
	int cnt = 0;
	for (auto& it : indexes_)
		if (it->IsOrdered()) cnt++;
	return cnt;
}

void NamespaceImpl::updateSortedIdxCount() {
	int sortedIdxCount = getSortedIdxCount();
	for (auto& idx : indexes_) idx->SetSortedIdxCount(sortedIdxCount);
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
			if (id == std::numeric_limits<IdType>::max()) {
				throw Error(errParams, "Max item ID value is reached: %d", id);
			}
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

void NamespaceImpl::setFieldsBasedOnPrecepts(ItemImpl* ritem, UpdatesContainer& replUpdates, const NsContext& ctx) {
	for (auto& precept : ritem->GetPrecepts()) {
		SelectFuncParser sqlFunc;
		SelectFuncStruct sqlFuncStruct = sqlFunc.Parse(precept);

		ritem->GetPayload().Get(sqlFuncStruct.field, krefs);

		skrefs.clear<false>();
		if (sqlFuncStruct.isFunction) {
			skrefs.emplace_back(FunctionExecutor(*this, replUpdates).Execute(sqlFuncStruct, ctx));
		} else {
			skrefs.emplace_back(make_key_string(sqlFuncStruct.value));
		}

		skrefs.back().convert(krefs[0].Type());
		bool unsafe = ritem->IsUnsafe();
		ritem->Unsafe(false);
		ritem->SetField(ritem->GetPayload().Type().FieldByName(sqlFuncStruct.field), skrefs);
		ritem->Unsafe(unsafe);
	}
}

int64_t NamespaceImpl::GetSerial(const std::string& field, UpdatesContainer& replUpdates, const NsContext& ctx) {
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

void NamespaceImpl::FillResult(LocalQueryResults& result, const IdSet& ids) const {
	for (auto& id : ids) {
		result.Add({id, items_[id], 0, 0});
	}
}

void NamespaceImpl::getFromJoinCache(const Query& q, const JoinedQuery& jq, JoinCacheRes& out) const {
	if (config_.cacheMode == CacheModeOff || optimizationState_ != OptimizationCompleted) return;
	out.key.SetData(jq, q);
	getFromJoinCacheImpl(out);
}

void NamespaceImpl::getFromJoinCache(const Query& q, JoinCacheRes& out) const {
	if (config_.cacheMode == CacheModeOff || optimizationState_ != OptimizationCompleted) return;
	out.key.SetData(q);
	getFromJoinCacheImpl(out);
}

void NamespaceImpl::getFromJoinCacheImpl(JoinCacheRes& ctx) const {
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

void NamespaceImpl::getIndsideFromJoinCache(JoinCacheRes& ctx) const {
	if (config_.cacheMode != CacheModeAggressive || optimizationState_ != OptimizationCompleted) return;
	getFromJoinCacheImpl(ctx);
}

void NamespaceImpl::putToJoinCache(JoinCacheRes& res, JoinPreResult::Ptr preResult) const {
	JoinCacheVal joinCacheVal;
	res.needPut = false;
	joinCacheVal.inited = true;
	joinCacheVal.preResult = std::move(preResult);
	joinCache_->Put(res.key, std::move(joinCacheVal));
}
void NamespaceImpl::putToJoinCache(JoinCacheRes& res, JoinCacheVal&& val) const {
	val.inited = true;
	joinCache_->Put(res.key, std::move(val));
}

const FieldsSet& NamespaceImpl::pkFields() {
	auto it = indexesNames_.find(kPKIndexName);
	if (it != indexesNames_.end()) {
		return indexes_[it->second]->Fields();
	}

	static FieldsSet ret;
	return ret;
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
		item->setLSN(lsn);
	}
#ifdef REINDEX_WITH_V3_FOLLOWERS
	if (!repl_.temporary && !ctx.inSnapshot) {
		observers_.OnWALUpdate(LSNPair(lsn, lsn), name_, wrec);
	}
#endif	// REINDEX_WITH_V3_FOLLOWERS
}

void NamespaceImpl::replicateAsync(cluster::UpdateRecord&& rec, const RdxContext& ctx) {
	if (!repl_.temporary) {
		auto err = clusterizator_.ReplicateAsync(std::move(rec), ctx);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.what());
		}
	}
}

void NamespaceImpl::replicateAsync(NamespaceImpl::UpdatesContainer&& recs, const RdxContext& ctx) {
	if (!repl_.temporary) {
		auto err = clusterizator_.ReplicateAsync(std::move(recs), ctx);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.what());
		}
	}
}

std::set<std::string> NamespaceImpl::GetFTIndexes(const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	std::set<std::string> ret;
	for (const auto& idx : indexes_) {
		switch (idx->Type()) {
			case IndexFastFT:
			case IndexFuzzyFT:
			case IndexCompositeFastFT:
			case IndexCompositeFuzzyFT:
				ret.insert(idx->Name());
				break;
			case IndexStrHash:
			case IndexStrBTree:
			case IndexIntBTree:
			case IndexIntHash:
			case IndexInt64BTree:
			case IndexInt64Hash:
			case IndexDoubleBTree:
			case IndexCompositeBTree:
			case IndexCompositeHash:
			case IndexBool:
			case IndexIntStore:
			case IndexInt64Store:
			case IndexStrStore:
			case IndexDoubleStore:
			case IndexTtl:
			case IndexRTree:
			case IndexUuidHash:
			case IndexUuidStore:
				break;
		}
	}
	return ret;
}

NamespaceImpl::IndexesCacheCleaner::~IndexesCacheCleaner() {
	for (auto& idx : ns_.indexes_) idx->ClearCache(sorts_);
}

}  // namespace reindexer
