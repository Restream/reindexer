#include "core/namespace/namespaceimpl.h"

#include <algorithm>
#include <ctime>
#include <memory>
#include <string_view>
#include "core/cjson/cjsondecoder.h"
#include "core/cjson/jsonbuilder.h"
#include "core/cjson/uuid_recoders.h"
#include "core/embedding/embedder.h"
#include "core/embedding/embedderscache.h"
#include "core/index/float_vector/float_vector_index.h"
#include "core/index/index.h"
#include "core/index/indexfastupdate.h"
#include "core/index/ttlindex.h"
#include "core/itemimpl.h"
#include "core/itemmodifier.h"
#include "core/nsselecter/nsselecter.h"
#include "core/payload/payloadiface.h"
#include "core/query/expression/function_executor.h"
#include "core/query/expression/function_parser.h"
#include "core/querystat.h"
#include "core/rdxcontext.h"
#include "debug/crashqueryreporter.h"
#include "estl/gift_str.h"
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
#include "tx_concurrent_inserter.h"
#include "wal/walselecter.h"

using std::chrono::duration_cast;
using std::chrono::microseconds;
using std::chrono::nanoseconds;
using namespace std::string_view_literals;

namespace {
constexpr std::string_view kStorageIndexesPrefix{"indexes"sv};
constexpr std::string_view kStorageSchemaPrefix{"schema"sv};
constexpr std::string_view kStorageReplStatePrefix{"repl"sv};
constexpr std::string_view kStorageTagsPrefix{"tags"sv};

constexpr std::string_view kPKIndexName{"#pk"sv};

const std::string kTupleName{"-tuple"};
const std::string kStorageMetaPrefix{"meta"};
const std::string kFFFFFFFF{"\xFF\xFF\xFF\xFF"};

// TODO disabled due to #1771
// constexpr int kWALStatementItemsThreshold{5};

constexpr uint32_t kStorageMagic{0x1234FEDC};
constexpr uint32_t kStorageVersion{0x8};
}  // namespace

namespace reindexer {

std::atomic_bool rxAllowNamespaceLeak = {false};

constexpr int64_t kStorageSerialInitial = 1;
constexpr uint8_t kSysRecordsBackupCount = 8;
constexpr uint8_t kSysRecordsFirstWriteCopies = 3;
constexpr size_t kMaxMemorySizeOfStringsHolder = 1ull << 24;
constexpr size_t kMaxSchemaCharsToPrint = 128;

NamespaceImpl::IndexesStorage::IndexesStorage(const NamespaceImpl& ns) noexcept : ns_(ns) {}

// private implementation and NOT THREADSAFE of copy CTOR
NamespaceImpl::NamespaceImpl(const NamespaceImpl& src, size_t newCapacity, AsyncStorage::FullLock& storageLock)
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
	  floatVectorsIndexesPositions_{src.floatVectorsIndexesPositions_},
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
	for (auto& idxIt : src.indexes_) {
		indexes_.push_back(idxIt->Clone(newCapacity));
	}
	queryCountCache_.CopyInternalPerfStatsFrom(src.queryCountCache_);
	joinCache_.CopyInternalPerfStatsFrom(src.joinCache_);

	markUpdated(IndexOptimization::Full);
	logFmt(LogInfo, "Namespace::CopyContentsFrom ({}).Workers: {}, timeout: {}, tm: {{ state_token: {:#08x}, version: {} }}", name_,
		   config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher_.stateToken(), tagsMatcher_.version());
}

static int64_t GetCurrentTimeUS() noexcept { return duration_cast<microseconds>(system_clock_w::now().time_since_epoch()).count(); }

NamespaceImpl::NamespaceImpl(const std::string& name, std::optional<int32_t> stateToken, const cluster::IDataSyncer& syncer,
							 UpdatesObservers& observers, const std::shared_ptr<EmbeddersCache>& embeddersCache)
	: intrusive_atomic_rc_base(),
	  indexes_{*this},
	  name_{name},
	  payloadType_{name},
	  tagsMatcher_(payloadType_, {}, stateToken.has_value() ? stateToken.value() : tools::RandomGenerator::gets32()),
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
		   config_.optimizationSortWorkers, config_.optimizationTimeout, tagsMatcher_.stateToken(),
		   stateToken.has_value() ? "preset" : "rand", tagsMatcher_.version());
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
		threadsCount = k * hardware_concurrency();
		if (threadsCount > indexes_.size() + 1) {
			threadsCount = indexes_.size() + 1;
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

	if (allowLeak) {
		logFmt(LogTrace, "Namespace::~Namespace:{} {} items. Leak mode", name_, items_.size());
		for (auto& indx : indexes_) {
			indx.release();	 // NOLINT(bugprone-unused-return-value)
		}
		return;
	}

	if (multithreadingMode) {
		logFmt(LogTrace, "Namespace::~Namespace:{} {} items. Multithread mode. Deletion threads: {}", name_, items_.size(), threadsCount);
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

	configData.maxIterationsIdSetPreResult = correctMaxIterationsIdSetPreResult(configData.maxIterationsIdSetPreResult);
	const bool needReconfigureIdxCache = !config_.cacheConfig.IsIndexesCacheEqual(configData.cacheConfig);
	const bool needReconfigureJoinCache = !config_.cacheConfig.IsJoinCacheEqual(configData.cacheConfig);
	const bool needReconfigureQueryCountCache = !config_.cacheConfig.IsQueryCountCacheEqual(configData.cacheConfig);
	config_ = configData;
	storage_.SetForceFlushLimit(config_.syncStorageFlushLimit);

	for (auto& idx : indexes_) {
		idx->EnableUpdatesCountingMode(config_.idxUpdatesCountingMode);
		if (auto vecIdx = dynamic_cast<FloatVectorIndex*>(idx.get()); vecIdx) {
			vecIdx->EnablePerfStat(enablePerfCounters_);
		}
	}
	if (needReconfigureIdxCache) {
		for (auto& idx : indexes_) {
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
	indexOptimizer_.SetConfig(name_, indexes_,
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
			   tagsMatcher_.stateToken());
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

template <NeedRollBack needRollBack>
class [[nodiscard]] NamespaceImpl::RollBack_recreateCompositeIndexes final : private RollBackBase {
public:
	RollBack_recreateCompositeIndexes(NamespaceImpl& ns, size_t startIdx, size_t count) : ns_{ns}, startIdx_{startIdx} {
		indexes_.reserve(count);
	}
	~RollBack_recreateCompositeIndexes() override {
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
		if (IsDisabled()) {
			return;
		}
		for (size_t i = 0, s = indexes_.size(); i < s; ++i) {
			std::swap(ns_.indexes_[i + startIdx_], indexes_[i]);
		}
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
	size_t startIdx_{0};
};

template <>
class [[nodiscard]] NamespaceImpl::RollBack_recreateCompositeIndexes<NeedRollBack::No> {
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
NamespaceImpl::RollBack_recreateCompositeIndexes<needRollBack> NamespaceImpl::recreateCompositeIndexes(FieldChangeType fieldChangeType,
																									   size_t startIdx, size_t endIdx) {
	RollBack_recreateCompositeIndexes<needRollBack> rollbacker{*this, startIdx, endIdx - startIdx};
	for (size_t i = startIdx; i < endIdx; ++i) {
		std::unique_ptr<Index>& index(indexes_[i]);
		if (IsComposite(index->Type())) {
			IndexDef indexDef{index->Name(), {}, index->Type(), index->Opts()};

			FieldsSet fields;
			const auto& curFields = index->Fields();
			if (fieldChangeType == FieldChangeType::Add) {
				size_t jsonPathIdx = 0;
				for (int field : curFields) {
					if (field == SetByJsonPath) {
						const auto& jsonPath = curFields.getJsonPath(jsonPathIdx);
						if (!tryGetScalarIndexByName(jsonPath, field)) {
							fields.push_back(curFields.getTagsPath(jsonPathIdx));
							fields.push_back(jsonPath);
						}
						++jsonPathIdx;
					}
					fields.push_back(field);
				}
				assertrx(fields.getJsonPathsLength() == fields.getTagsPathsLength());
			} else {
				fields = curFields;
			}

			auto newIndex{Index::New(indexDef, PayloadType{payloadType_}, std::move(fields), config_.cacheConfig, itemsCount())};
			rollbacker.SaveIndex(std::move(index));
			std::swap(index, newIndex);
		}
	}
	return rollbacker;
}

template <NeedRollBack needRollBack>
class [[nodiscard]] NamespaceImpl::RollBack_updateItems final : private RollBackBase {
public:
	RollBack_updateItems(NamespaceImpl& ns, RollBack_recreateCompositeIndexes<needRollBack>&& rb, uint64_t dh, size_t ds) noexcept
		: ns_{ns}, rollbacker_recreateCompositeIndexes_{std::move(rb)}, dataHash_{dh}, itemsDataSize_{ds} {
		items_.reserve(ns_.items_.size());
	}
	~RollBack_updateItems() override { RollBack(); }
	void Disable() noexcept override {
		rollbacker_recreateCompositeIndexes_.Disable();
		RollBackBase::Disable();
	}
	// NOLINTNEXTLINE(bugprone-exception-escape) Termination here is better, than inconsistent state of the user's data
	void RollBack() noexcept {
		if (IsDisabled()) {
			return;
		}
		if (!items_.empty()) {
			ns_.repl_.dataHash = dataHash_;
			ns_.itemsDataSize_ = itemsDataSize_;
		}
		if (tuple_) {
			std::swap(ns_.indexes_[0], tuple_);
		}
		WrSerializer pk, data;
		const auto pkFieldsSet = ns_.pkFields();
		const auto vectorIndexes = ns_.getVectorIndexes();
		for (auto& [rowId, pv] : items_) {
			ns_.items_[rowId] = std::move(pv);
			if (storageWasRewritten_) {
				// We have to rewrite storage data in case, when have arrays with double values in storage,
				// otherwise datahash won't match
				logFmt(LogInfo, "[{}] Rewriting storage on rollback", ns_.name_);
				ItemImpl item(ns_.payloadType_, ns_.items_[rowId], ns_.tagsMatcher_);
				item.Unsafe(true);
				ns_.tryWriteItemIntoStorage(pkFieldsSet, item, rowId, vectorIndexes, pk, data);
			}
		}
		rollbacker_recreateCompositeIndexes_.RollBack();
		for (auto& idx : ns_.indexes_) {
			idx->UpdatePayloadType(PayloadType{ns_.payloadType_});
		}
		Disable();
	}
	void SaveItem(size_t rowId, PayloadValue&& pv) { items_.emplace_back(rowId, std::move(pv)); }
	void SaveTuple() { tuple_ = ns_.indexes_[0]->Clone(0); }
	void MarkStorageRewrite() noexcept { storageWasRewritten_ = true; }

	// NOLINTNEXTLINE (performance-noexcept-move-constructor)
	RollBack_updateItems(RollBack_updateItems&&) = default;
	RollBack_updateItems(const RollBack_updateItems&) = delete;
	RollBack_updateItems& operator=(const RollBack_updateItems&) = delete;
	RollBack_updateItems& operator=(RollBack_updateItems&&) = delete;

private:
	NamespaceImpl& ns_;
	RollBack_recreateCompositeIndexes<needRollBack> rollbacker_recreateCompositeIndexes_;
	std::vector<std::pair<size_t, PayloadValue>> items_;
	uint64_t dataHash_{0};
	size_t itemsDataSize_{0};
	std::unique_ptr<Index> tuple_;
	bool storageWasRewritten_{false};
};

template <>
class [[nodiscard]] NamespaceImpl::RollBack_updateItems<NeedRollBack::No> {
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

void NamespaceImpl::tryWriteItemIntoStorage(const FieldsSet& pkFields, ItemImpl& item, IdType rowId,
											const FloatVectorsIndexes& vectorIndexes, WrSerializer& pk, WrSerializer& data) noexcept {
	if (item.payloadValue_.IsFree()) {
		return;
	}
	try {
		if (storage_.IsValid()) {
			item.CopyIndexedVectorsValuesFrom(rowId, vectorIndexes);

			auto pl = item.GetConstPayload();
			pk.Reset();
			data.Reset();
			pk << kRxStorageItemPrefix;
			pl.SerializeFields(pk, pkFields);
			data.PutUInt64(uint64_t(pl.Value()->GetLSN()));
			storage_.Write(pk.Slice(), item.GetCJSON(data));
			storage_.TryForceFlush();
		}
	} catch (Error& err) {
		logFmt(LogWarning, "[{}] Unable to write item into storage: {}", name_, err.what());
	} catch (std::exception& err) {
		logFmt(LogWarning, "[{}] Unable to write item into storage: {}", name_, err.what());
	} catch (...) {
		logFmt(LogWarning, "[{}] Unable to write item into storage: <unknown exception>", name_);
	}
}

template <NeedRollBack needRollBack, NamespaceImpl::FieldChangeType fieldChangeType>
NamespaceImpl::RollBack_updateItems<needRollBack> NamespaceImpl::updateItems(const PayloadType& oldPlType, int changedField) {
	logFmt(LogTrace, "Namespace::updateItems({}) changeType={}", name_, fieldChangeType == FieldChangeType::Add ? "Add" : "Delete");

	assertrx(oldPlType->NumFields() + int(fieldChangeType) == payloadType_->NumFields());

	const int compositeStartIdx = (fieldChangeType == FieldChangeType::Add) ? indexes_.firstCompositePos()
																			: indexes_.firstCompositePos(oldPlType, sparseIndexesCount_);
	const int compositeEndIdx = indexes_.totalSize();
	// all composite indexes must be recreated, because those indexes are holding pointers to old Payloads
	RollBack_updateItems<needRollBack> rollbacker{
		*this, recreateCompositeIndexes<needRollBack>(fieldChangeType, compositeStartIdx, compositeEndIdx), repl_.dataHash, itemsDataSize_};
	for (auto& idx : indexes_) {
		idx->UpdatePayloadType(PayloadType{payloadType_});
	}

	// no items, work done, stop processing
	if (items_.empty()) {
		return rollbacker;
	}

	std::unique_ptr<Recoder> recoder;
	if constexpr (fieldChangeType == FieldChangeType::Delete) {
		assertrx_throw(changedField > 0);
		const auto& fld = oldPlType.Field(changedField);
		const auto& jsonPaths = fld.JsonPaths();
		fld.Type().EvaluateOneOf(
			[&](KeyValueType::Uuid) {
				assertrx(jsonPaths.size() == 1);
				const auto tags = tagsMatcher_.path2tag(jsonPaths[0]);
				if (fld.IsArray()) {
					recoder = std::make_unique<RecoderUuidToString<true>>(tags);
				} else {
					recoder = std::make_unique<RecoderUuidToString<false>>(tags);
				}
			},
			Skip<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String,
				 KeyValueType::Bool, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
				 KeyValueType::FloatVector>{});

	} else {
		static_assert(fieldChangeType == FieldChangeType::Add);
		assertrx_throw(changedField > 0);
		const auto& fld = payloadType_.Field(changedField);
		fld.Type().EvaluateOneOf(
			[&](KeyValueType::Uuid) {
				if (fld.IsArray()) {
					recoder = std::make_unique<RecoderStringToUuidArray>(changedField);
				} else {
					recoder = std::make_unique<RecoderStringToUuid>(changedField);
				}
			},
			Skip<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String,
				 KeyValueType::Bool, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
				 KeyValueType::FloatVector>{});
	}
	rollbacker.SaveTuple();

	VariantArray skrefsDel, skrefsUps;
	ItemImpl newItem(payloadType_, tagsMatcher_);
	newItem.Unsafe(true);
	repl_.dataHash = 0;
	itemsDataSize_ = 0;
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};

	auto& tuple = *indexes_[0];
	auto& index = *indexes_[changedField];

	bool needToRewriteStorageData = false;
	if constexpr (fieldChangeType == FieldChangeType::Add) {
		if (index.IsFloatVector()) {
			rollbacker.MarkStorageRewrite();
			needToRewriteStorageData = true;
		}
	}

	WrSerializer pk, data;
	const auto pkFieldsSet = pkFields();
	const auto vectorIndexesNew =
		getVectorIndexes(payloadType_);	 // the indexes in the indexesNames_ set have already changed and do not match the indexes_ array.
	const auto vectorIndexesOld = getVectorIndexes(oldPlType);
	for (size_t rowId = 0; rowId < items_.size(); ++rowId) {
		if (items_[rowId].IsFree()) {
			continue;
		}
		PayloadValue& plCurr = items_[rowId];
		Payload oldValue(oldPlType, plCurr);
		ItemImpl oldItem(oldPlType, plCurr, tagsMatcher_);
		oldItem.Unsafe(true);
		oldItem.CopyIndexedVectorsValuesFrom(rowId, vectorIndexesOld);
		if (recoder) {
			recoder->Prepare(rowId);
		}

		try {
			newItem.FromCJSON(oldItem, recoder.get());
		} catch (const std::exception& err) {
			throwIndexUpsertErrorWithPKInfo(ConstPayload(oldPlType, plCurr), err);
		}

		PayloadValue plNew = oldValue.CopyTo(payloadType_, fieldChangeType == FieldChangeType::Add);
		plNew.SetLSN(plCurr.GetLSN());

		// update tuple
		oldValue.Get(0, skrefsDel, Variant::hold);
		bool needClearCache{false};
		tuple.Delete(skrefsDel, rowId, MustExist_True, *strHolder_, needClearCache);
		newItem.GetPayload().Get(0, skrefsUps);
		krefs.resize(0);
		tuple.Upsert(krefs, skrefsUps, rowId, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(tuple);
		}

		// update index
		Payload newValue(payloadType_, plNew);
		newValue.Set(0, krefs);

		if constexpr (fieldChangeType == FieldChangeType::Delete) {
			oldValue.Get(changedField, skrefsDel, Variant::hold);
			needClearCache = false;
			index.Delete(skrefsDel, rowId, MustExist_True, *strHolder_, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
		} else {
			static_assert(fieldChangeType == FieldChangeType::Add);

			newItem.GetPayload().Get(changedField, skrefsUps);
			krefs.resize(0);
			needClearCache = false;
			index.Upsert(krefs, skrefsUps, rowId, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
			newValue.Set(changedField, krefs);

			if (needToRewriteStorageData) {
				// We have to rewrite storage data in case, when have arrays with double values in storage,
				// otherwise datahash won't match.
				newItem.payloadValue_.SetLSN(plNew.GetLSN());
				tryWriteItemIntoStorage(pkFieldsSet, newItem, rowId, vectorIndexesNew, pk, data);
			}
		}

		for (int fieldIdx = compositeStartIdx; fieldIdx < compositeEndIdx; ++fieldIdx) {
			needClearCache = false;
			auto& fieldIndex = *indexes_[fieldIdx];
			std::ignore = fieldIndex.Upsert(Variant(plNew), rowId, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(fieldIndex);
			}
		}

		rollbacker.SaveItem(rowId, std::move(plCurr));
		plCurr = std::move(plNew);
		repl_.dataHash ^= calculateItemHash(rowId, (fieldChangeType == FieldChangeType::Delete) ? changedField : -1);
		itemsDataSize_ += plCurr.GetCapacity() + sizeof(PayloadValue::dataHeader);
	}

	markUpdated(IndexOptimization::Partial);
	return rollbacker;
}

uint64_t NamespaceImpl::calculateItemHash(IdType rowId, int removedIdxId) const noexcept {
	return ConstPayload{payloadType_, items_[rowId]}.GetHash(
		[this, rowId, removedIdxId](unsigned field, ConstFloatVectorView vec) noexcept -> uint64_t {
			if (vec.IsStripped()) {
				unsigned actualField = field;
				if (removedIdxId >= 0 && field >= unsigned(removedIdxId)) {
					actualField = field + 1;
				}
				auto idx = dynamic_cast<const FloatVectorIndex*>(indexes_[actualField].get());
				assertf(idx, "Expecting '{}' in '{}' being float vector index", indexes_[actualField]->Name(), name_);
				return idx->GetHash(rowId);
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

	auto wlck = dataWLock(rdxCtx, true);

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

	auto wlck = dataWLock(rdxCtx);

	if (doUpdateIndex(indexDef, pendedRepl, ctx)) {
		saveIndexesToStorage();
		replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
	}
}

void NamespaceImpl::DropIndex(const IndexDef& indexDef, const RdxContext& rdxCtx) {
	indexDef.Validate();

	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	auto wlck = dataWLock(rdxCtx);
	doDropIndex(indexDef, pendedRepl, ctx);
	saveIndexesToStorage();
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::SetSchema(std::string_view schema, const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

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
	} else if (format == ProtobufSchemaType) {
		throw Error(errLogic, "Schema is not initialized either just empty");
	}
	return std::string(ser.Slice());
}

void NamespaceImpl::dumpIndex(std::ostream& os, std::string_view index) const {
	auto itIdxName = indexesNames_.find(index);
	if (itIdxName == indexesNames_.end()) {
		constexpr auto errMsg = "Cannot dump index {}: doesn't exist";
		logFmt(LogError, errMsg, index);
		throw Error(errParams, errMsg, index);
	}
	indexes_[itIdxName->second]->Dump(os);
}

void NamespaceImpl::clearNamespaceCaches() {
	queryCountCache_.Clear();
	joinCache_.Clear();
}

class [[nodiscard]] NamespaceImpl::RollBack_dropIndex final : private RollBackBase {
public:
	explicit RollBack_dropIndex(NamespaceImpl& ns, int fieldIdx) noexcept : ns_{ns}, fieldIdx_{fieldIdx} {}
	~RollBack_dropIndex() override { RollBack(); }
	void RollBack() noexcept {
		if (IsDisabled()) {
			return;
		}
		if (oldPayloadType_) {
			ns_.payloadType_ = std::move(*oldPayloadType_);
		}
		if (needResetPayloadTypeInTagsMatcher_) {
			try {
				ns_.tagsMatcher_.UpdatePayloadType(ns_.payloadType_, ns_.indexes_.SparseIndexes(), needChangeTmVersion_);
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
		for (auto& [idx, fields] : oldIndexesFieldsSets_) {
			try {
				idx->SetFields(std::move(fields));
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
		if (needInsertPKIndexName_) {
			try {
				ns_.indexesNames_.emplace(kPKIndexName, fieldIdx_);
				// NOLINTBEGIN(bugprone-empty-catch)
			} catch (...) {
			}
			// NOLINTEND(bugprone-empty-catch)
		}
		if (needUpdateIndexesNames_) {
			for (auto& namePair : ns_.indexesNames_) {
				if (namePair.second >= fieldIdx_) {
					++namePair.second;
				}
			}
		}
		if (needIncrementSparseIndexesCount_) {
			++ns_.sparseIndexesCount_;
		}
		if (updateFloatVectorsIndexesPositions_ != FloatVectorsIndexsPositions::No) {
			auto& positions = ns_.floatVectorsIndexesPositions_;
			auto it = positions.begin();
			auto end = positions.end();
			for (; it != end && *it < size_t(fieldIdx_); ++it) {
			}
			if (updateFloatVectorsIndexesPositions_ == FloatVectorsIndexsPositions::Insert) {
				try {
					it = positions.insert(it, fieldIdx_);
					++it;
					// NOLINTNEXTLINE(bugprone-empty-catch)
				} catch (...) {
				}
				end = positions.end();
			}
			for (; it != end; ++it) {
				++*it;
			}
		}
	}
	void NeedIncrementSparseIndexesCount() noexcept { needIncrementSparseIndexesCount_ = true; }
	void NeedMoveFloatVectorsIndexesPositions() noexcept { updateFloatVectorsIndexesPositions_ = FloatVectorsIndexsPositions::Move; }
	void NeedInsertFloatVectorsIndexesPositions() noexcept { updateFloatVectorsIndexesPositions_ = FloatVectorsIndexsPositions::Insert; }
	void NeedUpdateIndexesNames() noexcept { needUpdateIndexesNames_ = true; }
	void NeedInsertPKIndexName() noexcept { needInsertPKIndexName_ = true; }
	void NeedResetIndexFieldsSet(Index* idx, FieldsSet&& fields) { oldIndexesFieldsSets_.emplace_back(idx, std::move(fields)); }
	void NeedResetPayloadTypeInTagsMatcher(bool disableTmVersionDec) noexcept {
		needResetPayloadTypeInTagsMatcher_ = true;
		needChangeTmVersion_ = disableTmVersionDec ? NeedChangeTmVersion::No : NeedChangeTmVersion::Decrement;
	}
	void SetOldPayloadType(PayloadType&& oldPt) noexcept { oldPayloadType_.emplace(std::move(oldPt)); }
	const PayloadType& GetOldPayloadType() const noexcept {
		// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
		return *oldPayloadType_;
	}
	void RollBacker_updateItems(RollBack_updateItems<NeedRollBack::Yes>&& rb) noexcept { rollbacker_updateItems_.emplace(std::move(rb)); }
	void Disable() noexcept override {
		if (rollbacker_updateItems_) {
			rollbacker_updateItems_->Disable();
		}
		RollBackBase::Disable();
	}

private:
	NamespaceImpl& ns_;
	enum class [[nodiscard]] FloatVectorsIndexsPositions {
		No,
		Insert,
		Move
	} updateFloatVectorsIndexesPositions_{FloatVectorsIndexsPositions::No};
	bool needIncrementSparseIndexesCount_{false};
	bool needUpdateIndexesNames_{false};
	bool needInsertPKIndexName_{false};
	bool needResetPayloadTypeInTagsMatcher_{false};
	NeedChangeTmVersion needChangeTmVersion_{NeedChangeTmVersion::No};
	std::optional<PayloadType> oldPayloadType_;
	int fieldIdx_;
	std::vector<std::pair<Index*, FieldsSet>> oldIndexesFieldsSets_;
	std::optional<RollBack_updateItems<NeedRollBack::Yes>> rollbacker_updateItems_;
};

void NamespaceImpl::updateFloatVectorsIndexesPositionsInCaseOfDrop(const Index& indexToRemove, size_t positionToRemove,
																   RollBack_dropIndex& rollBacker) {
	auto it = floatVectorsIndexesPositions_.begin();
	auto end = floatVectorsIndexesPositions_.end();
	for (; it != end && *it < positionToRemove; ++it) {
	}
	if (indexToRemove.IsFloatVector()) {
		assertrx(it != end && *it == positionToRemove);
		it = floatVectorsIndexesPositions_.erase(it);
		end = floatVectorsIndexesPositions_.end();
		rollBacker.NeedInsertFloatVectorsIndexesPositions();
	} else {
		assertrx(it == end || *it > positionToRemove);
		rollBacker.NeedMoveFloatVectorsIndexesPositions();
	}
	for (; it != end; ++it) {
		--*it;
	}
}

void NamespaceImpl::verifyDropIndex(const IndexDef& index, IndexNamesMap::const_iterator idxNameIt) const {
	if (idxNameIt == indexesNames_.end()) {
		constexpr auto errMsg = "Cannot remove index '{}': doesn't exist";
		logFmt(LogError, errMsg, index.Name());
		throw Error(errParams, errMsg, index.Name());
	}
	// Check, that index to remove is not a part of float index with auto embedding
	auto embeddedIndexName = payloadType_.CheckEmbeddersAuxiliaryField(index.Name());
	if (!embeddedIndexName.empty()) {
		throw Error(errLogic, "Cannot remove index '{}': it's a part of a auto embedding logic in index '{}'", index.Name(),
					embeddedIndexName);
	}
	if (indexes_[idxNameIt->second]->Opts().IsPK() && itemsCount() > 0) {
		for (const auto& idx : indexes_) {
			if (idx->IsFloatVector()) {
				// TODO remove this after #2220
				throw Error(errLogic, "Cannot remove PK index '{}' from namespace '{}': the namespace contains float vector index '{}'",
							index.Name(), name_, idx->Name());
			}
		}
	}
}

void NamespaceImpl::dropIndex(const IndexDef& index, bool disableTmVersionInc) {
	auto itIdxName = indexesNames_.find(index.Name());

	verifyDropIndex(index, itIdxName);

	// Guard approach is a bit suboptimal, but simpler
	const auto compositesMappingGuard =
		MakeScopeGuard([this] { indexesToComposites_.clear(); }, [this] { rebuildIndexesToCompositeMapping(); });

	int fieldIdx = itIdxName->second;
	RollBack_dropIndex rollBacker{*this, fieldIdx};
	std::unique_ptr<Index>& indexToRemove = indexes_[fieldIdx];
	if (!IsComposite(indexToRemove->Type()) && indexToRemove->Opts().IsSparse()) {
		--sparseIndexesCount_;
		rollBacker.NeedIncrementSparseIndexesCount();
	}

	// Check, that index to remove is not a part of composite index
	for (int i = indexes_.firstCompositePos(); i < indexes_.totalSize(); ++i) {
		if (indexes_[i]->Fields().contains(fieldIdx)) {
			throw Error(errLogic, "Cannot remove index '{}': it's a part of a composite index '{}'", index.Name(), indexes_[i]->Name());
		}
	}

	updateFloatVectorsIndexesPositionsInCaseOfDrop(*indexToRemove, fieldIdx, rollBacker);

	for (auto& namePair : indexesNames_) {
		if (namePair.second > fieldIdx) {
			--namePair.second;
		}
	}
	rollBacker.NeedUpdateIndexesNames();

	if (indexToRemove->Opts().IsPK()) {
		indexesNames_.erase(kPKIndexName);
		rollBacker.NeedInsertPKIndexName();
	}

	// Update indexes fields refs
	for (size_t i = 0; i < indexes_.size(); ++i) {
		if (i == size_t(fieldIdx)) {
			continue;
		}
		auto& idx = *indexes_[i];
		FieldsSet fields = idx.Fields(), newFields;
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
		idx.SetFields(std::move(newFields));
		rollBacker.NeedResetIndexFieldsSet(&idx, std::move(fields));
	}

	if (!IsComposite(indexToRemove->Type())) {
		if (indexToRemove->Opts().IsSparse()) {
			tagsMatcher_.DropSparseIndex(indexToRemove->Name());
		} else {
			auto sparseIndexes = indexes_.SparseIndexes();
			{
				PayloadType oldPlType = payloadType_;
				payloadType_.Drop(index.Name());
				rollBacker.SetOldPayloadType(std::move(oldPlType));
			}
			tagsMatcher_.UpdatePayloadType(payloadType_, sparseIndexes,
										   disableTmVersionInc ? NeedChangeTmVersion::No : NeedChangeTmVersion::Increment);
			rollBacker.NeedResetPayloadTypeInTagsMatcher(disableTmVersionInc);
			rollBacker.RollBacker_updateItems(
				updateItems<NeedRollBack::Yes, FieldChangeType::Delete>(rollBacker.GetOldPayloadType(), fieldIdx));
		}
	}

	rollBacker.Disable();
	removeIndex(std::move(indexToRemove));
	indexes_.erase(indexes_.begin() + fieldIdx);
	indexesNames_.erase(itIdxName);
	indexOptimizer_.UpdateSortedIdxCount(indexes_);
	storage_.Remove(ann_storage_cache::GetStorageKey(index.Name()));
}

void NamespaceImpl::doDropIndex(const IndexDef& index, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	dropIndex(index, ctx.IsInSnapshot());

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
	if (indexDef.Opts().IsSparse()) {
		throw Error(errParams, "Composite index cannot be sparse. Use non-sparse composite instead");
	}
	for (const auto& jp : indexDef.JsonPaths()) {
		int idx;
		if (!tryGetIndexByName(jp, idx)) {
			if (!IsFullText(indexDef.IndexType())) {
				throw Error(errParams,
							"Composite indexes over non-indexed field ('{}') are not supported yet (except for full-text indexes). Create "
							"at least column index('-') over each field inside the composite index",
							jp);
			}
		} else {
			const auto& index = *indexes_[idx];
			if (index.Opts().IsFloatVector()) {
				throw Error(errParams, "Composite indexes over float vector indexed field ('{}') are not supported yet", jp);
			}
			if (index.Opts().IsSparse()) {
				throw Error(errParams, "Composite indexes over sparse indexed field ('{}') are not supported yet", jp);
			}
			if (type != IndexCompositeHash && index.IsUuid()) {
				throw Error{errParams, "Only hash index allowed on UUID field"};
			}
			if (index.Opts().IsArray() && !IsFullText(type)) {
				throw Error(errParams, "Cannot add array subindex '{}' to not fulltext composite index '{}'", jp, indexDef.Name());
			}
			if (IsComposite(index.Type())) {
				throw Error(errParams, "Cannot create composite index '{}' over the other composite '{}'", indexDef.Name(), index.Name());
			}
		}
	}
}

void NamespaceImpl::verifyEmbeddingFields(const h_vector<std::string, 1>& fields, std::string_view fieldName,
										  std::string_view action) const {
	for (const auto& field : fields) {
		int idx = 0;
		if (!tryGetIndexByName(field, idx)) {
			throw Error(errLogic, "Cannot {} index field named '{}' in namespace '{}'. Auxiliary field '{}' not found", action, fieldName,
						name_, field);
		}
		if (idx >= indexes_.firstCompositePos()) {
			throw Error(errParams,
						"Cannot {} index field named '{}' in namespace '{}'. Support for embedding only for "
						"scalar index fields. Using composite field '{}' for embedding is invalid",
						action, fieldName, name_, field);
		}
		if (indexes_[idx]->Opts().IsSparse()) {
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

void NamespaceImpl::verifyUpsertIndex(std::string_view action, const IndexDef& indexDef) const {
	const auto idxType = indexDef.IndexType();
	if (!validateIndexName(indexDef.Name(), idxType)) {
		throw Error(errParams,
					"Cannot {} index '{}' in namespace '{}'. Index name contains invalid characters. Only alphas, digits, '+' (for "
					"composite indexes only), '.', '_' and '-' are allowed",
					action, indexDef.Name(), name_);
	}
	const auto& indexDefOpts = indexDef.Opts();
	if (indexDefOpts.IsPK()) {
		if (indexDefOpts.IsArray()) {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be array", action, indexDef.Name(), name_);
		} else if (indexDefOpts.IsSparse()) {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be sparse", action, indexDef.Name(), name_);
		} else if (isStore(idxType)) {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't have '-' type", action, indexDef.Name(), name_);
		} else if (IsFullText(idxType)) {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. PK field can't be fulltext index", action, indexDef.Name(),
						name_);
		}
	}
	if ((idxType == IndexUuidHash || idxType == IndexUuidStore) && indexDefOpts.IsSparse()) {
		throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. UUID field can't be sparse", action, indexDef.Name(), name_);
	}
	if (indexDef.JsonPaths().size() > 1 && !IsComposite(idxType) && !indexDefOpts.IsArray()) {
		throw Error(errParams,
					"Cannot {} index '{}' in namespace '{}'. Scalar (non-array and non-composite) index can not have multiple JSON-paths. "
					"Use array index instead",
					action, indexDef.Name(), name_);
	}
	if (indexDef.JsonPaths().empty()) {
		throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. JSON paths array can not be empty", action, indexDef.Name(), name_);
	}
	for (const auto& jp : indexDef.JsonPaths()) {
		if (jp.empty()) {
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. JSON path can not be empty", action, indexDef.Name(), name_);
		}
	}
	if (indexDefOpts.IsFloatVector()) {
		verifyUpsertEmbedder(action, indexDef);
		if (indexesNames_.find(kPKIndexName) == indexesNames_.cend() && itemsCount() > 0) {
			// TODO remove this after #2220
			throw Error(errParams, "Cannot {} index '{}' in namespace '{}'. The namespace does not have PK index", action, indexDef.Name(),
						name_);
		}
	}
}

void NamespaceImpl::verifyUpdateIndex(const IndexDef& indexDef) {
	const auto idxNameIt = indexesNames_.find(indexDef.Name());
	const auto currentPKIt = indexesNames_.find(kPKIndexName);

	if (idxNameIt == indexesNames_.end()) {
		throw Error(errParams, "Cannot update index '{}': doesn't exist", indexDef.Name());
	}
	const auto& oldIndex = indexes_[idxNameIt->second];
	const auto& indexDefOpts = indexDef.Opts();
	if (indexDefOpts.IsPK() && !oldIndex->Opts().IsPK() && currentPKIt != indexesNames_.end()) {
		throw Error(errConflict, "Cannot add PK index '{}.{}'. Already exists another PK index - '{}'", name_, indexDef.Name(),
					indexes_[currentPKIt->second]->Name());
	}
	if (indexDefOpts.IsArray() != oldIndex->Opts().IsArray() && itemsCount() > 0) {
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
		const auto newIndex = std::unique_ptr<Index>(
			Index::New(indexDef, PayloadType{payloadType_}, createFieldsSetFromJsonPaths(indexDef), config_.cacheConfig, itemsCount()));
	} else if (indexDefOpts.IsSparse()) {
		if (indexDef.JsonPaths().size() != 1) {
			throw Error(errParams, "Sparse index must have exactly 1 JSON-path, but {} paths found for '{}'", indexDef.JsonPaths().size(),
						indexDef.Name());
		}
		FieldsSet fields;
		fields.push_back(indexDef.JsonPaths()[0]);
		const auto newSparseIndex = Index::New(indexDef, PayloadType{payloadType_}, std::move(fields), config_.cacheConfig, itemsCount());
		if (itemsCount() > 0) {
			verifyConvertSparseType(oldIndex->KeyType(), newSparseIndex->KeyType());
		}
	} else {
		const auto newIndex = std::unique_ptr<Index>(Index::New(indexDef, PayloadType(), FieldsSet(), config_.cacheConfig, itemsCount()));
		PayloadType newPlType = payloadType_;
		newPlType.Drop(indexDef.Name());
		newPlType.Add(PayloadFieldType(name_.ToLower(), *newIndex, indexDef, embeddersCache_, enablePerfCounters_));

		if (itemsCount() > 0) {
			FieldsSet changedFields{idxNameIt->second};
			verifyConvertType(oldIndex->KeyType(), newIndex->KeyType(), newPlType, changedFields);
		}
	}
	if (indexDefOpts.IsFloatVector() && currentPKIt == indexesNames_.cend() && itemsCount() > 0) {
		// TODO remove this after #2220
		throw Error(errParams, "Cannot update index '{}' in namespace '{}'. The namespace does not have PK index", indexDef.Name(), name_);
	}
}

class [[nodiscard]] NamespaceImpl::RollBack_insertIndex final : private RollBackBase {
	using IndexesNamesIt = decltype(NamespaceImpl::indexesNames_)::iterator;

public:
	RollBack_insertIndex(NamespaceImpl& ns, NamespaceImpl::IndexesStorage::iterator idxIt, int idxNo) noexcept
		: ns_{ns}, insertedIndex_{idxIt}, insertedIdxNo_{idxNo} {}
	RollBack_insertIndex(RollBack_insertIndex&&) noexcept = default;
	~RollBack_insertIndex() override { RollBack(); }
	void RollBack() noexcept {
		if (IsDisabled()) {
			return;
		}
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
			// NOLINTNEXTLINE(bugprone-empty-catch)
		} catch (...) {
		}
		if (needUpdateFloatVectorsIndexesPositions_) {
			auto& positions = ns_.floatVectorsIndexesPositions_;
			auto it = positions.begin();
			auto end = positions.end();
			for (; it != end && *it < size_t(insertedIdxNo_); ++it) {
			}
			if (it != end && *it == size_t(insertedIdxNo_)) {
				try {
					it = positions.erase(it);
					// NOLINTBEGIN(bugprone-empty-catch)
				} catch (...) {
				}
				// NOLINTEND(bugprone-empty-catch)
				end = positions.end();
			}
			for (; it != end; ++it) {
				--*it;
			}
		}
		Disable();
	}
	void PkIndexNameInserted() noexcept { pkIndexNameInserted_ = true; }
	void NeedUpdateFloatVectorsIndexesPositions() noexcept { needUpdateFloatVectorsIndexesPositions_ = true; }
	void InsertedIndexName(const IndexesNamesIt& it) noexcept { insertedIdxName_.emplace(it); }
	std::string_view IndexName() const& noexcept { return (*insertedIndex_)->Name(); }
	using RollBackBase::Disable;

	RollBack_insertIndex(const RollBack_insertIndex&) = delete;
	RollBack_insertIndex operator=(const RollBack_insertIndex&) = delete;
	RollBack_insertIndex operator=(RollBack_insertIndex&&) = delete;
	auto IndexName() const&& = delete;

private:
	NamespaceImpl& ns_;
	NamespaceImpl::IndexesStorage::iterator insertedIndex_;
	std::optional<IndexesNamesIt> insertedIdxName_;
	int insertedIdxNo_{NotSet};
	bool pkIndexNameInserted_{false};
	bool needUpdateFloatVectorsIndexesPositions_{false};
};

void NamespaceImpl::updateFloatVectorsIndexesPositionsInCaseOfInsert(size_t position, RollBack_insertIndex& rollBacker) {
	auto it = floatVectorsIndexesPositions_.begin();
	auto end = floatVectorsIndexesPositions_.end();
	for (; it != end && *it < position; ++it) {
	}
	if (indexes_[position]->IsFloatVector()) {
		it = floatVectorsIndexesPositions_.insert(it, position);
		++it;
		end = floatVectorsIndexesPositions_.end();
	}
	for (; it != end; ++it) {
		--*it;
	}
	rollBacker.NeedUpdateFloatVectorsIndexesPositions();
}

NamespaceImpl::RollBack_insertIndex NamespaceImpl::insertIndex(std::unique_ptr<Index> newIndex, int idxNo, const std::string& realName) {
	const auto isPK = newIndex->Opts().IsPK();
	const auto it = indexes_.insert(indexes_.begin() + idxNo, std::move(newIndex));
	RollBack_insertIndex rollbacker{*this, it, idxNo};

	for (auto& n : indexesNames_) {
		if (n.second >= idxNo) {
			++n.second;
		}
	}

	if (isPK) {
		if (const auto [it2, ok] = indexesNames_.emplace(kPKIndexName, idxNo); ok) {
			(void)it2;
			rollbacker.PkIndexNameInserted();
		}
	}
	if (const auto [it2, ok] = indexesNames_.emplace(realName, idxNo); ok) {
		rollbacker.InsertedIndexName(it2);
	}
	updateFloatVectorsIndexesPositionsInCaseOfInsert(idxNo, rollbacker);
	return rollbacker;
}

template <typename T>
class [[nodiscard]] NamespaceImpl::RollBack_addIndex final : private RollBackBase {
public:
	explicit RollBack_addIndex(NamespaceImpl& ns, T& remapCompositeIndexes) noexcept
		: ns_{ns}, remapCompositeIndexes_{remapCompositeIndexes} {}
	RollBack_addIndex(const RollBack_addIndex&) = delete;
	~RollBack_addIndex() override { RollBack(); }
	void RollBack() noexcept {
		if (IsDisabled()) {
			return;
		}
		if (oldPayloadType_) {
			ns_.payloadType_ = std::move(*oldPayloadType_);
		}
		if (needRemoveSparseIndex_) {
			// NOLINTNEXTLINE(bugprone-unchecked-optional-access)
			ns_.tagsMatcher_.DropSparseIndex(rollbacker_insertIndex_->IndexName());
			--ns_.sparseIndexesCount_;
		}
		if (rollbacker_updateItems_) {
			rollbacker_updateItems_->RollBack();
		}
		if (rollbacker_insertIndex_) {
			rollbacker_insertIndex_->RollBack();
		}
		if (needResetPayloadTypeInTagsMatcher_) {
			ns_.tagsMatcher_.UpdatePayloadType(ns_.payloadType_, ns_.indexes_.SparseIndexes(),
											   disableTmVersionInc_ ? NeedChangeTmVersion::No : NeedChangeTmVersion::Decrement);
		}
		Disable();
		remapCompositeIndexes_.Disable();
	}
	void RollBacker_insertIndex(RollBack_insertIndex&& rb) noexcept { rollbacker_insertIndex_.emplace(std::move(rb)); }
	void RollBacker_updateItems(RollBack_updateItems<NeedRollBack::Yes>&& rb) noexcept { rollbacker_updateItems_.emplace(std::move(rb)); }
	void Disable() noexcept override {
		if (rollbacker_insertIndex_) {
			rollbacker_insertIndex_->Disable();
		}
		if (rollbacker_updateItems_) {
			rollbacker_updateItems_->Disable();
		}
		RollBackBase::Disable();
	}
	void NeedRemoveSparseIndex() noexcept { needRemoveSparseIndex_ = true; }
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
	T& remapCompositeIndexes_;
	std::optional<PayloadType> oldPayloadType_;
	bool needRemoveSparseIndex_{false};
	bool needResetPayloadTypeInTagsMatcher_{false};
	bool disableTmVersionInc_{false};
};

void NamespaceImpl::addIndex(const IndexDef& indexDef, bool disableTmVersionInc, bool skipEqualityCheck) {
	if (bool requireTtlUpdate = false; !skipEqualityCheck && checkIfSameIndexExists(indexDef, &requireTtlUpdate)) {
		if (requireTtlUpdate) {
			auto idxNameIt = indexesNames_.find(indexDef.Name());
			assertrx(idxNameIt != indexesNames_.end());
			UpdateExpireAfter(indexes_[idxNameIt->second].get(), indexDef.ExpireAfter());
		}
		return;
	}

	const auto currentPKIndex = indexesNames_.find(kPKIndexName);
	const auto& indexName = indexDef.Name();
	// New index case. Just add
	if (currentPKIndex != indexesNames_.end() && indexDef.Opts().IsPK()) {
		throw Error(errConflict, "Cannot add PK index '{}.{}'. Already exists another PK index - '{}'", name_, indexName,
					indexes_[currentPKIndex->second]->Name());
	}

	// Guard approach is a bit suboptimal, but simpler
	auto compositesMappingGuard = MakeScopeGuard([this]() noexcept { rebuildIndexesToCompositeMapping(); });

	if (IsComposite(indexDef.IndexType())) {
		verifyCompositeIndex(indexDef);
		addCompositeIndex(indexDef);
		return;
	}

	const int idxNo = payloadType_->NumFields();
	if (idxNo >= kMaxIndexes) {
		throw Error(errConflict, "Cannot add index '{}.{}'. Too many non-composite indexes. {} non-composite indexes are allowed only",
					name_, indexName, kMaxIndexes - 1);
	}
	const JsonPaths& jsonPaths = indexDef.JsonPaths();
	RollBack_addIndex rollbacker{*this, compositesMappingGuard};
	if (indexDef.Opts().IsSparse()) {
		if (jsonPaths.size() != 1) {
			throw Error(errParams, "Sparse index must have exactly 1 JSON-path, but {} paths found for '{}':'{}'", jsonPaths.size(), name_,
						indexDef.Name());
		}
		FieldsSet fields;
		fields.push_back(jsonPaths[0]);
		TagsPath tagsPath = tagsMatcher_.path2tag(jsonPaths[0], CanAddField_True);
		assertrx(!tagsPath.empty());
		fields.push_back(std::move(tagsPath));
		auto newIndexPtr =
			Index::New(indexDef, PayloadType{payloadType_}, std::move(fields), config_.cacheConfig, itemsCount(), LogCreation_True);
		const Index& newIndexRef = *newIndexPtr;
		rollbacker.RollBacker_insertIndex(insertIndex(std::move(newIndexPtr), idxNo, indexName));
		tagsMatcher_.AddSparseIndex(newIndexRef);
		rollbacker.NeedRemoveSparseIndex();
		++sparseIndexesCount_;
		fillSparseIndex(*indexes_[idxNo], jsonPaths[0]);
	} else {
		PayloadType oldPlType = payloadType_;
		const auto sparseIndexes = indexes_.SparseIndexes();
		auto newIndex = Index::New(indexDef, PayloadType(), FieldsSet(), config_.cacheConfig, itemsCount(), LogCreation_True);
		payloadType_.Add(PayloadFieldType(name_.ToLower(), *newIndex, indexDef, embeddersCache_, enablePerfCounters_));

		rollbacker.SetOldPayloadType(std::move(oldPlType));
		tagsMatcher_.UpdatePayloadType(payloadType_, sparseIndexes,
									   disableTmVersionInc ? NeedChangeTmVersion::No : NeedChangeTmVersion::Increment);
		rollbacker.NeedResetPayloadTypeInTagsMatcher(disableTmVersionInc);
		newIndex->SetFields(FieldsSet{idxNo});
		newIndex->UpdatePayloadType(PayloadType(payloadType_));

		rollbacker.RollBacker_insertIndex(insertIndex(std::move(newIndex), idxNo, indexName));
		rollbacker.RollBacker_updateItems(updateItems<NeedRollBack::Yes, FieldChangeType::Add>(rollbacker.GetOldPayloadType(), idxNo));
	}
	indexOptimizer_.UpdateSortedIdxCount(indexes_);
	rollbacker.Disable();
}

void NamespaceImpl::fillSparseIndex(Index& index, std::string_view jsonPath) {
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (size_t rowId = 0; rowId < items_.size(); rowId++) {
		if (items_[rowId].IsFree()) {
			continue;
		}
		try {
			Payload{payloadType_, items_[rowId]}.GetByJsonPath(jsonPath, tagsMatcher_, skrefs, index.KeyType());
			krefs.resize(0);
			bool needClearCache{false};

			index.Upsert(krefs, skrefs, int(rowId), needClearCache);

			if (needClearCache) {
				indexesCacheCleaner.Add(index);
			}
		} catch (const std::exception& err) {
			throwIndexUpsertErrorWithPKInfo(ConstPayload(payloadType_, items_[rowId]), err);
		}
	}
	indexOptimizer_.ScheduleOptimization(IndexOptimization::Partial);
}

void NamespaceImpl::doAddIndex(const IndexDef& indexDef, bool skipEqualityCheck, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	addIndex(indexDef, ctx.IsInSnapshot(), skipEqualityCheck);

	addToWAL(indexDef, WalIndexAdd, ctx);
	pendedRepl.emplace_back(updates::URType::IndexAdd, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), indexDef);
}

bool NamespaceImpl::updateIndex(const IndexDef& indexDef, bool disableTmVersionInc) {
	IndexDef foundIndex = getIndexDefinition(indexDef.Name());

	if (indexDef.Compare(foundIndex).Equal()) {
		return false;
	}

	if (!IndexFastUpdate::Try(*this, foundIndex, indexDef)) {
		verifyUpdateIndex(indexDef);
		dropIndex(indexDef, disableTmVersionInc);
		addIndex(indexDef, disableTmVersionInc);
	}
	return true;
}

bool NamespaceImpl::doUpdateIndex(const IndexDef& indexDef, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (updateIndex(indexDef, ctx.IsInSnapshot()) || !ctx.GetOriginLSN().isEmpty()) {
		addToWAL(indexDef, WalIndexUpdate, ctx);
		pendedRepl.emplace_back(updates::URType::IndexUpdate, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), indexDef);
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
	throw Error(errParams, "Index '{}' not found in '{}'", indexName, name_);
}

void NamespaceImpl::addCompositeIndex(const IndexDef& indexDef) {
	assertrx_throw(indexesNames_.find(indexDef.Name()) == indexesNames_.end());

	auto fields = createFieldsSetFromJsonPaths(indexDef);
	const auto& indexName = indexDef.Name();
	const int idxPos = indexes_.size();
	auto insertIndex_rollbacker{
		insertIndex(Index::New(indexDef, PayloadType{payloadType_}, std::move(fields), config_.cacheConfig, itemsCount(), LogCreation_True),
					idxPos, indexName)};

	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (IdType rowId = 0; rowId < int(items_.size()); rowId++) {
		if (!items_[rowId].IsFree()) {
			bool needClearCache{false};
			std::ignore = indexes_[idxPos]->Upsert(Variant(items_[rowId]), rowId, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(*indexes_[idxPos]);
			}
		}
	}

	indexOptimizer_.UpdateSortedIdxCount(indexes_);
	insertIndex_rollbacker.Disable();
}

FieldsSet NamespaceImpl::createFieldsSetFromJsonPaths(const IndexDef& indexDef) {
	FieldsSet fields;
	for (const auto& jsonPath : indexDef.JsonPaths()) {
		int idx = SetByJsonPath;
		if (!tryGetScalarIndexByName(jsonPath, idx)) {
			TagsPath tagsPath = tagsMatcher_.path2tag(jsonPath, CanAddField_True);
			if (tagsPath.empty()) {
				throw Error(errLogic, "Unable to get or create json-path '{}' for composite index '{}'", jsonPath, indexDef.Name());
			}
			fields.push_back(tagsPath);
			fields.push_back(jsonPath);
		}
		fields.push_back(idx);
	}
	assertrx_throw(fields.getJsonPathsLength() == fields.getTagsPathsLength());
	return fields;
}

static bool BasicCompatibilityOnly(IndexDef::DiffResult diff) {
	// clang-format off
	return 
		diff.AllOfIsEqual(
			IndexDef::Diff::Name,
			IndexDef::Diff::IndexType,
			IndexDef::Diff::FieldType,
			IndexDef::Diff::JsonPaths,
			IndexDef::Diff::ExpireAfter,

			IndexOpts::OptsDiff::kIndexOptPK,
			IndexOpts::OptsDiff::kIndexOptSparse,
			IndexOpts::OptsDiff::kIndexOptArray,

			IndexOpts::ParamsDiff::CollateOpts,
			IndexOpts::ParamsDiff::RTreeIndexType,
			
			FloatVectorIndexOpts::Diff::Base
		);
	// clang-format on
}

bool NamespaceImpl::checkIfSameIndexExists(const IndexDef& indexDef, bool* requireTtlUpdate) const {
	auto idxNameIt = indexesNames_.find(indexDef.Name());
	if (idxNameIt != indexesNames_.end()) {
		IndexDef oldIndexDef = getIndexDefinition(indexDef.Name());
		if (oldIndexDef.IndexType() == IndexTtl && indexDef.IndexType() == IndexTtl) {
			if (requireTtlUpdate && oldIndexDef.ExpireAfter() != indexDef.ExpireAfter()) {
				*requireTtlUpdate = true;
			}
			oldIndexDef.SetExpireAfter(indexDef.ExpireAfter());
		}
		if (BasicCompatibilityOnly(indexDef.Compare(oldIndexDef))) {
			return true;
		}
		throw Error(errConflict, "Index '{}.{}' already exists with different settings", name_, indexDef.Name());
	}
	return false;
}

int NamespaceImpl::getIndexByName(std::string_view index) const {
	const auto idxIt = indexesNames_.find(index);
	if (idxIt == indexesNames_.end()) {
		throw Error(errParams, "Index '{}' not found in '{}'", index, name_);
	}
	return idxIt->second;
}

bool NamespaceImpl::tryGetIndexByName(std::string_view name, int& index) const noexcept {
	auto it = indexesNames_.find(name);
	if (it == indexesNames_.end()) {
		return false;
	}
	index = it->second;
	return true;
}

bool NamespaceImpl::tryGetIndexByNameOrJsonPath(std::string_view name, int& index, EnableMultiJsonPath multi) const {
	if (tryGetIndexByName(name, index)) {
		return true;
	}
	return tryGetIndexByJsonPath(name, index, multi);
}

bool NamespaceImpl::tryGetIndexByJsonPath(std::string_view name, int& index, EnableMultiJsonPath multi) const noexcept {
	// regular indexes handling
	auto idx = payloadType_.FieldByJsonPath(name);
	if (idx > 0) {
		if (!multi && payloadType_.Field(idx).JsonPaths().size() > 1) {
			return false;
		}
		index = idx;
		return true;
	}

	// sparse indexes handling
	auto field = tagsMatcher_.tags2field(tagsMatcher_.path2tag(name));
	assertrx_dbg(!field.IsIndexed() || field.IsSparse());
	if (field.IsIndexed() && field.IsSparse()) {
		auto& idxData = tagsMatcher_.SparseIndex(field.SparseNumber());
		if (!multi && idxData.paths.size() > 1) {
			return false;
		}
		return tryGetIndexByName(idxData.name, index);
	}
	return false;
}

bool NamespaceImpl::tryGetScalarIndexByName(std::string_view name, int& index) const noexcept {
	int idx = 0;
	if (tryGetIndexByName(name, idx)) {
		if (idx < indexes_.firstCompositePos()) {
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

void NamespaceImpl::doDelete(IdType id, TransactionContext* txCtx) {
	assertrx(items_.exists(id));

	Payload pl(payloadType_, items_[id]);

	WrSerializer pk;
	pk << kRxStorageItemPrefix;
	pl.SerializeFields(pk, pkFields());

	repl_.dataHash ^= calculateItemHash(id);
	std::ignore = wal_.Set(WALRecord(), items_[id].GetLSN(), false);

	storage_.Remove(pk.Slice());

	// erase last item
	int field = 0;

	// erase from composite indexes
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	for (field = indexes_.firstCompositePos(); field < indexes_.totalSize(); ++field) {
		// No txCtx modification required for composite indexes

		bool needClearCache{false};
		indexes_[field]->Delete(Variant(items_[id]), id, MustExist_True, *strHolder_, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(*indexes_[field]);
		}
	}

	// Holder for tuple. It is required for sparse indexes will be valid
	VariantArray tupleHolder;
	pl.Get(0, tupleHolder);

	// Deleting fields from dense and sparse indexes: we start with 1st index (not index 0) because
	// changing cjson of sparse index changes entire payload value (and not only 0 item)
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
			pl.Get(field, skrefs, Variant::hold);
		} else {
			pl.Get(field, skrefs);
		}

		// Data in vector multithreading transactions are not indexed, so they should not be deleted from the index
		const bool IsVectorMTTxItem = txCtx && index.IsSupportMultithreadTransactions() && txCtx->Delete(field, id);
		if (!IsVectorMTTxItem) {
			// Delete value from index
			bool needClearCache{false};
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
	markUpdated(IndexOptimization::Full);
}

void NamespaceImpl::removeIndex(std::unique_ptr<Index>&& idx) {
	if (idx->HoldsStrings() && (!strHoldersWaitingToBeDeleted_.empty() || !strHolder_.unique())) {
		strHolder_->Add(std::move(idx));
	}
}

void NamespaceImpl::doTruncate(UpdatesContainer& pendedRepl, const NsContext& ctx) {
	const bool storageIsValid = storage_.IsValid();
	for (IdType id = 0, sz = items_.size(); id < sz; ++id) {
		auto& pv = items_[id];
		if (pv.IsFree()) {
			continue;
		}
		std::ignore = wal_.Set(WALRecord(), pv.GetLSN(), false);
		if (storageIsValid) {
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
		if (indexes_[i]->IsFloatVector()) {
			storage_.Remove(ann_storage_cache::GetStorageKey(indexes_[i]->Name()));
			annStorageCacheState_.Remove(indexes_[i]->Name());
		}
		const IndexOpts opts = indexes_[i]->Opts();
		std::unique_ptr<Index> newIdx{Index::New(getIndexDefinition(i), PayloadType{indexes_[i]->GetPayloadType()},
												 FieldsSet{indexes_[i]->Fields()}, config_.cacheConfig, itemsCount())};
		newIdx->SetOpts(opts);
		std::swap(indexes_[i], newIdx);
		removeIndex(std::move(newIdx));
	}

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

	CounterGuardAIR32 cg(cancelCommitCnt_);
	auto wlck = dataWLock(rdxCtx);
	cg.Reset();
	calc.LockHit();
	if (mode == ModeDelete && item.PkFields() != pkFields()) [[unlikely]] {
		throw Error(errNotValid, "Item has outdated PK metadata (probably PK has been change during the Delete-call)");
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
	state.dataHash = repl_.dataHash;
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
	return LocalTransaction(name_, payloadType_, tagsMatcher_, pkFields(), schema_, ctx.GetOriginLSN());
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
		const auto expectedInsertionsCount = tx.ExpectedInsertionsCount();
		for (auto& idx : indexes_) {
			idx->GrowFor(expectedInsertionsCount);
		}
		txCtxPtr = &txCtx;
	}
	{
		// Insert data in concurrent vector indexes in ScopeGuard
		TransactionConcurrentInserter mtInserter(*this, annInsertionThreads);
		auto mtInsertGuard = MakeScopeGuard([&mtInserter, txCtxPtr] {
			if (txCtxPtr) {
				mtInserter(*txCtxPtr);
			}
		});
		{
			WALRecord initWrec(WalInitTransaction, 0, true);
			auto lsn = wal_.Add(initWrec, tx.GetLSN());
			if (!ctx.IsInSnapshot()) {
				replicateAsync({updates::URType::BeginTx, name_, lsn, repl_.nsVersion, ctx.EmitterServerId()}, ctx.rdxContext);
			}
		}

		AsyncStorage::AdviceGuardT storageAdvice;
		if (tx.GetSteps().size() >= AsyncStorage::kLimitToAdviceBatching) {
			storageAdvice = storage_.AdviceBatching();
		}

		result.addNSContext(payloadType_, tagsMatcher_, FieldsFilter(), schema_, incarnationTag_);

		for (auto&& step : tx.GetSteps()) {
			UpdatesContainer pendedRepl;
			switch (step.type_) {
				case TransactionStep::Type::ModifyItem: {
					const auto mode = std::get<TransactionItemStep>(step.data_).mode;
					const auto lsn = step.lsn_;
					Item item = tx.GetItem(std::move(step));
					modifyItem(item, mode, pendedRepl, NsContext(ctx).InTransaction(lsn, txCtxPtr));
					result.AddItemNoHold(item, incarnationTag_);
					break;
				}
				case TransactionStep::Type::Query: {
					LocalQueryResults qr;
					qr.AddNamespace(this, true);
					auto& data = std::get<TransactionQueryStep>(step.data_);
					if (data.query->type_ == QueryDelete) {
						doDeleteTr(qr, pendedRepl, *data.query, NsContext(ctx).InTransaction(step.lsn_, txCtxPtr));
					} else {
						doUpdateTr(qr, pendedRepl, *data.query, NsContext(ctx).InTransaction(step.lsn_, txCtxPtr));
					}
					for (const auto& it : qr.Items()) {
						result.AddItemRef(it.GetItemRef().Id(), PayloadValue());
					}
					break;
				}
				case TransactionStep::Type::Nop:
					assertrx(ctx.IsInSnapshot());
					// NOLINTNEXTLINE (bugprone-unused-return-value)
					wal_.Add(WALRecord(WalEmpty), step.lsn_);
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

			if (!ctx.IsInSnapshot()) {
				replicateAsync(std::move(pendedRepl), ctx.rdxContext);
			}
		}

		processWalRecord(WALRecord(WalCommitTransaction, 0, true), ctx);
		logFmt(LogTrace, "[repl:{}]:{} CommitTransaction end", name_, wal_.GetServer());
		if (!ctx.IsInSnapshot() && !ctx.isCopiedNsRequest) {
			// If commit happens in ns copy, then the copier have to handle replication
			UpdatesContainer pendedRepl;
			pendedRepl.emplace_back(updates::URType::CommitTx, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId());
			replicate(std::move(pendedRepl), std::move(wlck), true, queryStatCalculator, ctx);
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
	Payload pl(payloadType_, plData);

	Payload plNew = item.GetPayload();
	auto indexesCacheCleaner{GetIndexesCacheCleaner()};
	Variant oldData;
	h_vector<bool, 32> needUpdateCompIndexes;
	if (doUpdate) {
		repl_.dataHash ^= calculateItemHash(id);
		itemsDataSize_ -= plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
		plData.Clone(pl.RealSize());
		const size_t compIndexesCount = indexes_.compositeIndexesSize();
		needUpdateCompIndexes = h_vector<bool, 32>(compIndexesCount, false);
		bool needUpdateAnyCompIndex = false;
		for (size_t field = 0; field < compIndexesCount; ++field) {
			const auto& fields = indexes_[field + indexes_.firstCompositePos()]->Fields();
			for (const auto f : fields) {
				if (f == IndexValueType::SetByJsonPath) {
					continue;
				}
				pl.Get(f, skrefs);
				plNew.Get(f, krefs);
				if (skrefs != krefs) {
					needUpdateCompIndexes[field] = true;
					needUpdateAnyCompIndex = true;
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

	plData.SetLSN(item.Value().GetLSN());

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
		const auto isIndexSparse = index.Opts().IsSparse();
		if (isIndexSparse) {
			assertrx(index.Fields().getTagsPathsLength() > 0);
			try {
				plNew.GetByJsonPath(index.Fields().getTagsPath(0), skrefs, index.KeyType());
			} catch (const Error& e) {
				logFmt(LogInfo, "[{}]:{} Unable to index sparse value (index name: '{}'): '{}'", name_, wal_.GetServer(), index.Name(),
					   e.what());
				skrefs.resize(0);
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
				} catch (const Error& e) {
					logFmt(LogInfo, "[{}]:{} Unable to remove sparse value from the index (index name: '{}'): '{}'", name_,
						   wal_.GetServer(), index.Name(), e.what());
					krefs.resize(0);
				}
			} else if (index.Opts().IsArray()) {
				pl.Get(field, krefs, Variant::hold);
			} else if (index.Opts().IsFloatVector()) {
				std::optional<ConstFloatVectorView> fvView;
				if (isMultithreadTxInsertion) {
					fvView = txCtx->TryGetValue(field, id);
				}
				if (!fvView.has_value()) {
					const FloatVectorIndex& fvIdx = static_cast<FloatVectorIndex&>(index);
					fvView = fvIdx.GetFloatVectorView(id);
				}
				krefs.clear<false>();
				krefs.emplace_back(fvView.value());
			} else {
				pl.Get(field, krefs);
			}
			if (krefs == skrefs) {
				// Do not modify indexes, if documents content was not changed
				continue;
			}

			if (!isMultithreadTxInsertion || !txCtx->TryGetValue(field, id).has_value()) {
				// No txCtx modification required here
				bool needClearCache{false};
				index.Delete(krefs, id, MustExist_True, *strHolder_, needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(index);
				}
			}
		}
		if (isMultithreadTxInsertion) {
			ConstFloatVectorView view;
			if (!skrefs.empty()) {
				assertrx(skrefs.size() == 1);
				assertrx(skrefs[0].Type().Is<KeyValueType::FloatVector>());
				view = ConstFloatVectorView{skrefs[0]};
			}
			assertrx(!isIndexSparse);
			auto vec = txCtx->Upsert(field, id, view);
			pl.Set(field, Variant{vec, Variant::noHold});
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
	for (int field2 = indexes_.firstCompositePos(); field2 < indexes_.totalSize(); ++field2) {
		// No txCtx modification required for composite indexes

		bool needClearCache{false};
		if (doUpdate) {
			if (!needUpdateCompIndexes[field2 - indexes_.firstCompositePos()]) {
				continue;
			}
			// Delete from composite indexes first
			indexes_[field2]->Delete(oldData, id, MustExist_True, *strHolder_, needClearCache);
		}
		std::ignore = indexes_[field2]->Upsert(Variant{plData}, id, needClearCache);
		if (needClearCache) {
			indexesCacheCleaner.Add(*indexes_[field2]);
		}
	}
	repl_.dataHash ^= calculateItemHash(id);
	itemsDataSize_ += plData.GetCapacity() + sizeof(PayloadValue::dataHeader);
	item.RealValue() = plData;
}

void NamespaceImpl::updateTagsMatcherFromItem(ItemImpl* ritem, const NsContext& ctx) {
	if (ritem->tagsMatcher().isUpdated()) {
		logFmt(LogTrace, "Updated TagsMatcher of namespace '{}' on modify:\n{}", name_, ritem->tagsMatcher().Dump());
		if (!ctx.GetOriginLSN().isEmpty()) {
			throw Error(errLogic, "{}: Replicated item requires explicit tagsmatcher update: {}", name_, ritem->GetJSON());
		}
	}
	if (ritem->Type().get() != payloadType_.get() || (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher()))) {
		std::string jsonSliceBuf(ritem->GetJSON());
		logFmt(LogTrace, "Conflict TagsMatcher of namespace '{}' on modify: item:\n{}\ntm is\n{}\nnew tm is\n {}\n", name_, jsonSliceBuf,
			   tagsMatcher_.Dump(), ritem->tagsMatcher().Dump());

		ItemImpl tmpItem(payloadType_, tagsMatcher_);
		tmpItem.Value().SetLSN(ritem->Value().GetLSN());
		*ritem = std::move(tmpItem);

		auto err = ritem->FromJSON(jsonSliceBuf, nullptr);
		if (!err.ok()) {
			throw err;
		}

		if (ritem->tagsMatcher().isUpdated() && !tagsMatcher_.try_merge(ritem->tagsMatcher())) {
			throw Error(errLogic, "Could not insert item. TagsMatcher was not merged.");
		}
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

	auto itItem = findByPK(ritem, ctx.IsInTransaction(), ctx.rdxContext);
	IdType id = itItem.first;

	item.setID(-1);
	if (itItem.second || ctx.IsWalSyncItem()) {
		item.setID(id);

		WrSerializer cjson;
		WALRecord wrec{WalItemModify, ritem->GetCJSON(cjson, false), ritem->tagsMatcher().version(), ModeDelete, ctx.IsInTransaction()};

		if (itItem.second) {
			ritem->RealValue() = items_[id];
			if (!ctx.txCtx) {
				if (const auto& fvIndexes = getVectorIndexes(); !fvIndexes.empty()) {
					ritem->RealValue().Clone();
					Payload pl(payloadType_, ritem->RealValue());
					for (const auto& fvIdx : fvIndexes) {
						if (ritem->floatVectorsHolder_.Add(fvIdx.ptr->GetFloatVector(id))) {
							pl.Set(fvIdx.ptField, Variant(ritem->floatVectorsHolder_.Back()));
						}
					}
				}
			}
			doDelete(id, ctx.txCtx);
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
	const auto oldTmV = tagsMatcher_.version();
	ItemImpl* itemImpl = item.impl_;
	setFieldsBasedOnPrecepts(itemImpl, pendedRepl, ctx);
	updateTagsMatcherFromItem(itemImpl, ctx);
	auto newPl = itemImpl->GetPayload();

	auto realItem = findByPK(itemImpl, ctx.IsInTransaction(), ctx.rdxContext);
	const bool exists = realItem.second;

	if ((exists && mode == ModeInsert) || (!exists && mode == ModeUpdate)) {
		item.setID(-1);
		replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
		return;
	}

	// Validate strings encoding
	for (int field = 1, regularIndexes = indexes_.firstCompositePos(); field < regularIndexes; ++field) {
		const Index& index = *indexes_[field];
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

	if (suggestedId >= 0 && exists && suggestedId != realItem.first) {
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
	std::ignore = itemImpl->GetCJSON(cjson, false);

	saveTagsMatcherToStorage(true);
	if (storage_.IsValid()) {
		WrSerializer pk, data;
		pk << kRxStorageItemPrefix;
		newPl.SerializeFields(pk, pkFields());
		data.PutUInt64(int64_t(lsn));
		data.Write(cjson.Slice());
		storage_.Write(pk.Slice(), data.Slice());
	}

	markUpdated(exists ? IndexOptimization::Partial : IndexOptimization::Full);

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
	} else {
		cpl.Get(fieldNum, keys);
	}
	return keys;
}

std::pair<Index*, int> NamespaceImpl::getPkIdx() const noexcept {
	auto pkIndexIt = indexesNames_.find(kPKIndexName);
	if (pkIndexIt == indexesNames_.end()) {
		return std::pair<Index*, int>(nullptr, -1);
	}
	return std::make_pair(indexes_[pkIndexIt->second].get(), pkIndexIt->second);
}

RX_ALWAYS_INLINE SelectKeyResult NamespaceImpl::getPkDocs(const ConstPayload& cpl, bool inTransaction, const RdxContext& ctx) {
	auto [pkIndex, pkField] = getPkIdx();
	if (!pkIndex) {
		throw Error(errLogic, "Trying to modify namespace '{}', but it doesn't contain PK index", name_);
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
	if (!res.empty() && !res[0].ids_.empty()) {
		return {res[0].ids_[0], true};
	}
	return {-1, false};
}

void NamespaceImpl::checkUniquePK(const ConstPayload& cpl, bool inTransaction, const RdxContext& ctx) {
	SelectKeyResult res = getPkDocs(cpl, inTransaction, ctx);
	if (!res.empty() && res[0].ids_.size() > 1) {
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
	// This is read lock only, atomics-based implementation of background indexes optimization.
	// If indexOptimizer_.State() == OptimizationState::Completed, then indexes are completely built.
	// If indexOptimizer_.State() == OptimizationState::Error, then indexes can not be optimized for some unexpected reason.
	if (!indexOptimizer_.IsOptimizationAvailable()) {
		return;
	}

	Locker::RLockT rlck;
	if (!ctx.isCopiedNsRequest) {
		rlck = rLock(ctx.rdxContext);
	}
	if (isSystem() || isTemporary()) {
		return;
	}

	indexOptimizer_.TryOptimize(
		IndexOptimizer::Context{.nsName = name_,
								.enablePerfCounters = enablePerfCounters_.load(),
								.skipTimeCheck = ctx.isCopiedNsRequest,
								.lastUpdateTime = std::chrono::milliseconds(lastUpdateTime_.load(std::memory_order_acquire)),
								.indexes = indexes_,
								.items = items_},
		[this] { return cancelCommitCnt_.load(std::memory_order_relaxed) || dbDestroyed_.load(std::memory_order_relaxed); });
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

void NamespaceImpl::doUpdateTr(LocalQueryResults& result, UpdatesContainer& pendedRepl, const Query& query, const NsContext& ctx) {
	NsSelecter selecter(this);
	SelectCtxWithJoinPreSelect selCtx(query, nullptr, nullptr);
	FtFunctionsHolder func;
	selCtx.functions = &func;
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.IsInTransaction();
	selCtx.selectBeforeUpdate = true;
	selecter(result, selCtx, ctx.rdxContext);
	doUpdate(result, pendedRepl, query, ctx);
}

void NamespaceImpl::doUpdate(LocalQueryResults& result, UpdatesContainer& pendedRepl, const Query& query, const NsContext& ctx) {
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

	if (!ctx.GetOriginLSN().isEmpty() && withExpressions) {
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

	ItemModifier itemModifier(query.UpdateFields(), *this, pendedRepl, ctx);
	pendedRepl.reserve(pendedRepl.size() + result.Count());	 // Required for item-based replication only
	for (auto& it : result) {
		ItemRef& item = it.GetItemRef();
		assertrx(items_.exists(item.Id()));
		const auto oldTmV = tagsMatcher_.version();
		PayloadValue& pv(items_[item.Id()]);
		Payload pl(payloadType_, pv);

		const uint64_t oldItemHash = calculateItemHash(item.Id());
		size_t oldItemCapacity = pv.GetCapacity();
		const bool isPKModified = itemModifier.Modify(item.Id(), ctx, pendedRepl);
		std::optional<PKModifyRevertData> modifyData;
		if (isPKModified) {
			// statementReplication = false;
			modifyData.emplace(itemModifier.GetPayloadValueBackup(), item.Value().GetLSN());
		}

		replicateItem(item.Id(), ctx, statementReplication, oldItemHash, oldItemCapacity, oldTmV, std::move(modifyData), pendedRepl);
		item.Value() = items_[item.Id()];
	}
	result.getTagsMatcher(0) = tagsMatcher_;
	result.GetFloatVectorsHolder().Add(*this, result.begin(), result.end(), FieldsFilter::AllFields());
	assertrx(result.IsNamespaceAdded(this));

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

	if (query.GetDebugLevel() >= LogInfo) {
		logFmt(LogInfo, "Updated {} items in {} µs", result.Count(), duration_cast<microseconds>(system_clock_w::now() - tmStart).count());
	}
	//	if (statementReplication) {
	//		assertrx(!lsn.isEmpty());
	//		pendedRepl.emplace_back(UpdateRecord::Type::UpdateQuery, name_, lsn, query);
	//	}
}

void NamespaceImpl::replicateItem(IdType itemId, const NsContext& ctx, bool statementReplication, uint64_t oldItemHash,
								  size_t oldItemCapacity, int oldTmVersion, std::optional<PKModifyRevertData>&& modifyData,
								  UpdatesContainer& pendedRepl) {
	PayloadValue& pv(items_[itemId]);
	Payload pl(payloadType_, pv);
	const auto vectorIndexes = getVectorIndexes();

	if (!statementReplication) {
		replicateTmUpdateIfRequired(pendedRepl, oldTmVersion, ctx);
		auto sendWalUpdate = [this, itemId, &ctx, &pv, &pendedRepl, &vectorIndexes](updates::URType mode) {
			lsn_t lsn;
			if (ctx.IsForceSyncItem()) {
				lsn = ctx.GetOriginLSN();
			} else {
				lsn = wal_.Add(WALRecord(WalItemUpdate, itemId, ctx.IsInTransaction()), lsn_t(), items_[itemId].GetLSN());
			}
			assertrx(!lsn.isEmpty());

			pv.SetLSN(lsn);
			ItemImpl item(payloadType_, pv, tagsMatcher_);
			item.Unsafe(true);
			item.CopyIndexedVectorsValuesFrom(itemId, vectorIndexes);
			WrSerializer cjson;
			std::ignore = item.GetCJSON(cjson, false);
			pendedRepl.emplace_back(mode, name_, lsn, repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
		};

		if (modifyData.has_value()) {
			ItemImpl itemSave(payloadType_, modifyData->pv, tagsMatcher_);
			itemSave.Unsafe(true);
			itemSave.CopyIndexedVectorsValuesFrom(itemId, vectorIndexes);
			WrSerializer cjson;
			std::ignore = itemSave.GetCJSON(cjson, false);
			processWalRecord(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete, ctx.IsInTransaction()), ctx,
							 modifyData->lsn);
			pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::ItemDeleteTx : updates::URType::ItemDelete, name_,
									wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
			sendWalUpdate(ctx.IsInTransaction() ? updates::URType::ItemInsertTx : updates::URType::ItemInsert);
		} else {
			sendWalUpdate(ctx.IsInTransaction() ? updates::URType::ItemUpdateTx : updates::URType::ItemUpdate);
		}
	}

	repl_.dataHash ^= oldItemHash;
	repl_.dataHash ^= calculateItemHash(itemId);
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
		item.Unsafe(true);
		item.CopyIndexedVectorsValuesFrom(itemId, vectorIndexes);
		storage_.Write(pk.Slice(), item.GetCJSON(data));
	}
}

void NamespaceImpl::doDeleteTr(LocalQueryResults& result, UpdatesContainer& pendedRepl, const Query& query, const NsContext& ctx) {
	NsSelecter selecter(this);
	SelectCtxWithJoinPreSelect selCtx(query, nullptr, &result.GetFloatVectorsHolder());
	selCtx.contextCollectingMode = true;
	selCtx.requiresCrashTracking = true;
	selCtx.inTransaction = ctx.IsInTransaction();
	selCtx.selectBeforeUpdate = true;
	FtFunctionsHolder func;
	selCtx.functions = &func;
	selecter(result, selCtx, ctx.rdxContext);
	assertrx(result.IsNamespaceAdded(this));
	doDelete(result, pendedRepl, query, ctx);
}

void NamespaceImpl::doDelete(LocalQueryResults& result, UpdatesContainer& pendedRepl, const Query& query, const NsContext& ctx) {
	ActiveQueryScope queryScope(query, QueryDelete, indexOptimizer_.StateRef(), strHolder_.get());
	const auto tmStart = system_clock_w::now();
	const auto oldTmV = tagsMatcher_.version();
	for (const auto& it : result.Items()) {
		doDelete(it.GetItemRef().Id(), ctx.txCtx);
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
		auto err = it.GetCJSON(cjson, false);
		assertf(err.ok(), "Unable to get CJSON after Delete-query: '{}'", err.what());
		(void)err;	// There are no good ways to handle this error
		processWalRecord(WALRecord(WalItemModify, cjson.Slice(), tagsMatcher_.version(), ModeDelete, ctx.IsInTransaction()), ctx,
						 it.GetLSN());
		pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::ItemDeleteTx : updates::URType::ItemDelete, name_, wal_.LastLSN(),
								repl_.nsVersion, ctx.EmitterServerId(), std::move(cjson));
	}
	// }
	if (query.GetDebugLevel() >= LogInfo) {
		logFmt(LogInfo, "Deleted {} items in {} µs", result.Count(), duration_cast<microseconds>(system_clock_w::now() - tmStart).count());
	}
}

void NamespaceImpl::checkClusterRole(lsn_t originLsn) const {
	switch (repl_.clusterStatus.role) {
		case ClusterOperationStatus::Role::None:
			if (!originLsn.isEmpty()) {
				throw Error(errWrongReplicationData, "Can't modify ns '{}' with 'None' replication status from node {}", name_,
							originLsn.Server());
			}
			break;
		case ClusterOperationStatus::Role::SimpleReplica:
			if (originLsn.isEmpty()) {
				throw Error(errWrongReplicationData, "Can't modify replica's ns '{}' without origin LSN", name_, originLsn.Server());
			}
			break;
		case ClusterOperationStatus::Role::ClusterReplica:
			if (originLsn.isEmpty() || originLsn.Server() != repl_.clusterStatus.leaderId) {
				throw Error(errWrongReplicationData, "Can't modify cluster ns '{}' with incorrect origin LSN: ({}) (s1:{} s2:{})", name_,
							originLsn, originLsn.Server(), repl_.clusterStatus.leaderId);
			}
			break;
	}
}

void NamespaceImpl::checkClusterStatus(lsn_t originLsn) const {
	checkClusterRole(originLsn);
	if (!originLsn.isEmpty() && wal_.LSNCounter() != originLsn.Counter()) {
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
	if (oldTmVersion != tagsMatcher_.version()) {
		assertrx(ctx.GetOriginLSN().isEmpty());
		const auto lsn = wal_.Add(WALRecord(WalEmpty, 0, ctx.IsInTransaction()), lsn_t());
		pendedRepl.emplace_back(ctx.IsInTransaction() ? updates::URType::SetTagsMatcherTx : updates::URType::SetTagsMatcher, name_, lsn,
								repl_.nsVersion, ctx.EmitterServerId(), tagsMatcher_);
	}
}

template <typename JoinPreResultCtx>
void NamespaceImpl::Select(LocalQueryResults& result, SelectCtxWithJoinPreSelect<JoinPreResultCtx>& params, const RdxContext& ctx) {
	if (!params.query.IsWALQuery()) {
		NsSelecter selecter(this);
		selecter(result, params, ctx);
	} else {
		WALSelecter selecter(this, true);
		selecter(result, params);
	}
}
template void NamespaceImpl::Select(LocalQueryResults&, SelectCtxWithJoinPreSelect<void>&, const RdxContext&);
template void NamespaceImpl::Select(LocalQueryResults&, SelectCtxWithJoinPreSelect<JoinPreResultBuildCtx>&, const RdxContext&);
template void NamespaceImpl::Select(LocalQueryResults&, SelectCtxWithJoinPreSelect<JoinPreResultExecuteCtx>&, const RdxContext&);

IndexDef NamespaceImpl::getIndexDefinition(size_t i) const {
	assertrx(i < indexes_.size());
	const Index& index = *indexes_[i];

	if (index.Opts().IsSparse() || static_cast<int>(i) >= payloadType_.NumFields()) {
		int fIdx = 0;
		JsonPaths jsonPaths;
		for (auto& f : index.Fields()) {
			if (f != IndexValueType::SetByJsonPath) {
				jsonPaths.push_back(indexes_[f]->Name());
			} else {
				jsonPaths.push_back(index.Fields().getJsonPath(fIdx++));
			}
		}
		return {index.Name(), std::move(jsonPaths), index.Type(), index.Opts(), index.GetTTLValue()};
	} else {
		return {index.Name(), payloadType_.Field(i).JsonPaths(), index.Type(), index.Opts(), index.GetTTLValue()};
	}
}

NamespaceDef NamespaceImpl::getDefinition() const {
	NamespaceDef nsDef(std::string(name_), StorageOpts().Enabled(storage_.GetStatusCached().isEnabled));
	nsDef.indexes.reserve(indexes_.size());
	for (size_t i = 1; i < indexes_.size(); ++i) {
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
	ret.indexes.reserve(indexes_.size());
	for (const auto& idx : indexes_) {
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

	ret.tagsMatcher.tagsCount = tagsMatcher_.size();
	ret.tagsMatcher.version = tagsMatcher_.version();
	ret.tagsMatcher.stateToken = tagsMatcher_.stateToken();

	logFmt(LogTrace, "[GetMemStat:{}]:{} replication (dataHash={}  dataCount={}  lastLsn={})", ret.name, wal_.GetServer(),
		   ret.replication.dataHash, ret.replication.dataCount, ret.replication.lastLsn);

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
	ret.indexes.reserve(indexes_.size() - 1);
	for (unsigned i = 1; i < indexes_.size(); i++) {
		ret.indexes.emplace_back(indexes_[i]->GetIndexPerfStat());
	}
	return ret;
}

void NamespaceImpl::ResetPerfStat(const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	selectPerfCounter_.Reset();
	updatePerfCounter_.Reset();
	for (auto& i : indexes_) {
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
		if (!status.ok() && status.code() != errNotFound) {
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
		if (!status.ok() && status.code() != errNotFound) {
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
	assertrx(indexes_.size() == 1);
	assertrx(items_.empty());

	std::string def;
	Error status = loadLatestSysRecord(kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, def);
	if (!status.ok() && status.code() != errNotFound) {
		throw status;
	}
	if (!def.empty()) {
		Serializer ser(def.data(), def.size());
		tagsMatcher_.deserialize(ser);
		tagsMatcher_.clearUpdated();
		logFmt(LogInfo, "[tm:{}]:{}: TagsMatcher was loaded from storage. tm: {{ state_token: {:#08x}, version: {} }}", name_,
			   wal_.GetServer(), tagsMatcher_.stateToken(), tagsMatcher_.version());
		logFmt(LogTrace, "Loaded tags(version: {}) of namespace {}:\n{}",
			   sysRecordsVersions_.tagsVersion ? sysRecordsVersions_.tagsVersion - 1 : 0, name_, tagsMatcher_.Dump());
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
		auto err = schema_->BuildProtobufSchema(tagsMatcher_, payloadType_);
		if (!err.ok()) {
			logFmt(LogInfo, "Unable to build protobuf schema for the '{}' namespace: {}", name_, err.what());
		}
	}

	logFmt(LogTrace, "Loaded index structure(version {}) of namespace '{}'\n{}",
		   sysRecordsVersions_.idxVersion ? sysRecordsVersions_.idxVersion - 1 : 0, name_, payloadType_->ToString());

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
		WrSerializer ser_log;
		JsonBuilder builder_log(ser_log, ObjType::TypePlain);
		repl_.GetJSON(builder_log);
		logFmt(LogTrace, "[load_repl:{}]:{} Loading replication state {}", name_, wal_.GetServer(), ser_log.c_str());
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
	if (storage_.IsValid() && tagsMatcher_.isUpdated()) {
		WrSerializer ser;
		ser.PutUInt64(sysRecordsVersions_.tagsVersion);
		tagsMatcher_.serialize(ser);
		if (clearUpdate) {	// Update flags should be cleared after some items updates (to replicate tagsmatcher with WALItemModify record)
			tagsMatcher_.clearUpdated();
		}
		writeSysRecToStorage(ser.Slice(), kStorageTagsPrefix, sysRecordsVersions_.tagsVersion, false);
		logFmt(LogTrace, "Saving tags of namespace {}:\n{}", name_, tagsMatcher_.Dump());
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
			if (!opts.IsCreateIfMissing() && !storageDirExists) {
				throw Error(errNotFound,
							"Storage directory doesn't exist for namespace '{}' on path '{}' and CreateIfMissing option is not set", name_,
							path);
			}
			Error status = storage_.Open(storageType, name_, dbpath, opts);
			if (!status.ok()) {
				if (!opts.IsDropOnFileFormatError()) {
					storage_.Close();
					throw Error(errLogic, "Cannot enable storage for namespace '{}' on path '{}' - {}", name_, path, status.what());
				}
			} else {
				success = loadIndexesFromStorage();
				if (!success && !opts.IsDropOnFileFormatError()) {
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
			} else {
				if (fs::RmDirAll(dbpath) != 0) {
					logFmt(LogError, "Failed to remove folder {}, error : {}", dbpath, strerror(errno));
				}
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

Error NamespaceImpl::SetClusterOperationStatus(ClusterOperationStatus&& status, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	if (isTemporary() && status.role == ClusterOperationStatus::Role::None) {
		return {errParams, "Unable to set replication role 'none' to temporary namespace"};
	}
	repl_.clusterStatus = std::move(status);
	saveReplStateToStorage(true);
	return {};
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

	auto wlck = dataWLock(rdxCtx);
	setTagsMatcher(std::move(tm), pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), true, nullptr, ctx);
}

FloatVectorsIndexes NamespaceImpl::getVectorIndexes() const {
	FloatVectorsIndexes result;
	for (size_t i = 0, count = size_t(payloadType_.NumFields()); i < count; ++i) {
		const auto& field = payloadType_.Field(i);
		if (!field.IsFloatVector()) {
			continue;
		}
		const auto indexId = getIndexByName(field.Name());
		if (auto idx = dynamic_cast<FloatVectorIndex*>(indexes_[indexId].get()); idx) {
			result.emplace_back(FloatVectorIndexData{.ptField = i, .ptr = idx});
		} else {
			throw Error(errParams, "Incorrect payload type for '{}', index '{}' must have vector type", name_, field.Name());
		}
	}
	return result;
}

FloatVectorsIndexes NamespaceImpl::getVectorIndexes(const PayloadType& pt) const {
	FloatVectorsIndexes result;
	if (!iequals(pt.Name(), payloadType_.Name())) [[unlikely]] {
		throw Error(errParams, "Attempt to get vector indexes for incorrect payload type. Expected name is '{}', actual name is '{}'",
					payloadType_.Name(), pt.Name());
	}
	for (size_t i = 0, total = size_t(pt.NumFields()); i < total; ++i) {
		auto& field = pt.Field(i);
		if (!field.IsFloatVector()) {
			continue;
		}
		const std::string& fieldName = field.Name();
		auto indexIt = std::ranges::find_if(indexes_, [&fieldName](const auto& idx) noexcept { return idx->Name() == fieldName; });
		if (indexIt == indexes_.end()) {
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

	if (!isTemporary()) {
		assertrx_dbg(false);
		throw Error(errLogic, "Unexpected manual free items rebuild on non-temporary namespace");
	}
	for (IdType i = 0, sz = IdType(items_.size()); i < sz; ++i) {
		if (items_[i].IsFree()) {
			newFree.emplace_back(i);
		}
	}
	free_ = std::move(newFree);
}

void NamespaceImpl::LoadFromStorage(unsigned threadsCount, const RdxContext& ctx) {
	auto wlck = simpleWLock(ctx);
	FlagGuardT nsLoadingGuard(nsIsLoading_);

	const uint64_t dataHash = repl_.dataHash;
	repl_.dataHash = 0;

	ItemsLoader loader(threadsCount, *this);
	auto ldata = loader.Load();

	initWAL(ldata.minLSN, ldata.maxLSN);
	if (!isSystem()) {
		repl_.lastLsn.SetServer(wal_.GetServer());
	}

	logFmt(LogInfo, "[{}] Done loading storage. {} items loaded ({} errors {}), lsn #{}, total size={}M, dataHash={}", name_, items_.size(),
		   ldata.errCount, ldata.lastErr.what(), repl_.lastLsn, ldata.ldcount / (1024 * 1024), repl_.dataHash);
	if (dataHash != repl_.dataHash) {
		logFmt(LogError, "[{}] Warning dataHash mismatch {} != {}", name_, dataHash, repl_.dataHash);
		replStateUpdates_.fetch_add(1, std::memory_order_release);
	}

	markUpdated(IndexOptimization::Full);
}

void NamespaceImpl::initWAL(int64_t minLSN, int64_t maxLSN) {
	wal_.Init(getWalSize(config_), minLSN, maxLSN, storage_);
	// Fill existing records
	for (IdType rowId = 0; rowId < IdType(items_.size()); rowId++) {
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
	auto wlck = dataWLock(rdxCtx);
	const auto now = std::chrono::duration_cast<std::chrono::seconds>(system_clock_w::now().time_since_epoch());
	if (now == lastExpirationCheckTs_) {
		return;
	}
	lastExpirationCheckTs_ = now;
	for (const std::unique_ptr<Index>& index : indexes_) {
		if ((index->Type() != IndexTtl) || (index->Size() == 0)) {
			continue;
		}
		const int64_t expirationThreshold =
			std::chrono::duration_cast<std::chrono::seconds>(system_clock_w::now_coarse().time_since_epoch()).count() -
			index->GetTTLValue();
		LocalQueryResults qr;
		qr.AddNamespace(this, true);
		auto q = Query(name_).Where(index->Name(), CondLt, expirationThreshold);
		doDeleteTr(qr, pendedRepl, q, nsCtx);
		if (qr.Count()) {
			logFmt(LogInfo, "[{}] {} items were removed: TTL({}) has expired", name_, qr.Count(), index->Name());
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

	for (const auto& index : indexes_) {
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
	const auto oldTmV = tagsMatcher_.version();
	auto fields = schema_->GetPaths();
	for (auto& field : fields) {
		[[maybe_unused]] auto _ = tagsMatcher_.path2tag(field, CanAddField_True);
	}
	if (oldTmV != tagsMatcher_.version()) {
		logFmt(LogInfo,
			   "[tm:{}]:{}: TagsMatcher was updated from schema. Old tm: {{ state_token: {:#08x}, version: {} }}, new tm: {{ state_token: "
			   "{:#08x}, version: {} }}",
			   name_, wal_.GetServer(), tagsMatcher_.stateToken(), oldTmV, tagsMatcher_.stateToken(), tagsMatcher_.version());
	}

	auto err = schema_->BuildProtobufSchema(tagsMatcher_, payloadType_);
	if (!err.ok()) {
		logFmt(LogInfo, "Unable to build protobuf schema for the '{}' namespace: {}", name_, err.what());
	}

	replicateTmUpdateIfRequired(pendedRepl, oldTmV, ctx);
	addToWAL(schema, WalSetSchema, ctx);
	pendedRepl.emplace_back(updates::URType::SetSchema, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), std::string(schema));
}

void NamespaceImpl::setTagsMatcher(TagsMatcher&& tm, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (ctx.GetOriginLSN().isEmpty()) {
		throw Error(errLogic, "Tagsmatcher may be set by replication only");
	}
	if (tm.stateToken() != tagsMatcher_.stateToken()) {
		throw Error(errParams, "Tagsmatcher have different statetokens: {:#08x} vs {:#08x}", tagsMatcher_.stateToken(), tm.stateToken());
	}
	logFmt(
		LogInfo,
		"[tm:{}]:{} Set new TagsMatcher (replicated): {{ state_token: {:#08x}, version: {} }} -> {{ state_token: {:#08x}, version: {} }}",
		name_, wal_.GetServer(), tagsMatcher_.stateToken(), tagsMatcher_.version(), tm.stateToken(), tm.version());
	tagsMatcher_ = tm;
	tagsMatcher_.UpdatePayloadType(payloadType_, indexes_.SparseIndexes(), NeedChangeTmVersion::No);
	tagsMatcher_.setUpdated();

	const auto lsn = wal_.Add(WALRecord(WalEmpty, 0, ctx.IsInTransaction()), ctx.GetOriginLSN());
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
}

void NamespaceImpl::StorageFlushingRoutine() { storage_.Flush(StorageFlushOpts()); }

void NamespaceImpl::ANNCachingRoutine() {
	const bool skipTimeCheck = false;
	UpdateANNStorageCache(skipTimeCheck, RdxContext());
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
		for (auto& idx : indexes_) {
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
		for (auto& idx : indexes_) {
			rebuildSingleIndex(idx.get());
		}
	} else {
		int indexId = -1;
		if (tryGetIndexByName(index, indexId)) {
			rebuildSingleIndex(indexes_[indexId].get());
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

// Get metadata from storage by key
std::string NamespaceImpl::GetMeta(const std::string& key, const RdxContext& ctx) {
	auto rlck = rLock(ctx);
	return getMeta(key);
}

std::string NamespaceImpl::getMeta(const std::string& key) const {
	if (key.empty()) {
		throw Error(errParams, "Empty key is not supported");
	}

	auto it = meta_.find(key);
	return it == meta_.end() ? std::string() : it->second;
}

// Put metadata to storage by key
void NamespaceImpl::PutMeta(const std::string& key, std::string_view data, const RdxContext& rdxCtx) {
	UpdatesContainer pendedRepl;
	const NsContext ctx{rdxCtx};

	auto wlck = dataWLock(rdxCtx);

	putMeta(key, data, pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::putMeta(const std::string& key, std::string_view data, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (key.empty()) {
		throw Error(errParams, "Empty key is not supported");
	}

	meta_[key] = std::string(data);

	storage_.WriteSync(StorageOpts().FillCache(), kStorageMetaPrefix + key, data);

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

	auto wlck = dataWLock(rdxCtx);

	deleteMeta(key, pendedRepl, ctx);
	replicate(std::move(pendedRepl), std::move(wlck), false, nullptr, ctx);
}

void NamespaceImpl::deleteMeta(const std::string& key, UpdatesContainer& pendedRepl, const NsContext& ctx) {
	if (key.empty()) {
		throw Error(errParams, "Empty key is not supported");
	}
	if (ctx.IsInTransaction()) {
		throw Error(errParams, "DeleteMeta command not supported in transaction mode");
	}

	meta_.erase(key);

	storage_.RemoveSync(StorageOpts().FillCache(), kStorageMetaPrefix + key);

	processWalRecord(WALRecord(WalDeleteMeta, key, ctx.IsInTransaction()), ctx);
	pendedRepl.emplace_back(updates::URType::DeleteMeta, name_, wal_.LastLSN(), repl_.nsVersion, ctx.EmitterServerId(), key, std::string());
}

void NamespaceImpl::warmupFtIndexes() {
	for (auto& idx : indexes_) {
		if (idx->IsFulltext()) {
			idx->CommitFulltext();
		}
	}
}

IdType NamespaceImpl::createItem(size_t realSize, IdType suggestedId, const NsContext& ctx) {
	IdType id = 0;
	if (suggestedId < 0) {
		if (!free_.empty()) {
			id = free_.back();
			free_.pop_back();
			assertrx(id < IdType(items_.size()));
			assertrx(items_[id].IsFree());
			items_[id] = PayloadValue(realSize);
		} else {
			id = items_.size();
			if (id == std::numeric_limits<IdType>::max()) {
				throw Error(errParams, "Max item ID value is reached. Unable to store more than {} items in '{}' namespace", id, name_);
			}
			items_.emplace_back(PayloadValue(realSize));
		}
	} else {
		if (!ctx.IsForceSyncItem()) {
			throw Error(errParams, "Suggested ID should only be used during force-sync replication: {}", suggestedId);
		}
		id = suggestedId;
		if (size_t(id) < items_.size()) {
			if (!items_[size_t(id)].IsFree()) {
				throw Error(errParams, "Suggested ID {} is not empty", id);
			}
		} else {
			items_.resize(size_t(id) + 1);
			items_[id] = PayloadValue(realSize);
		}
	}
	return id;
}

void NamespaceImpl::setFieldsBasedOnPrecepts(ItemImpl* ritem, UpdatesContainer& replUpdates, const NsContext& ctx) {
	for (auto& precept : ritem->GetPrecepts()) {
		auto sqlFunc = QueryFunctionParser::Parse(precept);

		ritem->GetPayload().Get(sqlFunc.field, krefs);

		skrefs.clear<false>();
		if (sqlFunc.isFunction) {
			if (auto& pt = payloadType_; pt->Field(pt.FieldByName(sqlFunc.field)).IsArray()) {
				throw Error(errLogic, "Precepts are not allowed for array fields ('{}')", sqlFunc.field);
			}
			skrefs.emplace_back(FunctionExecutor(*this, replUpdates).Execute(QueryFunction{sqlFunc}, ctx));
		} else {
			skrefs.emplace_back(make_key_string(sqlFunc.value));
		}

		std::ignore = skrefs.back().convert(krefs[0].Type());
		bool unsafe = ritem->IsUnsafe();
		ritem->Unsafe(false);
		ritem->SetField(ritem->GetPayload().Type().FieldByName(sqlFunc.field), skrefs);
		ritem->Unsafe(unsafe);
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

void NamespaceImpl::FillResult(LocalQueryResults& result, const IdSet& ids) const {
	for (auto id : ids) {
		result.AddItemRef(id, items_[id]);
	}
}

void NamespaceImpl::getFromJoinCache(const Query& q, const JoinedQuery& jq, JoinCacheRes& out) const {
	if (config_.cacheMode == CacheModeOff || !indexOptimizer_.IsOptimizationCompleted()) {
		return;
	}
	out.key.SetData(jq, q);
	getFromJoinCacheImpl(out);
}

void NamespaceImpl::getFromJoinCache(const Query& q, JoinCacheRes& out) const {
	if (config_.cacheMode == CacheModeOff || !indexOptimizer_.IsOptimizationCompleted()) {
		return;
	}
	out.key.SetData(q);
	getFromJoinCacheImpl(out);
}

void NamespaceImpl::getFromJoinCacheImpl(JoinCacheRes& ctx) const {
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

void NamespaceImpl::getInsideFromJoinCache(JoinCacheRes& ctx) const {
	if (config_.cacheMode != CacheModeAggressive || !indexOptimizer_.IsOptimizationCompleted()) {
		return;
	}
	getFromJoinCacheImpl(ctx);
}

void NamespaceImpl::putToJoinCache(JoinCacheRes& res, JoinPreResult::CPtr preResult) const {
	JoinCacheVal joinCacheVal;
	res.needPut = false;
	joinCacheVal.inited = true;
	joinCacheVal.preResult = std::move(preResult);
	joinCache_.Put(res.key, std::move(joinCacheVal));
}
void NamespaceImpl::putToJoinCache(JoinCacheRes& res, JoinCacheVal&& val) const {
	val.inited = true;
	joinCache_.Put(res.key, std::move(val));
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
			throw Error(errUpdateReplication, err.what());
		}
	}
}

void NamespaceImpl::replicateAsync(UpdatesContainer&& recs, const RdxContext& ctx) {
	if (!isTemporary()) {
		auto err = observers_.SendAsyncUpdates(std::move(recs), ctx);
		if (!err.ok()) {
			throw Error(errUpdateReplication, err.what());
		}
	}
}

bool NamespaceImpl::IsFulltextOrVector(std::string_view indexName, const RdxContext& ctx) const {
	auto rlck = rLock(ctx);
	for (const auto& idx : indexes_) {
		if (indexName == idx->Name()) {
			return idx->IsFloatVector() || idx->IsFulltext();
		}
	}
	return false;
}

std::shared_ptr<const reindexer::QueryEmbedder> NamespaceImpl::QueryEmbedder(std::string_view fieldName, const RdxContext& ctx) const {
	auto rlck = rLock(ctx);

	int idxNo = NotSet;
	if (!tryGetIndexByNameOrJsonPath(fieldName, idxNo)) {
		throw Error(errParams, "Can't find field by name or json path: '{}'", fieldName);
	}
	if (idxNo >= payloadType_.NumFields()) {
		throw Error(errParams, "Can't use embedding with sparse/composite indexes ('{}')", fieldName);
	}
	const auto& type = payloadType_.Field(idxNo);
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
		for (auto& idx : ns_.indexes_) {
			if (idx->IsSupportSortedIdsBuild()) {
				idx->ClearCache();
			}
		}
	}
}

int64_t NamespaceImpl::correctMaxIterationsIdSetPreResult(int64_t maxIterationsIdSetPreResult) const {
	auto res = maxIterationsIdSetPreResult;
	static constexpr int64_t minBound = JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization() + 1;
	static constexpr int64_t maxBound = std::numeric_limits<int>::max();
	if ((maxIterationsIdSetPreResult < minBound) || (maxBound < maxIterationsIdSetPreResult)) {
		res = std::min<int64_t>(std::max<int64_t>(minBound, maxIterationsIdSetPreResult), maxBound);
		if (!isSystem()) {
			logFmt(LogWarning,
				   "Namespace ({}): 'max_iterations_idset_preresult' variable is forced to be adjusted. Inputted: {}, adjusted: {}", name_,
				   maxIterationsIdSetPreResult, res);
		}
	}
	return res;
}

// This method has to be noexcept to keep indexes consistency
void NamespaceImpl::rebuildIndexesToCompositeMapping() noexcept {
	// The only possible exception here is bad_alloc, but required memory footprint is tiny
	const auto beg = indexes_.firstCompositePos();
	const auto end = beg + indexes_.compositeIndexesSize();
	std::optional<fast_hash_map<int, std::vector<int>>> indexesToComposites;
	for (auto i = beg; i < end; ++i) {
		const auto& index = indexes_[i];
		assertrx(IsComposite(index->Type()));
		const auto& fields = index->Fields();
		for (auto field : fields) {
			try {
				if (!indexesToComposites.has_value()) {
					indexesToComposites.emplace();
				}
				indexesToComposites.value()[field].emplace_back(i);
			} catch (...) {
				// Termination here is better, than inconsistent state of the indexes
				std::terminate();
			}
		}
	}
	if (indexesToComposites.has_value()) {
		indexesToComposites_ = std::move(indexesToComposites.value());
	}
}

void NamespaceImpl::throwIndexUpsertErrorWithPKInfo(const ConstPayload& pl, const std::exception& err) {
	auto [pkIndex, pkField] = getPkIdx();
	assertrx_throw(pkIndex);
	VariantArray keys = getPkKeys(pl, pkIndex, pkField);
	auto errPtr = dynamic_cast<const Error*>(&err);
	throw Error{errPtr ? errPtr->code() : errParams, fmt::format("Error during processing item with primary key `{}`={}: {}",
																 pkIndex->Name(), keys.Dump(payloadType_, pkFields()), err.what())};
}

}  // namespace reindexer
