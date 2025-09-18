#pragma once

#include "clusterproxy.h"

namespace reindexer {

namespace sharding {
struct ShardingControlRequestData;
struct SaveConfigCommand;
struct ResetConfigCommand;
struct ApplyConfigCommand;
class LocatorServiceAdapter;
class LocatorService;
}  // namespace sharding

class [[nodiscard]] ShardingProxy {
public:
	ShardingProxy(ReindexerConfig cfg);
	Error Connect(const std::string& dsn, ConnectOpts opts) RX_REQUIRES(!connectMtx_);
	Error OpenNamespace(std::string_view nsName, const StorageOpts& opts, const NsReplicationOpts& replOpts, const RdxContext& ctx);
	Error AddNamespace(const NamespaceDef& nsDef, const NsReplicationOpts& replOpts, const RdxContext& ctx);
	Error CloseNamespace(std::string_view nsName, const RdxContext& ctx);
	Error DropNamespace(std::string_view nsName, const RdxContext& ctx);
	Error TruncateNamespace(std::string_view nsName, const RdxContext& ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const RdxContext& ctx);
	Error AddIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx);
	Error DropIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const RdxContext& ctx);
	Error GetSchema(std::string_view nsName, int format, std::string& schema, const RdxContext& ctx);
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const RdxContext& ctx);
	Error Insert(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Insert(std::string_view nsName, Item& item, QueryResults& result, const RdxContext& ctx);
	Error Update(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Update(std::string_view nsName, Item& item, QueryResults& result, const RdxContext& ctx);
	Error Update(const Query& query, QueryResults& result, const RdxContext& ctx);
	Error Upsert(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Upsert(std::string_view nsName, Item& item, QueryResults& result, const RdxContext& ctx);
	Error Delete(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Delete(std::string_view nsName, Item& item, QueryResults& result, const RdxContext& ctx);
	Error Delete(const Query& query, QueryResults& result, const RdxContext& ctx);
	Error ExecSQL(std::string_view sql, QueryResults& result, unsigned proxyFetchLimit, const RdxContext& ctx);
	Error Select(const Query& query, QueryResults& result, unsigned proxyFetchLimit, const RdxContext& ctx);
	Item NewItem(std::string_view nsName, const RdxContext& ctx);

	Transaction NewTransaction(std::string_view nsName, const RdxContext& ctx);
	Error CommitTransaction(Transaction& tr, QueryResults& result, const RdxContext& ctx);
	Error RollBackTransaction(Transaction& tr, const RdxContext& ctx);

	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data, const RdxContext& ctx);
	Error GetMeta(std::string_view nsName, const std::string& key, std::vector<ShardedMeta>& data, const RdxContext& ctx);
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const RdxContext& ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const RdxContext& ctx);
	Error DeleteMeta(std::string_view nsName, const std::string& key, const RdxContext& ctx);

	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions, const RdxContext& ctx);
	Error Status() noexcept;
	Error GetProtobufSchema(WrSerializer& ser, std::vector<std::string>& namespaces);
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state, const RdxContext& ctx);
	Error SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status, const RdxContext& ctx);
	bool NeedTraceActivity() const noexcept { return impl_.NeedTraceActivity(); }
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const RdxContext& ctx);
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const RdxContext& ctx);
	Error CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts, lsn_t nsVersion,
								   const RdxContext& ctx);
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const RdxContext& ctx);
	Error DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index, const RdxContext& ctx);

	Error ClusterControlRequest(const ClusterControlRequestData& request);
	Error SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response);
	Error LeadersPing(const cluster::NodeData& leader);
	Error GetRaftInfo(cluster::RaftInfo& info, const RdxContext& ctx);

	void ShutdownCluster();

	template <typename QuerySerializer>
	RdxContext CreateRdxContext(const InternalRdxContext& baseCtx, QuerySerializer&& serialize) {
		using namespace std::string_view_literals;
		if (baseCtx.NeedTraceActivity()) {
			auto& ser = getActivitySerializer();
			ser.Reset();
			serialize(ser);
			return baseCtx.CreateRdxContext(ser.Slice(), activities_);
		}
		return baseCtx.CreateRdxContext(""sv, activities_);
	}

	template <typename QuerySerializer>
	RdxContext CreateRdxContext(const InternalRdxContext& baseCtx, QuerySerializer&& serialize, QueryResults& results) {
		using namespace std::string_view_literals;
		if (baseCtx.NeedTraceActivity()) {
			auto& ser = getActivitySerializer();
			ser.Reset();
			serialize(ser);
			return baseCtx.CreateRdxContext(ser.Slice(), activities_, results);
		}
		return baseCtx.CreateRdxContext(""sv, activities_, results);
	}

	Error ShardingControlRequest(const sharding::ShardingControlRequestData& request, sharding::ShardingControlResponseData& response,
								 const RdxContext& ctx) noexcept;

	Error SubscribeUpdates(IEventsObserver& observer, EventSubscriberConfig&& cfg);
	Error UnsubscribeUpdates(IEventsObserver& observer);

	RX_ALWAYS_INLINE bool IsConnected() const noexcept { return connected_.load(std::memory_order_relaxed); }

private:
	using ItemModifyFT = Error (client::Reindexer::*)(std::string_view, client::Item&) noexcept;
	using ItemModifyQrFT = Error (client::Reindexer::*)(std::string_view, client::Item&, client::QueryResults&) noexcept;

	static WrSerializer& getActivitySerializer() noexcept {
		thread_local static WrSerializer ser;
		return ser;
	}

	auto isWithSharding(const Query& q, const RdxContext& ctx, int& actualShardId, int64_t& cfgSourceId) const;
	auto isWithSharding(std::string_view nsName, const RdxContext& ctx) const;

	bool isWithSharding(const RdxContext& ctx) const noexcept;

	template <typename ShardingRouterLock>
	bool isSharderQuery(const Query& q, const ShardingRouterLock& shLockShardingRouter) const;

	template <typename ShardingRouterLock>
	bool isSharded(std::string_view nsName, const ShardingRouterLock& shLockShardingRouter) const noexcept;

	void calculateNewLimitOfsset(size_t count, size_t totalCount, unsigned& limit, unsigned& offset);
	reindexer::client::Item toClientItem(std::string_view ns, client::Reindexer* connection, reindexer::Item& item);
	template <typename LockedRouter, typename ClientF, typename LocalF, typename T, typename Predicate, typename... Args>
	Error collectFromShardsByNs(LockedRouter&, const RdxContext&, const ClientF&, const LocalF&, std::vector<T>& result, const Predicate&,
								std::string_view nsName, Args&&...);
	template <typename LockedRouter, typename Func, typename FLocal, typename... Args>
	Error delegateToShards(LockedRouter&, const RdxContext&, const Func& f, const FLocal& local, Args&&... args);
	template <typename LockedRouter, typename Func, typename FLocal, typename... Args>
	Error delegateToShardsByNs(LockedRouter&, const RdxContext&, const Func& f, const FLocal& local, std::string_view nsName,
							   Args&&... args);
	template <ItemModifyFT fn, typename LockedRouter, typename LocalFT>
	Error modifyItemOnShard(LockedRouter&, const RdxContext& ctx, std::string_view nsName, Item& item, const LocalFT& localFn);
	template <ItemModifyQrFT fn, typename LockedRouter, typename LocalFT>
	Error modifyItemOnShard(LockedRouter&, const RdxContext& ctx, std::string_view nsName, Item& item, QueryResults& result,
							const LocalFT& localFn);
	template <typename LockedRouter, typename LocalFT>
	Error executeQueryOnShard(LockedRouter&, const Query& query, QueryResults& result, unsigned proxyFetchLimit, const RdxContext&,
							  LocalFT&&) noexcept;
	template <typename CalucalteFT>
	Error executeQueryOnClient(client::Reindexer& connection, const Query& q, client::QueryResults& qrClient,
							   const CalucalteFT& limitOffsetCalc);

	Error handleNewShardingConfig(const gason::JsonNode& config, const RdxContext& ctx) noexcept;
	Error handleNewShardingConfigLocally(const gason::JsonNode& config, std::optional<int64_t> externalSourceId,
										 const RdxContext& ctx) noexcept;
	template <typename ConfigType>
	Error handleNewShardingConfigLocally(const ConfigType& rawConfig, std::optional<int64_t> externalSourceId,
										 const RdxContext& ctx) noexcept;

	void saveShardingCfgCandidate(const sharding::SaveConfigCommand& requestData, const RdxContext& ctx);
	void saveShardingCfgCandidateImpl(cluster::ShardingConfig config, int64_t sourceId, const RdxContext& ctx);
	// Resetting saved candidate after unsuccessful saveShardingCfgCandidate on other shards
	void resetConfigCandidate(const sharding::ResetConfigCommand& data, const RdxContext& ctx);
	void applyNewShardingConfig(const sharding::ApplyConfigCommand& requestData, const RdxContext& ctx);

	bool needProxyWithinCluster(const RdxContext& ctx);

	enum class [[nodiscard]] ConfigResetFlag { RollbackApplied = 0, ResetExistent = 1 };
	// Resetting the existing sharding configs on other shards before applying the new one OR
	// Rollback (perhaps) applied candidate after unsuccessful applyNewShardingConfig on other shards
	template <ConfigResetFlag resetFlag>
	void resetOrRollbackShardingConfig(const sharding::ResetConfigCommand& data, const RdxContext& ctx);
	template <ConfigResetFlag resetFlag>
	Error resetShardingConfigs(int64_t sourceId, const RdxContext& ctx) noexcept;
	void obtainConfigForResetRouting(std::optional<cluster::ShardingConfig>& config, ConfigResetFlag resetFlag,
									 const RdxContext& ctx) const;

	struct [[nodiscard]] NamespaceDataChecker {
		NamespaceDataChecker(const cluster::ShardingConfig::Namespace& ns, int thisShardId) noexcept : ns_(ns), thisShardId_(thisShardId) {}
		void Check(ShardingProxy& proxy, const RdxContext& ctx);

	private:
		Query query() const;
		bool needQueryCheck(const cluster::ShardingConfig&) const;

		const cluster::ShardingConfig::Namespace& ns_;
		int thisShardId_;
	};

	void checkNamespaces(const cluster::ShardingConfig& config, const RdxContext& ctx);
	void checkSyncCluster(const cluster::ShardingConfig& config);

	int64_t generateSourceId() const;

	ClusterProxy impl_;

	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Reindexer>;
	using RLocker = contexted_shared_lock<Mutex, const RdxContext>;
	using WLocker = contexted_unique_lock<Mutex, const RdxContext>;

	struct [[nodiscard]] ShardingRouter {
	private:
		template <typename Locker, typename LocatorServiceSharedPtr>
		struct [[nodiscard]] ShardingRouterTSWrapper {
			const auto& operator->() const noexcept { return locatorService_; }
			ShardingRouterTSWrapper(Locker lock, LocatorServiceSharedPtr& locatorService) noexcept
				: lock_(std::move(lock)), locatorService_(locatorService) {}

			void Reset(typename LocatorServiceSharedPtr::element_type* prt = nullptr) noexcept { locatorService_.reset(prt); }
			ShardingRouterTSWrapper& operator=(LocatorServiceSharedPtr sharedPrt) noexcept {
				if (sharedPrt == locatorService_) {
					return *this;
				}

				locatorService_ = std::move(sharedPrt);
				return *this;
			}
			void Unlock() noexcept { lock_.unlock(); }
			operator bool() const noexcept { return static_cast<bool>(locatorService_); }

		private:
			Locker lock_;
			LocatorServiceSharedPtr& locatorService_;
		};

	public:
		auto SharedLock(const RdxContext& ctx) const { return ShardingRouterTSWrapper{RLocker(mtx_, ctx), locatorService_}; }
		auto SharedLock() const RX_REQUIRES(!mtx_) { return ShardingRouterTSWrapper{shared_lock(mtx_), locatorService_}; }

		auto UniqueLock(const RdxContext& ctx) { return ShardingRouterTSWrapper{WLocker(mtx_, ctx), locatorService_}; }
		auto UniqueLock() { return ShardingRouterTSWrapper{unique_lock(mtx_), locatorService_}; }

		auto SharedPtr(const RdxContext& ctx) const;

	private:
		mutable Mutex mtx_;
		std::shared_ptr<sharding::LocatorService> locatorService_;
	};

	ShardingRouter shardingRouter_;

	struct [[nodiscard]] ConfigCandidate {
	private:
		template <typename Locker, typename ConfigCandidateType>
		struct [[nodiscard]] ConfigCandidateTSWrapper {
			ConfigCandidateTSWrapper(Locker&& lock, ConfigCandidateType& configCandidate) noexcept
				: lock_(std::move(lock)), configCandidate_(configCandidate) {}

			auto& SourceId() const { return configCandidate_.sourceId_; }
			auto& Config() const { return configCandidate_.config_; }
			void InitReseterThread(std::function<void()>&& f) const;
			void ShutdownReseter() noexcept;

		private:
			Locker lock_;
			ConfigCandidateType& configCandidate_;
		};

	public:
		auto SharedLock(const RdxContext& ctx) const { return ConfigCandidateTSWrapper{RLocker(mtx_, ctx), *this}; }
		auto UniqueLock(const RdxContext& ctx) { return ConfigCandidateTSWrapper{WLocker(mtx_, ctx), *this}; }

		auto SharedLock() const RX_REQUIRES(!mtx_) { return ConfigCandidateTSWrapper{shared_lock(mtx_), *this}; }
		auto UniqueLock() { return ConfigCandidateTSWrapper{unique_lock(mtx_), *this}; }

		bool NeedStopReseter() const;
		bool TryResetConfig();
		~ConfigCandidate();

	private:
		mutable Mutex mtx_;
		std::optional<cluster::ShardingConfig> config_;
		int64_t sourceId_;
		std::thread reseter_;
		std::atomic<bool> reseterEnabled_ = true;
	};

	ConfigCandidate configCandidate_;

	std::atomic_bool shardingInitialized_ = {false};
	std::atomic_bool connected_ = {false};
	mutable shared_timed_mutex connectMtx_;
	ActivityContainer activities_;
};

}  // namespace reindexer
