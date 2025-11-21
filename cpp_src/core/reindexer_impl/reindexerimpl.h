#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "cluster/config.h"
#include "core/dbconfig.h"
#include "core/namespace/bgnamespacedeleter.h"
#include "core/namespace/namespace.h"
#include "core/querystat.h"
#include "core/rdxcontext.h"
#include "core/reindexerconfig.h"
#include "core/transaction/transaction.h"
#include "estl/atomic_unique_ptr.h"
#include "estl/concepts.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/timed_mutex.h"
#include "events/observer.h"
#include "net/ev/ev.h"
#include "tools/errors.h"
#include "tools/filecontentwatcher.h"
#include "tools/mutex_set.h"
#include "tools/nsversioncounter.h"
#include "tools/tcmallocheapwathcher.h"

namespace reindexer {

class IClientsStats;
struct ClusterControlRequestData;
class IUpdatesObserverV3;
class UpdatesFilters;
class EmbeddersCache;

namespace cluster {
struct NodeData;
class ClusterManager;
class RoleSwitcher;
class LeaderSyncThread;
template <typename T>
class ReplThread;
class ClusterThreadParam;
struct RaftInfo;
}  // namespace cluster

class [[nodiscard]] ReindexerImpl {
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Reindexer>;
	using StatsSelectMutex = MarkedMutex<timed_mutex, MutexMark::ReindexerStats>;
	template <bool needUpdateSystemNs, typename MemFnType, MemFnType Namespace::* MemFn, typename Arg, typename... Args>
	Error applyNsFunction(std::string_view nsName, const RdxContext& ctx, Arg arg, Args&&... args);
	template <auto MemFn, typename Arg, typename... Args>
	Error applyNsFunction(std::string_view nsName, const RdxContext& ctx, Arg&&, Args&&...);

public:
	using Completion = std::function<void(const Error& err)>;

	struct [[nodiscard]] CallbackT {
		enum class [[nodiscard]] Type { System, User };
		struct [[nodiscard]] EmptyT {};
		struct [[nodiscard]] SourceIdT {
			int64_t sourceId;
		};
		using ExtrasT = std::variant<EmptyT, SourceIdT>;
		using Value = std::function<void(const gason::JsonNode& action, const ExtrasT& extras, const RdxContext& ctx)>;
		struct [[nodiscard]] Key {
			std::string_view key;
			Type type;

			bool operator==(const Key& other) const noexcept { return key == other.key && type == other.type; }
			bool operator<(const Key& other) const noexcept { return key < other.key && type < other.type; }
		};
		struct [[nodiscard]] Hash {
			std::size_t operator()(Key key) const noexcept { return std::hash<std::string_view>{}(key.key); }
		};
	};

	using CallbackMap = fast_hash_map<CallbackT::Key, CallbackT::Value, CallbackT::Hash>;

	ReindexerImpl(ReindexerConfig cfg, ActivityContainer& activities, CallbackMap&& proxyCallbacks);

	~ReindexerImpl();

	Error Connect(const std::string& dsn, ConnectOpts opts = ConnectOpts());
	Error OpenNamespace(std::string_view nsName, const StorageOpts& opts = StorageOpts().Enabled().CreateIfMissing(),
						const NsReplicationOpts& replOpts = NsReplicationOpts(), const RdxContext& ctx = RdxContext());
	Error AddNamespace(const NamespaceDef& nsDef, std::optional<NsReplicationOpts> replOpts = NsReplicationOpts{},
					   const RdxContext& ctx = RdxContext());
	Error CloseNamespace(std::string_view nsName, const RdxContext& ctx);
	Error DropNamespace(std::string_view nsName, const RdxContext& ctx);
	Error CreateTemporaryNamespace(std::string_view baseName, std::string& resultName, const StorageOpts& opts, lsn_t nsVersion,
								   const RdxContext& ctx);
	Error TruncateNamespace(std::string_view nsName, const RdxContext& ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string& dstNsName, const RdxContext& ctx);
	Error AddIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const RdxContext& ctx);
	Error GetSchema(std::string_view nsName, int format, std::string& schema, const RdxContext& ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef& indexDef, const RdxContext& ctx);
	Error DropIndex(std::string_view nsName, const IndexDef& index, const RdxContext& ctx);
	Error EnumNamespaces(std::vector<NamespaceDef>& defs, EnumNamespacesOpts opts, const RdxContext& ctx) noexcept;
	Error Insert(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Insert(std::string_view nsName, Item& item, LocalQueryResults&, const RdxContext& ctx);
	Error Update(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Update(std::string_view nsName, Item& item, LocalQueryResults&, const RdxContext& ctx);
	Error Update(const Query& query, LocalQueryResults& result, const RdxContext& ctx);
	Error Upsert(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Upsert(std::string_view nsName, Item& item, LocalQueryResults&, const RdxContext& ctx);
	Error Delete(std::string_view nsName, Item& item, const RdxContext& ctx);
	Error Delete(std::string_view nsName, Item& item, LocalQueryResults&, const RdxContext& ctx);
	Error Delete(const Query& query, LocalQueryResults& result, const RdxContext& ctx);
	Error Select(const Query& query, LocalQueryResults& result, const RdxContext& ctx);
	Item NewItem(std::string_view nsName, const RdxContext& ctx);

	LocalTransaction NewTransaction(std::string_view nsName, const RdxContext& ctx);
	Error CommitTransaction(LocalTransaction& tr, LocalQueryResults& result, const RdxContext& ctx);

	Error GetMeta(std::string_view nsName, const std::string& key, std::string& data, const RdxContext& ctx);
	Error PutMeta(std::string_view nsName, const std::string& key, std::string_view data, const RdxContext& ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string>& keys, const RdxContext& ctx);
	Error DeleteMeta(std::string_view nsName, const std::string& key, const RdxContext& ctx);
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string>& suggestions, const RdxContext& ctx);
	Error GetProtobufSchema(WrSerializer& ser, std::vector<std::string>& namespaces);
	Error GetReplState(std::string_view nsName, ReplicationStateV2& state, const RdxContext& ctx) noexcept;
	Error SetClusterOperationStatus(std::string_view nsName, const ClusterOperationStatus& status, const RdxContext& ctx) noexcept;
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts& opts, Snapshot& snapshot, const RdxContext& ctx) noexcept;
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk& ch, const RdxContext& ctx) noexcept;
	Error Status() noexcept {
		if (connected_.load(std::memory_order_acquire)) [[likely]] {
			return {};
		}
		return {errNotValid, "DB is not connected"};
	}
	Error SuggestLeader(const cluster::NodeData& suggestion, cluster::NodeData& response) noexcept;
	Error LeadersPing(const cluster::NodeData&) noexcept;
	Error GetRaftInfo(bool allowTransitState, cluster::RaftInfo&, const RdxContext& ctx) noexcept;
	Error ClusterControlRequest(const ClusterControlRequestData& request) noexcept;
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher&& tm, const RdxContext& ctx);
	void ShutdownCluster();

	bool NeedTraceActivity() const noexcept { return configProvider_.ActivityStatsEnabled(); }

	Error DumpIndex(std::ostream& os, std::string_view nsName, std::string_view index, const RdxContext& ctx);
	bool NamespaceIsInClusterConfig(std::string_view nsName);

	Error SubscribeUpdates(IEventsObserver& observer, EventSubscriberConfig&& cfg);
	Error UnsubscribeUpdates(IEventsObserver& observer);

private:
	using FilterNsNamesT = std::optional<h_vector<std::string, 6>>;
	using ShardinConfigPtr = intrusive_ptr<const intrusive_atomic_rc_wrapper<cluster::ShardingConfig>>;
	using NsCreationLockerT = MutexSet<RdxContext, MutexMark::StorageDirOps, 61>;

	class [[nodiscard]] BackgroundThread {
	public:
		~BackgroundThread() { Stop(); }

		template <typename F>
		void Run(F&& f) {
			Stop();
			async_.set(loop_);
			async_.set([this](net::ev::async&) noexcept { loop_.break_loop(); });
			async_.start();
			th_ = std::thread(std::forward<F>(f), std::ref(loop_));
		}
		void Stop() {
			if (th_.joinable()) {
				async_.send();
				th_.join();
				async_.stop();
			}
		}

	private:
		std::thread th_;
		net::ev::async async_;
		net::ev::dynamic_loop loop_;
	};

	class [[nodiscard]] StatsLocker {
	public:
		using StatsLockT = contexted_unique_lock<StatsSelectMutex, const RdxContext>;

		StatsLocker();
		StatsLockT LockIfRequired(std::string_view sysNsName, const RdxContext&);

	private:
		std::unordered_map<std::string_view, StatsSelectMutex, nocase_hash_str, nocase_equal_str> mtxMap_;
	};

	class [[nodiscard]] Locker {
	public:
		class [[nodiscard]] NsWLock {
		public:
			NsWLock(Mutex& mtx, const RdxContext& ctx, bool isCL) : impl_(mtx, ctx), isClusterLck_(isCL) {}
			void lock() { impl_.lock(); }
			void lock(ignore_cancel_ctx flag) { impl_.lock(flag); }
			void unlock() { impl_.unlock(); }
			bool owns_lock() const { return impl_.owns_lock(); }
			bool isClusterLck() const noexcept { return isClusterLck_; }

		private:
			contexted_unique_lock<Mutex, const RdxContext> impl_;
			bool isClusterLck_ = false;
		};
		typedef contexted_shared_lock<Mutex, const RdxContext> RLockT;
		typedef NsWLock WLockT;

		Locker(const cluster::IDataSyncer& clusterManager, ReindexerImpl& owner) noexcept : syncer_(clusterManager), owner_(owner) {}

		RLockT RLock(const RdxContext& ctx) const {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());
			return RLockT(mtx_, ctx);
		}
		WLockT DataWLock(const RdxContext& ctx, std::string_view nsName) const {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());

			checkReplTokens(nsName, ctx);
			WLockT lck(mtx_, ctx, true);
			awaitSync(lck, ctx);
			return lck;
		}
		WLockT SimpleWLock(const RdxContext& ctx) const {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());
			return WLockT(mtx_, ctx, false);
		}

		NsCreationLockerT::Locks CreationLock(std::string_view name, const RdxContext& ctx) {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());

			checkReplTokens(name, ctx);
			auto lck = nsCreationLocker_.Lock(name, ctx);
			awaitSync(lck, ctx);
			return lck;
		}
		NsCreationLockerT::Locks CreationLock(std::string_view name1, std::string_view name2, const RdxContext& ctx) {
			assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());

			checkReplTokens(name1, ctx);
			auto lck = nsCreationLocker_.Lock(name1, name2, ctx);
			awaitSync(lck, ctx);
			return lck;
		}

	private:
		template <typename LockT>
		void awaitSync(LockT& lck, const RdxContext& ctx) const {
			if (ctx.NoWaitSync() || !ctx.GetOriginLSN().isEmpty()) {
				return;
			}
			auto clusterStatus = owner_.clusterStatus_;
			const bool isFollowerDB = clusterStatus.role == ClusterOperationStatus::Role::SimpleReplica ||
									  clusterStatus.role == ClusterOperationStatus::Role::ClusterReplica;
			bool synchronized = isFollowerDB || syncer_.IsInitialSyncDone();

			while (!synchronized) {
				lck.unlock();
				syncer_.AwaitInitialSync(ctx);
				lck.lock();
				synchronized = syncer_.IsInitialSyncDone();
			}
		}

		void checkReplTokens(std::string_view nsName, const RdxContext& ctx) const {
			if (ctx.GetOriginLSN().isEmpty()) {
				return;
			}

			if (auto err = owner_.configProvider_.CheckAsyncReplicationToken(nsName, ctx.LeaderReplicationToken()); !err.ok()) {
				throw Error(err.code(), "Modification operation cannot be performed for namespace '{}': {}", nsName, err.what());
			}
		}

		NsCreationLockerT nsCreationLocker_;
		mutable Mutex mtx_;
		const cluster::IDataSyncer& syncer_;
		ReindexerImpl& owner_;
	};

	Error addNamespace(const NamespaceDef& nsDef, std::optional<NsReplicationOpts> replOpts, const RdxContext& ctx) noexcept;
	void getLeaderDsn(DSN& dsn, unsigned short serverId, const cluster::RaftInfo& info);
	Error insertDontUpdateSystemNS(std::string_view nsName, Item& item, const RdxContext& ctx);
	FilterNsNamesT detectFilterNsNames(const Query& q);
	StatsLocker::StatsLockT syncSystemNamespaces(std::string_view sysNsName, const FilterNsNamesT&, const RdxContext& ctx);
	void createSystemNamespaces();
	void handleDropANNCacheAction(const gason::JsonNode& action, const RdxContext& ctx);
	void handleRebuildIVFIndexAction(const gason::JsonNode& action, const RdxContext& ctx);
	void handleCreateEmbeddingsAction(const gason::JsonNode& action, const RdxContext& ctx);
	void handleClearEmbeddersCacheAction(const gason::JsonNode& action);
	void updateToSystemNamespace(std::string_view nsName, Item&, const RdxContext& ctx);
	void handleConfigAction(const gason::JsonNode& action, const std::vector<std::pair<std::string, Namespace::Ptr>>& namespaces,
							const RdxContext& ctx);
	void updateConfigProvider(const gason::JsonNode& config, bool autoCorrect = false);
	template <typename ConfigT>
	void updateConfFile(const ConfigT& newConf, std::string_view filename);
	void onProfilingConfigLoad();
	void onEmbeddersConfigLoad();
	void createEmbeddings(const Namespace::Ptr& ns, uint32_t batchSize, const RdxContext& ctx);
	Error initSystemNamespaces();
	template <const char* type, typename ConfigT>
	Error tryLoadConfFromFile(const std::string& filename);
	template <const char* type, typename ConfigT>
	Error tryLoadConfFromYAML(const std::string& yamlConf);
	Error tryLoadShardingConf(const RdxContext& ctx = RdxContext()) noexcept;

	void backgroundRoutine(net::ev::dynamic_loop& loop);
	void storageFlushingRoutine(net::ev::dynamic_loop& loop);
	void annCachingRoutine(net::ev::dynamic_loop& loop);
	Error closeNamespace(std::string_view nsName, const RdxContext& ctx, bool dropStorage);

	PayloadType getPayloadType(std::string_view nsName);
	bool isFulltextOrVector(std::string_view nsName, std::string_view indexName) const;

	Namespace::Ptr getNamespace(std::string_view nsName, const RdxContext& ctx);
	Namespace::Ptr getNamespaceNoThrow(std::string_view nsName, const RdxContext& ctx);
	lsn_t setNsVersion(Namespace::Ptr& ns, const std::optional<NsReplicationOpts>& replOpts, const RdxContext& ctx);

	Error openNamespace(std::string_view nsName, IsDBInitCall isDBInitCall, const StorageOpts& opts,
						std::optional<NsReplicationOpts> replOpts, const RdxContext& ctx);
	std::vector<std::pair<std::string, Namespace::Ptr>> getNamespaces(const RdxContext& ctx);
	std::vector<std::string> getNamespacesNames(const RdxContext& ctx);
	Error renameNamespace(std::string_view srcNsName, const std::string& dstNsName, bool fromReplication = false, bool skipResync = false,
						  const RdxContext& ctx = RdxContext());
	bool isSystemNamespaceNameStrict(std::string_view name) noexcept;
	Error readClusterConfigFile();
	Error readShardingConfigFile();
	void saveNewShardingConfigFile(const cluster::ShardingConfig& config) const;
	void checkDBClusterRole(std::string_view nsName, lsn_t originLsn) const;
	void setClusterOperationStatus(ClusterOperationStatus&& status, const RdxContext& ctx);
	std::string generateTemporaryNamespaceName(std::string_view baseName);
	Error enableStorage(const std::string& storagePath);

	Error saveShardingCfgCandidate(std::string_view config, int64_t sourceId, const RdxContext& ctx) noexcept;
	Error applyShardingCfgCandidate(int64_t sourceId, const RdxContext& ctx) noexcept;
	Error resetOldShardingConfig(int64_t sourceId, const RdxContext& ctx) noexcept;
	Error resetShardingConfigCandidate(int64_t sourceId, const RdxContext& ctx) noexcept;
	Error rollbackShardingConfigCandidate(int64_t sourceId, const RdxContext& ctx) noexcept;

	template <typename PreReplFunc, typename... Args>
	Error shardingConfigReplAction(const RdxContext& ctx, const PreReplFunc& func, Args&&... args) noexcept;

	template <typename... Args>
	Error shardingConfigReplAction(const RdxContext& ctx, updates::URType type, Args&&... args) noexcept;

	template <concepts::OneOf<Query, JoinedQuery> Q>
	std::optional<Q> embedKNNQueries(const Q& query, const RdxContext& ctx);

	template <concepts::OneOf<Query, JoinedQuery> Q>
	void embedNestedQueries(const Query& q, const std::vector<Q>& nestedQueries, std::invocable<Query&, size_t, Q&&> auto replacer,
							const RdxContext& ctx, std::optional<Query>& queryCopy);

	std::optional<Query> embedQuery(const Query& query, const RdxContext& ctx);

	template <QueryType TP>
	Error modifyQ(const Query& query, LocalQueryResults& result, const RdxContext& rdxCtx,
				  void (NamespaceImpl::*fn)(LocalQueryResults&, UpdatesContainer&, const Query&, const NsContext&));

	void maskingAsyncConfig(LocalQueryResults& result) const;

	fast_hash_map<std::string, Namespace::Ptr, nocase_hash_str, nocase_equal_str, nocase_less_str> namespaces_;

	StatsLocker statsLocker_;
	std::string storagePath_;

	BackgroundThread backgroundThread_;
	BackgroundThread storageFlushingThread_;
	BackgroundThread annCachingThread_;
	std::atomic<bool> dbDestroyed_ = {false};
	BackgroundNamespaceDeleter bgDeleter_;

	QueriesStatTracer queriesStatTracker_;
	std::unique_ptr<cluster::ClusterManager> clusterManager_;
	ClusterOperationStatus clusterStatus_;
	DBConfigProvider configProvider_;
	atomic_unique_ptr<cluster::ClusterConfigData> clusterConfig_;
	struct {
		auto Get() const noexcept {
			lock_guard lk(m_);
			return config_;
		}

		void Set(std::optional<cluster::ShardingConfig>&& other) noexcept {
			lock_guard lk(m_);
			config_.reset(other ? new intrusive_atomic_rc_wrapper<cluster::ShardingConfig>(std::move(*other)) : nullptr);
			if (handler_) {
				handler_(config_);
			}
		}

		explicit operator bool() const noexcept {
			lock_guard lk(m_);
			return config_;
		}
		void setHandled(std::function<void(const ShardinConfigPtr&)>&& handler) {
			lock_guard lk(m_);
			assertrx_dbg(!handler_);
			handler_ = std::move(handler);
		}

	private:
		mutable spinlock m_;
		ShardinConfigPtr config_ = nullptr;
		std::function<void(ShardinConfigPtr)> handler_;
	} shardingConfig_;

	std::deque<FileContetWatcher> configWatchers_;

	Locker nsLock_;

#ifdef REINDEX_WITH_GPERFTOOLS
	TCMallocHeapWathcher heapWatcher_;
#endif

	ActivityContainer& activities_;

	StorageType storageType_;
	std::atomic<bool> replicationEnabled_ = {true};
	std::atomic<bool> connected_ = {false};

	ReindexerConfig config_;

	NsVersionCounter nsVersion_;

	const CallbackMap proxyCallbacks_;
	UpdatesObservers observers_;
	std::optional<int> replCfgHandlerID_;

	const std::shared_ptr<EmbeddersCache> embeddersCache_;

	friend class cluster::ReplThread<cluster::ClusterThreadParam>;
	friend class ClusterProxy;
	friend class sharding::LocatorServiceAdapter;
	friend class cluster::LeaderSyncThread;
	friend class cluster::RoleSwitcher;
};

}  // namespace reindexer
