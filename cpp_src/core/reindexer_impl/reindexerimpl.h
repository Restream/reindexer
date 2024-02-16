#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "cluster/config.h"
#include "core/dbconfig.h"
#include "core/namespace/namespace.h"
#include "core/querystat.h"
#include "core/rdxcontext.h"
#include "core/reindexerconfig.h"
#include "core/transaction/transaction.h"
#include "estl/atomic_unique_ptr.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "net/ev/ev.h"
#include "tools/errors.h"
#include "tools/filecontentwatcher.h"
#include "tools/nsversioncounter.h"
#include "tools/tcmallocheapwathcher.h"

#include "replv3/updatesobserver.h"

namespace reindexer {

class IClientsStats;
struct ClusterControlRequestData;

// REINDEX_WITH_V3_FOLLOWERS
class IUpdatesObserver;
class UpdatesFilters;
// REINDEX_WITH_V3_FOLLOWERS

namespace cluster {
struct NodeData;
class Clusterizator;
class DataReplicator;
class RoleSwitcher;
class LeaderSyncThread;
template <typename T>
class ReplThread;
class ClusterThreadParam;
struct RaftInfo;
}  // namespace cluster

class ReindexerImpl {
	using Mutex = MarkedMutex<shared_timed_mutex, MutexMark::Reindexer>;
	using StatsSelectMutex = MarkedMutex<std::timed_mutex, MutexMark::ReindexerStats>;
	template <bool needUpdateSystemNs, typename MemFnType, MemFnType Namespace::*MemFn, typename Arg, typename... Args>
	Error applyNsFunction(std::string_view nsName, const RdxContext &ctx, Arg arg, Args &&...args);
	template <auto MemFn, typename Arg, typename... Args>
	Error applyNsFunction(std::string_view nsName, const RdxContext &ctx, Arg &&, Args &&...);

public:
	using Completion = std::function<void(const Error &err)>;

	struct CallbackT {
		enum class Type { System, User };
		struct EmptyT {};
		struct SourceIdT {
			int64_t sourceId;
		};
		using ExtrasT = std::variant<EmptyT, SourceIdT>;
		using Value = std::function<void(const gason::JsonNode &action, const ExtrasT &extras, const RdxContext &ctx)>;
		struct Key {
			std::string_view key;
			Type type;

			bool operator==(const Key &other) const noexcept { return key == other.key && type == other.type; }
			bool operator<(const Key &other) const noexcept { return key < other.key && type < other.type; }
		};
		struct Hash {
			std::size_t operator()(Key key) const noexcept { return std::hash<std::string_view>{}(key.key); }
		};
	};

	using CallbackMap = fast_hash_map<CallbackT::Key, CallbackT::Value, CallbackT::Hash>;

	ReindexerImpl(ReindexerConfig cfg, ActivityContainer &activities, CallbackMap &&proxyCallbacks);

	~ReindexerImpl();

	Error Connect(const std::string &dsn, ConnectOpts opts = ConnectOpts());
	Error OpenNamespace(std::string_view nsName, const StorageOpts &opts = StorageOpts().Enabled().CreateIfMissing(),
						const NsReplicationOpts &replOpts = NsReplicationOpts(), const RdxContext &ctx = RdxContext());
	Error AddNamespace(const NamespaceDef &nsDef, std::optional<NsReplicationOpts> replOpts = NsReplicationOpts{},
					   const RdxContext &ctx = RdxContext());
	Error CloseNamespace(std::string_view nsName, const RdxContext &ctx);
	Error DropNamespace(std::string_view nsName, const RdxContext &ctx);
	Error CreateTemporaryNamespace(std::string_view baseName, std::string &resultName, const StorageOpts &opts, lsn_t nsVersion,
								   const RdxContext &ctx);
	Error TruncateNamespace(std::string_view nsName, const RdxContext &ctx);
	Error RenameNamespace(std::string_view srcNsName, const std::string &dstNsName, const RdxContext &ctx);
	Error AddIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx);
	Error SetSchema(std::string_view nsName, std::string_view schema, const RdxContext &ctx);
	Error GetSchema(std::string_view nsName, int format, std::string &schema, const RdxContext &ctx);
	Error UpdateIndex(std::string_view nsName, const IndexDef &indexDef, const RdxContext &ctx);
	Error DropIndex(std::string_view nsName, const IndexDef &index, const RdxContext &ctx);
	Error EnumNamespaces(std::vector<NamespaceDef> &defs, EnumNamespacesOpts opts, const RdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Insert(std::string_view nsName, Item &item, LocalQueryResults &, const RdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Update(std::string_view nsName, Item &item, LocalQueryResults &, const RdxContext &ctx);
	Error Update(const Query &query, LocalQueryResults &result, const RdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Upsert(std::string_view nsName, Item &item, LocalQueryResults &, const RdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, const RdxContext &ctx);
	Error Delete(std::string_view nsName, Item &item, LocalQueryResults &, const RdxContext &ctx);
	Error Delete(const Query &query, LocalQueryResults &result, const RdxContext &ctx);
	Error Select(const Query &query, LocalQueryResults &result, const RdxContext &ctx);
	Error Commit(std::string_view nsName);
	Item NewItem(std::string_view nsName, const RdxContext &ctx);

	LocalTransaction NewTransaction(std::string_view nsName, const RdxContext &ctx);
	Error CommitTransaction(LocalTransaction &tr, LocalQueryResults &result, const RdxContext &ctx);

	Error GetMeta(std::string_view nsName, const std::string &key, std::string &data, const RdxContext &ctx);
	Error PutMeta(std::string_view nsName, const std::string &key, std::string_view data, const RdxContext &ctx);
	Error EnumMeta(std::string_view nsName, std::vector<std::string> &keys, const RdxContext &ctx);
	Error InitSystemNamespaces();
	Error GetSqlSuggestions(std::string_view sqlQuery, int pos, std::vector<std::string> &suggestions, const RdxContext &ctx);
	Error GetProtobufSchema(WrSerializer &ser, std::vector<std::string> &namespaces);
	Error GetReplState(std::string_view nsName, ReplicationStateV2 &state, const RdxContext &ctx);
	Error SetClusterizationStatus(std::string_view nsName, const ClusterizationStatus &status, const RdxContext &ctx);
	Error GetSnapshot(std::string_view nsName, const SnapshotOpts &opts, Snapshot &snapshot, const RdxContext &ctx);
	Error ApplySnapshotChunk(std::string_view nsName, const SnapshotChunk &ch, const RdxContext &ctx);
	Error Status() noexcept {
		if rx_likely (connected_.load(std::memory_order_acquire)) {
			return {};
		}
		return Error(errNotValid, "DB is not connected");
	}
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	Error LeadersPing(const cluster::NodeData &);
	Error GetRaftInfo(bool allowTransitState, cluster::RaftInfo &, const RdxContext &ctx);
	Error GetLeaderDsn(std::string &dsn, unsigned short serverId, const cluster::RaftInfo &info);
	Error ClusterControlRequest(const ClusterControlRequestData &request);
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher &&tm, const RdxContext &ctx);
	void ShutdownCluster();

	bool NeedTraceActivity() const noexcept { return configProvider_.ActivityStatsEnabled(); }

	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index, const RdxContext &ctx);
	bool NamespaceIsInClusterConfig(std::string_view nsName);

	// REINDEX_WITH_V3_FOLLOWERS
	Error SubscribeUpdates(IUpdatesObserver *observer, const UpdatesFilters &filters, SubscriptionOpts opts);
	Error UnsubscribeUpdates(IUpdatesObserver *observer);
	// REINDEX_WITH_V3_FOLLOWERS

private:
	using FilterNsNamesT = std::optional<h_vector<std::string, 6>>;

	class BackgroundThread {
	public:
		~BackgroundThread() { Stop(); }

		template <typename F>
		void Run(F &&f) {
			Stop();
			async_.set(loop_);
			async_.set([this](net::ev::async &) noexcept { loop_.break_loop(); });
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

	class StatsLocker {
	public:
		using StatsLockT = contexted_unique_lock<StatsSelectMutex, const RdxContext>;

		StatsLocker();
		[[nodiscard]] StatsLockT LockIfRequired(std::string_view sysNsName, const RdxContext &);

	private:
		std::unordered_map<std::string_view, StatsSelectMutex, nocase_hash_str, nocase_equal_str> mtxMap_;
	};

	class Locker {
	public:
		class NsWLock {
		public:
			NsWLock(Mutex &mtx, const RdxContext &ctx, bool isCL) : impl_(mtx, ctx), isClusterLck_(isCL) {}
			void lock() { impl_.lock(); }
			void unlock() { impl_.unlock(); }
			bool owns_lock() const { return impl_.owns_lock(); }
			bool isClusterLck() const noexcept { return isClusterLck_; }

		private:
			contexted_unique_lock<Mutex, const RdxContext> impl_;
			bool isClusterLck_ = false;
		};
		typedef contexted_shared_lock<Mutex, const RdxContext> RLockT;
		typedef NsWLock WLockT;

		Locker(cluster::INsDataReplicator &clusterizator, ReindexerImpl &owner) noexcept : clusterizator_(clusterizator), owner_(owner) {}

		RLockT RLock(const RdxContext &ctx) const { return RLockT(mtx_, ctx); }
		WLockT DataWLock(const RdxContext &ctx) const {
			const bool requireSync = !ctx.NoWaitSync() && ctx.GetOriginLSN().isEmpty();
			WLockT lck(mtx_, ctx, true);
			auto clusterStatus = owner_.clusterStatus_;
			const bool isFollowerDB = clusterStatus.role == ClusterizationStatus::Role::SimpleReplica ||
									  clusterStatus.role == ClusterizationStatus::Role::ClusterReplica;
			bool synchronized = isFollowerDB || !requireSync || clusterizator_.IsInitialSyncDone();
			while (!synchronized) {
				lck.unlock();
				clusterizator_.AwaitInitialSync(ctx);
				lck.lock();
				synchronized = clusterizator_.IsInitialSyncDone();
			}
			return lck;
		}
		WLockT SimpleWLock(const RdxContext &ctx) const { return WLockT(mtx_, ctx, false); }

	private:
		mutable Mutex mtx_;
		cluster::INsDataReplicator &clusterizator_;
		ReindexerImpl &owner_;
	};

	Error insertDontUpdateSystemNS(std::string_view nsName, Item &item, const RdxContext &ctx);
	FilterNsNamesT detectFilterNsNames(const Query &q);
	[[nodiscard]] StatsLocker::StatsLockT syncSystemNamespaces(std::string_view sysNsName, const FilterNsNamesT &, const RdxContext &ctx);
	void createSystemNamespaces();
	void updateToSystemNamespace(std::string_view nsName, Item &, const RdxContext &ctx);
	void handleConfigAction(const gason::JsonNode &action, const std::vector<std::pair<std::string, Namespace::Ptr>> &namespaces,
							const RdxContext &ctx);
	void updateConfigProvider(const gason::JsonNode &config, bool autoCorrect = false);
	template <typename ConfigT>
	void updateConfFile(const ConfigT &newConf, std::string_view filename);
	void onProfiligConfigLoad();
	template <char const *type, typename ConfigT>
	Error tryLoadConfFromFile(const std::string &filename);
	template <char const *type, typename ConfigT>
	Error tryLoadConfFromYAML(const std::string &yamlConf);
	[[nodiscard]] Error tryLoadShardingConf(const RdxContext &ctx = RdxContext()) noexcept;

	void backgroundRoutine(net::ev::dynamic_loop &loop);
	void storageFlushingRoutine(net::ev::dynamic_loop &loop);
	Error closeNamespace(std::string_view nsName, const RdxContext &ctx, bool dropStorage);

	PayloadType getPayloadType(std::string_view nsName);
	std::set<std::string> getFTIndexes(std::string_view nsName);

	Namespace::Ptr getNamespace(std::string_view nsName, const RdxContext &ctx);
	Namespace::Ptr getNamespaceNoThrow(std::string_view nsName, const RdxContext &ctx);
	lsn_t setNsVersion(Namespace::Ptr &ns, const std::optional<NsReplicationOpts> &replOpts, const RdxContext &ctx);

	Error openNamespace(std::string_view nsName, bool skipNameCheck, const StorageOpts &opts, std::optional<NsReplicationOpts> replOpts,
						const RdxContext &ctx);
	std::vector<std::pair<std::string, Namespace::Ptr>> getNamespaces(const RdxContext &ctx);
	std::vector<std::string> getNamespacesNames(const RdxContext &ctx);
	Error renameNamespace(std::string_view srcNsName, const std::string &dstNsName, bool fromReplication = false, bool skipResync = false,
						  const RdxContext &ctx = RdxContext());
	[[nodiscard]] bool isSystemNamespaceNameStrict(std::string_view name) noexcept;
	Error readClusterConfigFile();
	Error readShardingConfigFile();
	void saveNewShardingConfigFile(const cluster::ShardingConfig &config) const;
	void checkClusterRole(std::string_view nsName, lsn_t originLsn) const;
	void setClusterizationStatus(ClusterizationStatus &&status, const RdxContext &ctx);
	std::string generateTemporaryNamespaceName(std::string_view baseName);
	Error enableStorage(const std::string &storagePath);

	[[nodiscard]] Error saveShardingCfgCandidate(std::string_view config, int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error applyShardingCfgCandidate(int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error resetOldShardingConfig(int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error resetShardingConfigCandidate(int64_t sourceId, const RdxContext &ctx) noexcept;
	[[nodiscard]] Error rollbackShardingConfigCandidate(int64_t sourceId, const RdxContext &ctx) noexcept;

	template <typename PreReplFunc, typename... Args>
	[[nodiscard]] Error shardingConfigReplAction(const RdxContext &ctx, PreReplFunc func, Args &&...args) noexcept;

	template <typename... Args>
	[[nodiscard]] Error shardingConfigReplAction(const RdxContext &ctx, cluster::UpdateRecord::Type type, Args &&...args) noexcept;

	fast_hash_map<std::string, Namespace::Ptr, nocase_hash_str, nocase_equal_str, nocase_less_str> namespaces_;

	StatsLocker statsLocker_;
	std::string storagePath_;

	BackgroundThread backgroundThread_;
	BackgroundThread storageFlushingThread_;
	std::atomic<bool> dbDestroyed_ = {false};
	BackgroundNamespaceDeleter bgDeleter_;

	QueriesStatTracer queriesStatTracker_;
	std::unique_ptr<cluster::Clusterizator> clusterizator_;
	ClusterizationStatus clusterStatus_;
	DBConfigProvider configProvider_;
	atomic_unique_ptr<cluster::ClusterConfigData> clusterConfig_;
	struct {
		auto Get() const noexcept {
			std::lock_guard lk(m);
			return config;
		}

		void Set(std::optional<cluster::ShardingConfig> &&other) noexcept {
			std::lock_guard lk(m);
			config.reset(other ? new intrusive_atomic_rc_wrapper<const cluster::ShardingConfig>(std::move(*other)) : nullptr);
		}

		operator bool() const noexcept {
			std::lock_guard lk(m);
			return config;
		}

	private:
		mutable spinlock m;
		intrusive_ptr<intrusive_atomic_rc_wrapper<const cluster::ShardingConfig>> config = nullptr;
	} shardingConfig_;

	std::deque<FileContetWatcher> configWatchers_;

	Locker nsLock_;

#ifdef REINDEX_WITH_GPERFTOOLS
	TCMallocHeapWathcher heapWatcher_;
#endif

	ActivityContainer &activities_;

	StorageType storageType_;
	std::atomic<bool> autorepairEnabled_ = {false};
	std::atomic<bool> replicationEnabled_ = {true};
	std::atomic<bool> connected_ = {false};

	ReindexerConfig config_;

	NsVersionCounter nsVersion_;

	const CallbackMap proxyCallbacks_;

#ifdef REINDEX_WITH_V3_FOLLOWERS
	UpdatesObservers observers_;
#endif	// REINDEX_WITH_V3_FOLLOWERS

	friend class Replicator;
	friend class cluster::DataReplicator;
	friend class cluster::ReplThread<cluster::ClusterThreadParam>;
	friend class ClusterProxy;
	friend class sharding::LocatorServiceAdapter;
	friend class cluster::LeaderSyncThread;
	friend class cluster::RoleSwitcher;
};

}  // namespace reindexer
