#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#include "cluster/config.h"
#include "core/namespace/namespace.h"
#include "core/nsselecter/nsselecter.h"
#include "core/rdxcontext.h"
#include "dbconfig.h"
#include "estl/atomic_unique_ptr.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/smart_lock.h"
#include "querystat.h"
#include "reindexerconfig.h"
#include "tools/errors.h"
#include "tools/filecontentwatcher.h"
#include "tools/tcmallocheapwathcher.h"
#include "tools/nsversioncounter.h"
#include "transaction/transaction.h"

namespace reindexer {

class IClientsStats;
struct ClusterControlRequestData;

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
	struct NsLockerItem {
		NsLockerItem(NamespaceImpl::Ptr ins = {}) : ns(std::move(ins)), count(1) {}
		NamespaceImpl::Ptr ns;
		NamespaceImpl::Locker::RLockT nsLck;
		unsigned count = 1;
	};
	template <bool needUpdateSystemNs, typename MemFnType, MemFnType Namespace::*MemFn, typename Arg, typename... Args>
	Error applyNsFunction(std::string_view nsName, const RdxContext &ctx, Arg arg, Args &&...args);
	template <auto MemFn, typename Arg, typename... Args>
	Error applyNsFunction(std::string_view nsName, const RdxContext &ctx, Arg &&, Args &&...);

public:
	using Completion = std::function<void(const Error &err)>;

	ReindexerImpl(ReindexerConfig cfg);

	~ReindexerImpl();

	Error Connect(const std::string &dsn, ConnectOpts opts = ConnectOpts());
	Error EnableStorage(const std::string &storagePath, bool skipPlaceholderCheck = false, const RdxContext &ctx = RdxContext());
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
	Error Select(std::string_view query, LocalQueryResults &result, const RdxContext &ctx);
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
	Error Status();
	Error SuggestLeader(const cluster::NodeData &suggestion, cluster::NodeData &response);
	Error LeadersPing(const cluster::NodeData &);
	Error GetRaftInfo(bool allowTransitState, cluster::RaftInfo &, const RdxContext &ctx);
	Error GetLeaderDsn(std::string &dsn, unsigned short serverId, const cluster::RaftInfo &info);
	Error ClusterControlRequest(const ClusterControlRequestData &request);
	Error SetTagsMatcher(std::string_view nsName, TagsMatcher &&tm, const RdxContext &ctx);
	void ShutdownCluster();

	bool NeedTraceActivity() { return configProvider_.ActivityStatsEnabled(); }

	Error DumpIndex(std::ostream &os, std::string_view nsName, std::string_view index, const RdxContext &ctx);

protected:
	template <typename Context>
	class NsLocker : private h_vector<NsLockerItem, 4> {
	public:
		NsLocker(const Context &context) : context_(context) {}
		~NsLocker() {
			// Unlock first
			for (auto it = rbegin(); it != rend(); ++it) {
				// Some of the namespaces may be in unlocked statet in case of the exception during Lock() call
				if (it->nsLck.owns_lock()) {
					it->nsLck.unlock();
				} else {
					assertrx(!locked_);
				}
			}
			// Clean (ns may releases, if locker holds last ref)
		}

		void Add(NamespaceImpl::Ptr ns) {
			assertrx(!locked_);
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					++(it->count);
					return;
				}
			}

			emplace_back(std::move(ns));
			return;
		}
		void Delete(const NamespaceImpl::Ptr &ns) {
			for (auto it = begin(); it != end(); ++it) {
				if (it->ns.get() == ns.get()) {
					if (!--(it->count)) erase(it);
					return;
				}
			}
			assertrx(0);
		}
		void Lock() {
			std::sort(begin(), end(), [](const NsLockerItem &lhs, const NsLockerItem &rhs) { return lhs.ns.get() < rhs.ns.get(); });
			for (auto it = begin(); it != end(); ++it) {
				it->nsLck = it->ns->rLock(context_);
			}
			locked_ = true;
		}

		NamespaceImpl::Ptr Get(const std::string &name) {
			for (auto it = begin(); it != end(); it++) {
				if (iequals(it->ns->name_, name)) return it->ns;
			}
			return nullptr;
		}

	protected:
		bool locked_ = false;
		const Context &context_;
	};

	class Locker {
	public:
		class NsWLock {
		public:
			NsWLock(Mutex &mtx, const RdxContext &ctx, bool isCL) : impl_(mtx, &ctx), isClusterLck_(isCL) {}
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

		Locker(cluster::INsDataReplicator &clusterizator, ReindexerImpl &owner) : clusterizator_(clusterizator), owner_(owner) {}

		RLockT RLock(const RdxContext &ctx) const { return RLockT(mtx_, &ctx); }
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

	template <typename T>
	void doSelect(const Query &q, LocalQueryResults &result, NsLocker<T> &locks, SelectFunctionsHolder &func, const RdxContext &ctx);
	struct QueryResultsContext;
	template <typename T>
	JoinedSelectors prepareJoinedSelectors(const Query &q, LocalQueryResults &result, NsLocker<T> &locks, SelectFunctionsHolder &func,
										   std::vector<QueryResultsContext> &, const RdxContext &ctx);
	void prepareJoinResults(const Query &q, LocalQueryResults &result);
	static bool isPreResultValuesModeOptimizationAvailable(const Query &jItemQ, const NamespaceImpl::Ptr &jns);

	void syncSystemNamespaces(std::string_view sysNsName, std::string_view filterNsName, const RdxContext &ctx);
	void createSystemNamespaces();
	void updateToSystemNamespace(std::string_view nsName, Item &, const RdxContext &ctx);
	void handleConfigAction(const gason::JsonNode &action, const std::vector<std::pair<std::string, Namespace::Ptr>> &namespaces,
							const RdxContext &ctx);
	void updateConfigProvider(const gason::JsonNode &config);
	template <typename ConfigT>
	void updateConfFile(const ConfigT &newConf, std::string_view filename);
	void onProfiligConfigLoad();
	template <char const *type, typename ConfigT>
	Error tryLoadConfFromFile(const std::string &filename);
	template <char const *type, typename ConfigT>
	Error tryLoadConfFromYAML(const std::string &yamlConf);
	Error tryLoadShardingConf();

	void backgroundRoutine();
	void storageFlushingRoutine();
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
	Error readClusterConfigFile();
	Error readShardingConfigFile();
	void checkClusterRole(std::string_view nsName, lsn_t originLsn) const;
	void setClusterizationStatus(ClusterizationStatus &&status, const RdxContext &ctx);
	std::string generateTemporaryNamespaceName(std::string_view baseName);

	fast_hash_map<std::string, Namespace::Ptr, nocase_hash_str, nocase_equal_str> namespaces_;

	std::string storagePath_;

	std::thread backgroundThread_;
	std::thread storageFlushingThread_;
	std::atomic<bool> stopBackgroundThreads_;

	QueriesStatTracer queriesStatTracker_;
	std::unique_ptr<cluster::Clusterizator> clusterizator_;
	ClusterizationStatus clusterStatus_;
	DBConfigProvider configProvider_;
	atomic_unique_ptr<cluster::ClusterConfigData> clusterConfig_;
	atomic_unique_ptr<cluster::ShardingConfig> shardingConfig_;
	std::deque<FileContetWatcher> configWatchers_;

	Locker nsLock_;

#ifdef REINDEX_WITH_GPERFTOOLS
	TCMallocHeapWathcher heapWatcher_;
#endif

	ActivityContainer activities_;

	StorageType storageType_;
	bool autorepairEnabled_ = false;
	bool replicationEnabled_ = true;
	std::atomic<bool> connected_;

	ReindexerConfig config_;

	NsVersionCounter nsVersion_;

	friend class Replicator;
	friend class cluster::DataReplicator;
	friend class cluster::ReplThread<cluster::ClusterThreadParam>;
	friend class ClusterProxy;
	friend class sharding::LocatorService;
	friend class cluster::LeaderSyncThread;
	friend class cluster::RoleSwitcher;
};

}  // namespace reindexer
