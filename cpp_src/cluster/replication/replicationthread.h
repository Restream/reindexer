#pragma once

#include <span>
#include "client/cororeindexer.h"
#include "cluster/config.h"
#include "cluster/logger.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/dbconfig.h"
#include "coroutine/tokens_pool.h"
#include "net/ev/ev.h"
#include "terminationcontext.h"
#include "tools/assertrx.h"
#include "updates/updaterecord.h"
#include "updates/updatesqueue.h"

namespace reindexer {

class ReindexerImpl;
struct AsyncReplConfigSource;

namespace cluster {

constexpr size_t kUpdatesContainerOverhead = 48;

class NamespacesSyncScheduler;

struct [[nodiscard]] ReplThreadConfig {
	ReplThreadConfig() = default;
	ReplThreadConfig(const ReplicationConfigData& baseConfig, const AsyncReplConfigData& config);
	ReplThreadConfig(const ReplicationConfigData& baseConfig, const ClusterConfigData& config);

	std::string AppName = "rx_node";
	int UpdatesTimeoutSec = 20;
	int SyncTimeoutSec = 60;
	int RetrySyncIntervalMSec = 3000;
	int ParallelSyncsPerThreadCount = 2;
	int ClusterID = 1;
	size_t BatchingRoutinesCount = 100;
	int64_t MaxWALDepthOnForceSync = 1000;
	bool ForceSyncOnLogicError = false;
	bool EnableCompression = true;
	double OnlineUpdatesDelaySec = 0;
	std::string LeaderReplToken;
};

struct [[nodiscard]] UpdateApplyStatus {
	UpdateApplyStatus(Error&& _err = Error(), updates::URType _type = updates::URType::None) noexcept : err(std::move(_err)), type(_type) {}
	template <typename BehaviourParamT>
	bool NeedsSingleNsResync() const noexcept;
	template <typename BehaviourParamT>
	bool IsHaveToResync() const noexcept;

	Error err;
	updates::URType type;
};

namespace repl_thread_impl {
class [[nodiscard]] NamespaceData {
public:
	void UpdateLsnOnRecord(const updates::UpdateRecord& rec);

	ExtendedLsn latestLsn;
	client::CoroTransaction tx;
	bool requiresTmUpdate = true;
	bool isClosed = false;
	// Sampled at the start of a successful syncNamespace via UpdatesQueue::GetNextUpdateID().
	// Records of this ns with eventId < syncedAtQueueId are already in the snapshot (0 = never synced).
	uint64_t syncedAtQueueId = 0;
};

class [[nodiscard]] Node {
public:
	// This map should not invalidate references
	using MapT = std::unordered_map<NamespaceName, NamespaceData, NamespaceNameHash, NamespaceNameEqual>;
	using UpdatesChT = coroutine::channel<bool>;
	// The node's own update shows that it was alive recently: the first one in a rewind episode stops the rewind for a sync retry
	enum class [[nodiscard]] UnavailabilityCheck { NotStarted, SyncRetried, Confirmed };

	Node(int _serverId, uint32_t _uid, const client::ReindexerConfig& config) : serverId(_serverId), uid(_uid), client(config) {}
	void Reconnect(net::ev::dynamic_loop& loop, const ReplThreadConfig& config);
	void Disconnect() noexcept;
	void SetReplPermissionEpoch(uint64_t epoch) noexcept {
		if (std::exchange(replPermissionEpoch, epoch) != epoch) {
			unavailabilityCheck = UnavailabilityCheck::NotStarted;
		}
	}

	NamespaceData& NsData(const NamespaceName& nsName) {
		assertrx_dbg(!nsName.empty());
		return namespaceData_[nsName];
	}
	void EraseNsData(const NamespaceName& nsName) {
		assertrx_dbg(!nsName.empty());
		namespaceData_.erase(nsName);
	}
	auto EraseNsData(const typename MapT::const_iterator& it) { return namespaceData_.erase(it); }
	const MapT& NsData() const noexcept { return namespaceData_; }
	bool IsClusterTarget() const noexcept { return replicationSender.clusterTarget; }

	int serverId;
	uint32_t uid;
	DSN dsn;
	// Configured DSNs of the secondary cluster (cluster target only). 'dsn' is set to the validated leader among them
	std::vector<DSN> clusterDsns;
	client::CoroReindexer client;
	std::unique_ptr<UpdatesChT> updateNotifier = std::make_unique<UpdatesChT>();
	uint64_t nextUpdateId = 0;
	bool requireResync = false;
	ReplicationSender replicationSender;
	// Async replication to the RAFT cluster is performed to its leader only, which has to be found and validated first
	bool isLeaderValidated = false;
	// Queued async replication configs with lower ids are covered by the config, pushed on the node's sync (0 = not pushed)
	uint64_t asyncConfigPushedAtQueueId = 0;
	std::optional<int64_t> connObserverId;
	// Epoch of the async replication permission, which the current sync attempt has started with
	uint64_t replPermissionEpoch = 0;
	UnavailabilityCheck unavailabilityCheck = UnavailabilityCheck::NotStarted;

private:
	MapT namespaceData_;
};
}  // namespace repl_thread_impl

template <typename BehaviourParamT>
class [[nodiscard]] ReplThread {
public:
	using UpdatesQueueT = updates::UpdatesQueue<updates::UpdateRecord, ReplicationStatsCollector, Logger>;
	using Node = repl_thread_impl::Node;
	using NamespaceData = repl_thread_impl::NamespaceData;

	ReplThread(int serverId_, ReindexerImpl& thisNode, std::shared_ptr<UpdatesQueueT>, std::shared_ptr<NamespacesSyncScheduler>,
			   BehaviourParamT&&, ReplicationStatsCollector, const Logger&);

	template <typename NodeConfigT>
	void Run(ReplThreadConfig, const std::vector<std::pair<uint32_t, NodeConfigT>>& nodesList, size_t consensusCnt,
			 size_t requiredReplicas);
	void SetTerminate(bool val) noexcept;
	bool Terminated() const noexcept { return terminate_; }
	void DisconnectNodes();
	void SetNodesRequireResync() {
		for (auto& node : nodes) {
			node.requireResync = true;
			node.unavailabilityCheck = Node::UnavailabilityCheck::NotStarted;
		}
	}
	void SendUpdatesAsyncNotification() { updatesAsync_.send(); }

	std::deque<Node> nodes;
	net::ev::dynamic_loop loop;
	coroutine::wait_group wg;
	ReindexerImpl& thisNode;

private:
	enum class [[nodiscard]] SyncMode { FullSync, OnlineSingleNs };

	constexpr static bool isClusterReplThread() noexcept;
	void updateNodeStatus(size_t uid, NodeStats::Status st);
	bool awaitReplPermission(Node& node);
	bool suspendWhileForbidden(Node& node);
	void discardSuspendedUpdates(Node& node);
	bool isReplPermissionChanged(const Node& node) const noexcept;
	void nodeReplicationRoutine(Node& node);
	void awaitSyncRetry(Node& node, const Error& err);
	Error addConnectionObserver(Node& node) noexcept;
	Error namespacesSyncImpl(Node& node, std::span<const NamespaceDef> nsList, SyncMode syncMode);
	Error namespaceSyncRoutine(Node& node, const NamespaceName& nsName, SyncMode syncMode, const Error& integralError) noexcept;
	Error markExistingRemoteNs(Node& node, std::string_view nsName, ReplicationStateV2& replState) noexcept;
	Error markNewRemoteNs(Node& node, client::CoroReindexer& client, std::string_view nsName) noexcept;
	Error nodeReplicationImpl(Node& node);
	Expected<std::vector<NamespaceDef>> generateSyncNssList(const Node& node) const noexcept;
	void updatesNotifier() noexcept;
	void terminateNotifier() noexcept;
	std::tuple<bool, UpdateApplyStatus> handleNetworkCheckRecord(Node& node, UpdatesQueueT::UpdatePtr& updPtr, uint16_t offset,
																 bool currentlyOnline, const updates::UpdateRecord& rec) noexcept;

	Error syncNamespace(Node&, const NamespaceName&, const ReplicationStateV2& followerState, SyncMode syncMode);
	Error handleMissingLeaderNs(Node& node, client::CoroReindexer& client, const NamespaceName& nsName, std::string_view stage,
								bool& retrySync, ReplicationStateV2& retryLeaderState, bool allowOpenRetry = true);
	Error resyncSingleNamespace(Node& node, const NamespaceName& nsName) noexcept;
	void rollbackNamespaceTx(Node& node, const NamespaceName& nsName) { rollbackNamespaceTx(node, node.NsData(nsName)); }
	void rollbackNamespaceTx(Node& node, NamespaceData& nsData);
	Error syncShardingConfig(Node& node) noexcept;
	Error syncAsyncReplicationConfig(Node& node) noexcept;
	Error applyAsyncReplicationConfig(Node& node, const AsyncReplConfigSource& source) noexcept;
	Error sendAsyncReplicationConfig(Node& node, const AsyncReplConfigSource& source, std::optional<int64_t>& appliedSourceId) noexcept;
	Error setNamespaceOwnership(Node& node, std::string_view nsName, IsPrimary isPrimary) noexcept;
	Error syncNamespaceOwnership(Node& node, std::string_view nsName, IsPrimary isPrimaryOnFollower) noexcept;
	Error connectToClusterTargetLeader(Node& node) noexcept;
	bool isClusterTargetLeader(Node& node) noexcept;
	void invalidateClusterTargetLeader(Node& node) noexcept;
	UpdateApplyStatus nodeUpdatesHandlingLoop(Node& node) noexcept;
	bool handleUpdatesWithError(Node& node, const Error& err);
	Error prepareReplicationTarget(Node& node) noexcept;
	Error checkTargetNotInSyncCluster(Node& node) noexcept;

	UpdateApplyStatus applyUpdate(const updates::UpdateRecord& rec, Node& node, NamespaceData& nsData) noexcept;
	static bool isNetworkError(const Error& err) noexcept { return err.code() == errNetwork || err.code() == errConnectSSL; }
	static bool isNodeUnavailableError(const Error& err) noexcept { return isNetworkError(err) || err.code() == errVersion; }
	static bool isTimeoutError(const Error& err) noexcept { return err.code() == errTimeout || err.code() == errCanceled; }
	static bool isLeaderChangedError(const Error& err) noexcept { return err.code() == errWrongReplicationData; }
	static bool isTxCopyError(const Error& err) noexcept { return err.code() == errTxDoesNotExist; }
	constexpr static std::string_view logModuleName() noexcept {
		using namespace std::string_view_literals;
		if constexpr (isClusterReplThread()) {
			return "replicator:sync_t"sv;
		} else {
			return "replicator:async_t"sv;
		}
	}
	bool needForceSyncOnLogicError(const Error&) const noexcept;

	const int serverId_ = -1;
	uint32_t consensusCnt_ = 0;
	uint32_t requiredReplicas_ = 0;
	std::unique_ptr<coroutine::tokens_pool<bool>> nsSyncTokens_;
	std::shared_ptr<NamespacesSyncScheduler> nssSyncScheduler_;
	net::ev::async updatesAsync_;
	net::ev::timer updatesTimer_;
	bool notificationInProgress_ = false;
	bool hasPendingNotificaions_ = false;
	std::atomic<bool> terminate_ = {false};
	const TerminationContext terminateCtx_{terminate_};
	BehaviourParamT bhvParam_;
	ReplThreadConfig config_;
	std::shared_ptr<UpdatesQueueT> updates_;
	coroutine::channel<bool> terminateCh_;
	ReplicationStatsCollector statsCollector_;
	const Logger& log_;
};

}  // namespace cluster
}  // namespace reindexer
