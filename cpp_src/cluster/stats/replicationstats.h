#pragma once

#include <optional>
#include "cluster/config.h"
#include "estl/fast_hash_map.h"
#include "estl/lock.h"
#include "estl/shared_mutex.h"
#include "estl/spin_lock.h"

namespace reindexer::cluster {

struct [[nodiscard]] SyncStats {
	void FromJSON(const gason::JsonNode&);
	void GetJSON(JsonBuilder& builder) const;
	bool operator==(const SyncStats& r) const noexcept = default;
	bool operator!=(const SyncStats& r) const noexcept = default;

	size_t count;
	size_t maxTimeUs;
	size_t avgTimeUs;
};

struct [[nodiscard]] InitialSyncStats {
	void FromJSON(const gason::JsonNode&);
	void GetJSON(JsonBuilder& builder) const;
	bool operator==(const InitialSyncStats& r) const noexcept = default;
	bool operator!=(const InitialSyncStats& r) const noexcept = default;

	SyncStats forceSyncs;
	SyncStats walSyncs;
	size_t totalTimeUs;
};

// State of the cluster's part of the async replication config on the RAFT cluster's node
struct [[nodiscard]] AsyncReplConfigStats {
	void FromJSON(const gason::JsonNode&);
	void GetJSON(JsonBuilder& builder) const;
	bool operator==(const AsyncReplConfigStats&) const noexcept = default;
	bool operator!=(const AsyncReplConfigStats&) const noexcept = default;

	// Is not set, if the node's source is unknown
	std::optional<int64_t> sourceId = std::nullopt;
	// Result of the last applying of the cluster's part on the node
	Error applyError{};
};

struct [[nodiscard]] NodeStats {
	enum class [[nodiscard]] Status { None, Offline, Online, RaftError };
	enum class [[nodiscard]] SyncState { None, Syncing, AwaitingResync, OnlineReplication, InitialLeaderSync, Suspended };

	void FromJSON(const gason::JsonNode&);
	void GetJSON(JsonBuilder& builder) const;
	bool operator==(const NodeStats& r) const noexcept(noexcept(dsn == r.dsn)) = default;
	bool operator!=(const NodeStats& r) const noexcept(noexcept(dsn != r.dsn)) = default;

	// Node, the updates are replicated to. RAFT cluster target is replicated to its leader only, so the DSN is empty,
	// until the leader is found and validated
	DSN dsn;
	// All the DSNs of the RAFT cluster target. Empty for the single node target
	std::vector<DSN> dsns;
	int64_t updatesCount;
	int64_t nssSyncQueue;
	int serverId;
	Status status;
	SyncState syncState;
	RaftInfo::Role role;
	bool isSynchronized;
	std::vector<std::string> namespaces;
	Error lastError;
	// Is set for the nodes of the RAFT cluster only
	std::optional<AsyncReplConfigStats> asyncReplConfig;
};

struct [[nodiscard]] ReplicationStats {
	Error FromJSON(std::span<char> json);
	Error FromJSON(const gason::JsonNode& root);
	void GetJSON(JsonBuilder& builder) const;
	void GetJSON(WrSerializer& ser) const;
	bool operator==(const ReplicationStats& r) const noexcept(noexcept(nodeStats == r.nodeStats)) = default;
	bool operator!=(const ReplicationStats& r) const noexcept(noexcept(nodeStats != r.nodeStats)) = default;

	std::string type;
	int64_t updateDrops;
	int64_t pendingUpdatesCount;
	int64_t allocatedUpdatesCount;
	int64_t allocatedUpdatesSizeBytes;
	SyncStats walSyncs;
	SyncStats forceSyncs;
	InitialSyncStats initialSync;
	std::vector<NodeStats> nodeStats;
	LogLevel logLevel;
};

struct [[nodiscard]] SyncStatsCounter {
	void Hit(std::chrono::microseconds time) noexcept RX_REQUIRES(!mtx_);
	void Reset() noexcept RX_REQUIRES(!mtx_);
	SyncStats Get() const RX_REQUIRES(!mtx_);

	size_t count RX_GUARDED_BY(mtx_) = 0;
	int64_t maxTimeUs RX_GUARDED_BY(mtx_) = 0;
	int64_t totalTimeUs RX_GUARDED_BY(mtx_) = 0;
	mutable spinlock mtx_;
};

struct [[nodiscard]] NodeStatsCounter {
	static constexpr size_t kNoActiveDsn = std::numeric_limits<size_t>::max();

	NodeStatsCounter(DSN d, std::vector<std::string> nss) : dsn(std::move(d)), namespaces(std::move(nss)) {}
	NodeStatsCounter(std::vector<DSN> dsns, std::vector<std::string> nss) : clusterDsns(std::move(dsns)), namespaces(std::move(nss)) {
		assertrx_throw(!clusterDsns.empty());
	}
	void OnUpdateApplied(int64_t updateId) noexcept { lastAppliedUpdateId.store(updateId, std::memory_order_relaxed); }
	void OnStatusChanged(NodeStats::Status st) noexcept { status.store(st, std::memory_order_relaxed); }
	void OnSyncStateChanged(NodeStats::SyncState st) noexcept { syncState.store(st, std::memory_order_relaxed); }
	void OnServerIdChanged(int sId) noexcept { serverId.store(sId, std::memory_order_relaxed); }
	void OnActiveDsnChanged(std::optional<size_t> idx) noexcept {
		assertrx_dbg(!idx.has_value() || *idx < clusterDsns.size());
		activeDsnIdx.store(idx.value_or(kNoActiveDsn), std::memory_order_relaxed);
	}
	void OnEnqueueNamespacesSync(size_t count) noexcept { nssSyncQueueSize.store(count, std::memory_order_relaxed); }
	void OnNamespaceSynchronized() noexcept {
		[[maybe_unused]] auto curVal = nssSyncQueueSize.fetch_sub(1, std::memory_order_relaxed);
		assertrx_dbg(curVal > 0);
	}
	void SaveLastError(const Error& err) noexcept RX_REQUIRES(!mtx_);
	void SaveAsyncReplConfig(const AsyncReplConfigStats& stats) noexcept RX_REQUIRES(!mtx_);
	NodeStats Get() const RX_REQUIRES(!mtx_);

	const DSN dsn;
	const std::vector<DSN> clusterDsns;
	const std::vector<std::string> namespaces;
	std::atomic<size_t> activeDsnIdx = {kNoActiveDsn};
	std::atomic<int64_t> lastAppliedUpdateId = {-1};
	std::atomic<int64_t> nssSyncQueueSize = {0};
	std::atomic<int> serverId = {-1};
	std::atomic<NodeStats::Status> status = {NodeStats::Status::None};
	std::atomic<NodeStats::SyncState> syncState = {NodeStats::SyncState::None};
	Error lastError RX_GUARDED_BY(mtx_);
	std::optional<AsyncReplConfigStats> asyncReplConfig RX_GUARDED_BY(mtx_);
	mutable spinlock mtx_;
};

class [[nodiscard]] ReplicationStatCounter {
public:
	static constexpr size_t kLeaderUID = std::numeric_limits<size_t>::max();

	// Values of this node's element, which are evaluated by the owner on reading instead of collecting
	struct [[nodiscard]] ThisNodeOverrides {
		AsyncReplConfigStats asyncReplConfig;
		Error lastError;
	};

	ReplicationStatCounter(std::string t) : type_(std::move(t)) {}
	template <typename NodeT>
	void Init(const std::vector<NodeT>& nodes) RX_REQUIRES(!mtx_) {
		lock_guard wlck(mtx_);
		nodeCounters_.clear();
		thisNode_.reset();
		nodeCounters_.reserve(nodes.size());
		for (size_t i = 0; i < nodes.size(); ++i) {
			nodeCounters_.emplace(i, makeNodeCounter(nodes[i], nodes[i].GetNssVector()));
		}
	}
	template <typename NodeT>
	void Init(const NodeT& thisNode, const std::vector<NodeT>& nodes, const std::vector<std::string>& namespaces) RX_REQUIRES(!mtx_) {
		lock_guard wlck(mtx_);
		nodeCounters_.clear();
		thisNode_.emplace(thisNode.GetRPCDsn(), namespaces);
		thisNode_->serverId.store(thisNode.serverId, std::memory_order_relaxed);
		thisNode_->status.store(NodeStats::Status::Online, std::memory_order_relaxed);
		nodeCounters_.reserve(nodes.size());
		for (size_t i = 0; i < nodes.size(); ++i) {
			// Only the nodes of the RAFT cluster have the async replication config's stats
			nodeCounters_.emplace(i, makeNodeCounter(nodes[i], namespaces)).first->second->SaveAsyncReplConfig(AsyncReplConfigStats());
		}
	}
	void OnWalSync(std::chrono::microseconds time) noexcept { walSyncs_.Hit(time); }
	void OnForceSync(std::chrono::microseconds time) noexcept { forceSyncs_.Hit(time); }
	void OnInitialWalSync(std::chrono::microseconds time) noexcept { initialWalSyncs_.Hit(time); }
	void OnInitialForceSync(std::chrono::microseconds time) noexcept { initialForceSyncs_.Hit(time); }
	void OnInitialSyncDone(std::chrono::microseconds time) noexcept {
		initialSyncTotalTimeUs_.store(time.count(), std::memory_order_relaxed);
	}
	void OnUpdatePushed(int64_t updateId, size_t size) noexcept {
		lastPushedUpdateId_.store(updateId, std::memory_order_relaxed);
		allocatedUpdatesSizeBytes_.fetch_add(size, std::memory_order_relaxed);
	}
	void OnUpdateApplied(size_t nodeId, int64_t updateId) const noexcept RX_REQUIRES(!mtx_) {
		shared_lock rlck(mtx_);
		auto found = nodeCounters_.find(nodeId);
		if (found != nodeCounters_.end()) {
			found->second->OnUpdateApplied(updateId);
		}
	}
	void OnUpdatesDrop(int64_t updateId, size_t size) noexcept {
		updatesDrops_.fetch_add(1, std::memory_order_relaxed);
		lastReplicatedUpdateId_.store(updateId, std::memory_order_relaxed);
		lastErasedUpdateId_.store(updateId, std::memory_order_relaxed);
		allocatedUpdatesSizeBytes_.fetch_sub(size, std::memory_order_relaxed);
	}
	void OnUpdateHandled(int64_t updateId) noexcept { lastReplicatedUpdateId_.store(updateId, std::memory_order_relaxed); }
	void OnUpdateErased(int64_t updateId, size_t size) noexcept {
		lastErasedUpdateId_.store(updateId, std::memory_order_relaxed);
		allocatedUpdatesSizeBytes_.fetch_sub(size, std::memory_order_relaxed);
	}
	void OnStatusChanged(size_t nodeId, NodeStats::Status status) const noexcept RX_REQUIRES(!mtx_);
	void OnSyncStateChanged(size_t nodeId, NodeStats::SyncState state) noexcept RX_REQUIRES(!mtx_);
	void OnNamespaceSynchronized(size_t nodeId) noexcept RX_REQUIRES(!mtx_);
	void OnEnqueueNamespacesSync(size_t nodeId, size_t count) noexcept RX_REQUIRES(!mtx_);
	void OnServerIdChanged(size_t nodeId, int serverId) const noexcept RX_REQUIRES(!mtx_);
	void OnActiveDsnChanged(size_t nodeId, std::optional<size_t> dsnIdx) const noexcept RX_REQUIRES(!mtx_);
	void SaveNodeError(size_t nodeId, const Error& lastError) noexcept RX_REQUIRES(!mtx_);
	void SaveNodeAsyncReplConfig(size_t nodeId, const AsyncReplConfigStats& stats) noexcept RX_REQUIRES(!mtx_);
	void Clear() noexcept RX_REQUIRES(!mtx_);
	ReplicationStats Get(const ThisNodeOverrides& thisNodeOverrides = {}) const RX_REQUIRES(!mtx_);

private:
	template <typename NodeT>
	static std::unique_ptr<NodeStatsCounter> makeNodeCounter(const NodeT& node, std::vector<std::string> nss) {
		if constexpr (requires { node.IsClusterTarget(); }) {
			if (node.IsClusterTarget()) {
				return std::make_unique<NodeStatsCounter>(node.GetDSNs(), std::move(nss));
			}
			return std::make_unique<NodeStatsCounter>(node.GetDSNs().front(), std::move(nss));
		} else {
			return std::make_unique<NodeStatsCounter>(node.GetRPCDsn(), std::move(nss));
		}
	}
	static int64_t getUpdatesCountById(int64_t lastPushedId, int64_t lastErasedId) noexcept;

	const std::string type_;
	std::atomic<int64_t> updatesDrops_ = {0};
	std::atomic<int64_t> lastPushedUpdateId_ = {-1};
	std::atomic<int64_t> lastErasedUpdateId_ = {-1};
	std::atomic<int64_t> lastReplicatedUpdateId_ = {-1};
	std::atomic<int64_t> allocatedUpdatesSizeBytes_ = {0};
	SyncStatsCounter walSyncs_;
	SyncStatsCounter forceSyncs_;
	SyncStatsCounter initialForceSyncs_;
	SyncStatsCounter initialWalSyncs_;
	std::atomic<size_t> initialSyncTotalTimeUs_ = {0};
	fast_hash_map<size_t, std::unique_ptr<NodeStatsCounter>> nodeCounters_ RX_GUARDED_BY(mtx_);
	std::optional<NodeStatsCounter> thisNode_ RX_GUARDED_BY(mtx_);
	mutable read_write_spinlock mtx_;
};

}  // namespace reindexer::cluster
