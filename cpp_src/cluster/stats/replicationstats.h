#pragma once

#include <optional>
#include "cluster/config.h"
#include "estl/fast_hash_map.h"
#include "estl/lock.h"
#include "estl/shared_mutex.h"
#include "estl/spin_lock.h"

namespace reindexer {
namespace cluster {

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

struct [[nodiscard]] NodeStats {
	enum class [[nodiscard]] Status { None, Offline, Online, RaftError };
	enum class [[nodiscard]] SyncState { None, Syncing, AwaitingResync, OnlineReplication, InitialLeaderSync };

	void FromJSON(const gason::JsonNode&);
	void GetJSON(JsonBuilder& builder) const;
	bool operator==(const NodeStats& r) const noexcept = default;
	bool operator!=(const NodeStats& r) const noexcept = default;

	DSN dsn;
	int64_t updatesCount;
	int serverId;
	Status status;
	SyncState syncState;
	RaftInfo::Role role;
	bool isSynchronized;
	std::vector<std::string> namespaces;
	Error lastError;
};

struct [[nodiscard]] ReplicationStats {
	Error FromJSON(std::span<char> json);
	Error FromJSON(const gason::JsonNode& root);
	void GetJSON(JsonBuilder& builder) const;
	void GetJSON(WrSerializer& ser) const;
	bool operator==(const ReplicationStats& r) const noexcept = default;
	bool operator!=(const ReplicationStats& r) const noexcept = default;

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
	void Hit(std::chrono::microseconds time) noexcept;
	void Reset() noexcept;
	SyncStats Get() const;

	size_t count = 0;
	int64_t maxTimeUs = 0;
	int64_t totalTimeUs = 0;
	mutable spinlock mtx_;
};

struct [[nodiscard]] NodeStatsCounter {
	NodeStatsCounter(DSN d, std::vector<std::string> nss) : dsn(std::move(d)), namespaces(std::move(nss)) {}
	void OnUpdateApplied(int64_t updateId) noexcept { lastAppliedUpdateId_.store(updateId, std::memory_order_relaxed); }
	void OnStatusChanged(NodeStats::Status st) noexcept { status.store(st, std::memory_order_relaxed); }
	void OnSyncStateChanged(NodeStats::SyncState st) noexcept { syncState.store(st, std::memory_order_relaxed); }
	void OnServerIdChanged(int sId) noexcept { serverId.store(sId, std::memory_order_relaxed); }
	void SaveLastError(const Error& err) noexcept;
	Error GetLastError() const;
	NodeStats Get() const;

	const DSN dsn;
	const std::vector<std::string> namespaces;
	std::atomic<int64_t> lastAppliedUpdateId_ = {-1};
	std::atomic<int> serverId = {-1};
	std::atomic<NodeStats::Status> status = {NodeStats::Status::None};
	std::atomic<NodeStats::SyncState> syncState = {NodeStats::SyncState::None};
	Error lastError;  // Change under lock
	mutable spinlock mtx_;
};

class [[nodiscard]] ReplicationStatCounter {
public:
	static constexpr size_t kLeaderUID = std::numeric_limits<size_t>::max();

	ReplicationStatCounter(std::string t) : type_(std::move(t)) {}
	template <typename NodeT>
	void Init(const std::vector<NodeT>& nodes) {
		lock_guard wlck(mtx_);
		nodeCounters_.clear();
		thisNode_.reset();
		nodeCounters_.reserve(nodes.size());
		for (size_t i = 0; i < nodes.size(); ++i) {
			nodeCounters_.emplace(i, std::make_unique<NodeStatsCounter>(nodes[i].GetRPCDsn(), nodes[i].GetNssVector()));
		}
	}
	template <typename NodeT>
	void Init(const NodeT& thisNode, const std::vector<NodeT>& nodes, const std::vector<std::string>& namespaces) {
		lock_guard wlck(mtx_);
		nodeCounters_.clear();
		thisNode_.emplace(thisNode.GetRPCDsn(), namespaces);
		thisNode_->serverId.store(thisNode.serverId, std::memory_order_relaxed);
		thisNode_->status.store(NodeStats::Status::Online, std::memory_order_relaxed);
		nodeCounters_.reserve(nodes.size());
		for (size_t i = 0; i < nodes.size(); ++i) {
			nodeCounters_.emplace(i, std::make_unique<NodeStatsCounter>(nodes[i].GetRPCDsn(), namespaces));
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
	void OnServerIdChanged(size_t nodeId, int serverId) const noexcept RX_REQUIRES(!mtx_);
	void SaveNodeError(size_t nodeId, const Error& lastError) noexcept RX_REQUIRES(!mtx_);
	void Clear() noexcept;
	ReplicationStats Get() const RX_REQUIRES(!mtx_);

private:
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
	fast_hash_map<size_t, std::unique_ptr<NodeStatsCounter>> nodeCounters_;
	std::optional<NodeStatsCounter> thisNode_;
	mutable read_write_spinlock mtx_;
};

}  // namespace cluster
}  // namespace reindexer
