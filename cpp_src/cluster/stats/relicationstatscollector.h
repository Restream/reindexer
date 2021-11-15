#pragma once

#include "replicationstats.h"

namespace reindexer {
namespace cluster {

class ReplicationStatsCollector {
public:
	ReplicationStatsCollector() = default;
	ReplicationStatsCollector(std::string type) : counter_(new ReplicationStatCounter(std::move(type))), owner_(true) {}
	ReplicationStatsCollector(const ReplicationStatsCollector& o) noexcept : counter_(o.counter_), owner_(false) {}
	~ReplicationStatsCollector() {
		if (owner_) {
			delete counter_;
		}
	}
	ReplicationStatsCollector& operator=(const ReplicationStatsCollector& o) {
		if (this != &o) {
			if (owner_) {
				delete counter_;
			}
			owner_ = false;
			counter_ = o.counter_;
		}
		return *this;
	}

	template <typename NodeT>
	void Init(const std::vector<NodeT>& nodes) {
		if (counter_) counter_->Init(nodes);
	}
	template <typename NodeT>
	void Init(const NodeT& thisNode, const std::vector<NodeT>& nodes, const std::vector<std::string>& namespaces) {
		if (counter_) counter_->Init(thisNode, nodes, namespaces);
	}

	void OnWalSync(std::chrono::microseconds time) noexcept { counter_->OnWalSync(time); }
	void OnForceSync(std::chrono::microseconds time) noexcept { counter_->OnForceSync(time); }
	void OnInitialWalSync(std::chrono::microseconds time) noexcept { counter_->OnInitialWalSync(time); }
	void OnInitialForceSync(std::chrono::microseconds time) noexcept { counter_->OnInitialForceSync(time); }
	void OnInitialSyncDone(std::chrono::microseconds time) noexcept { counter_->OnInitialSyncDone(time); }
	void OnUpdatePushed(int64_t updateId, size_t size) noexcept { counter_->OnUpdatePushed(updateId, size); }
	void OnUpdateApplied(size_t nodeId, int64_t updateId) noexcept { counter_->OnUpdateApplied(nodeId, updateId); }
	void OnUpdatesDrop(int64_t updateId, size_t size) noexcept { counter_->OnUpdatesDrop(updateId, size); }
	void OnUpdateReplicated(int64_t updateId) noexcept { counter_->OnUpdateReplicated(updateId); }
	void OnUpdateErased(int64_t updateId, size_t size) noexcept { counter_->OnUpdateErased(updateId, size); }
	void OnStatusChanged(size_t nodeId, NodeStats::Status status) { counter_->OnStatusChanged(nodeId, status); }
	void OnSyncStateChanged(size_t nodeId, NodeStats::SyncState state) { counter_->OnSyncStateChanged(nodeId, state); }
	void OnServerIdChanged(size_t nodeId, int serverId) noexcept { counter_->OnServerIdChanged(nodeId, serverId); }
	void Reset() { counter_->Reset(); }
	ReplicationStats Get() const {
		if (counter_) {
			return counter_->Get();
		} else {
			return ReplicationStats();
		}
	}

private:
	ReplicationStatCounter* counter_ = nullptr;
	bool owner_ = false;
};

class SyncTimeCounter {
public:
	enum class Type { ForceSync, WalSync, InitialForceSync, InitialWalSync };

	SyncTimeCounter(Type type, ReplicationStatsCollector& statsCollector) noexcept
		: tmStart_(std::chrono::high_resolution_clock::now()), statsCollector_(statsCollector), type_(type) {}
	void SetType(Type type) noexcept { type_ = type; }
	~SyncTimeCounter() {
		std::chrono::microseconds time =
			std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - tmStart_);
		switch (type_) {
			case Type::ForceSync:
				statsCollector_.OnForceSync(time);
				break;
			case Type::WalSync:
				statsCollector_.OnWalSync(time);
				break;
			case Type::InitialForceSync:
				statsCollector_.OnInitialForceSync(time);
				break;
			case Type::InitialWalSync:
				statsCollector_.OnInitialWalSync(time);
				break;
		}
	}

private:
	const std::chrono::time_point<std::chrono::high_resolution_clock> tmStart_;
	ReplicationStatsCollector& statsCollector_;
	Type type_;
};

}  // namespace cluster
}  // namespace reindexer
