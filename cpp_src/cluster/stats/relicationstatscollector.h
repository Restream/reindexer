#pragma once

#include "replicationstats.h"
#include "tools/clock.h"

namespace reindexer {
namespace cluster {

class [[nodiscard]] ReplicationStatsCollector {
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
		if (counter_) {
			counter_->Init(nodes);
		}
	}
	template <typename NodeT>
	void Init(const NodeT& thisNode, const std::vector<NodeT>& nodes, const std::vector<std::string>& namespaces) {
		if (counter_) {
			counter_->Init(thisNode, nodes, namespaces);
		}
	}

	void OnWalSync(std::chrono::microseconds time) noexcept {
		if (counter_) {
			counter_->OnWalSync(time);
		}
	}
	void OnForceSync(std::chrono::microseconds time) noexcept {
		if (counter_) {
			counter_->OnForceSync(time);
		}
	}
	void OnInitialWalSync(std::chrono::microseconds time) noexcept {
		if (counter_) {
			counter_->OnInitialWalSync(time);
		}
	}
	void OnInitialForceSync(std::chrono::microseconds time) noexcept {
		if (counter_) {
			counter_->OnInitialForceSync(time);
		}
	}
	void OnInitialSyncDone(std::chrono::microseconds time) noexcept {
		if (counter_) {
			counter_->OnInitialSyncDone(time);
		}
	}
	void OnUpdatePushed(int64_t updateId, size_t size) noexcept {
		if (counter_) {
			counter_->OnUpdatePushed(updateId, size);
		}
	}
	void OnUpdateApplied(size_t nodeId, int64_t updateId) noexcept {
		if (counter_) {
			counter_->OnUpdateApplied(nodeId, updateId);
		}
	}
	void OnUpdatesDrop(int64_t updateId, size_t size) noexcept {
		if (counter_) {
			counter_->OnUpdatesDrop(updateId, size);
		}
	}
	void OnUpdateHandled(int64_t updateId) noexcept {
		if (counter_) {
			counter_->OnUpdateHandled(updateId);
		}
	}
	void OnUpdateErased(int64_t updateId, size_t size) noexcept {
		if (counter_) {
			counter_->OnUpdateErased(updateId, size);
		}
	}
	void OnStatusChanged(size_t nodeId, NodeStats::Status status) {
		if (counter_) {
			counter_->OnStatusChanged(nodeId, status);
		}
	}
	void OnSyncStateChanged(size_t nodeId, NodeStats::SyncState state) {
		if (counter_) {
			counter_->OnSyncStateChanged(nodeId, state);
		}
	}
	void OnServerIdChanged(size_t nodeId, int serverId) noexcept {
		if (counter_) {
			counter_->OnServerIdChanged(nodeId, serverId);
		}
	}
	void SaveNodeError(size_t nodeId, const Error& err) {
		if (counter_) {
			counter_->SaveNodeError(nodeId, err);
		}
	}
	void Clear() {
		if (counter_) {
			counter_->Clear();
		}
	}
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

class [[nodiscard]] SyncTimeCounter {
public:
	enum class [[nodiscard]] Type { ForceSync, WalSync, InitialForceSync, InitialWalSync };

	SyncTimeCounter(Type type, ReplicationStatsCollector& statsCollector) noexcept
		: tmStart_(steady_clock_w::now()), statsCollector_(statsCollector), type_(type) {}
	void SetType(Type type) noexcept { type_ = type; }
	~SyncTimeCounter() {
		std::chrono::microseconds time = std::chrono::duration_cast<std::chrono::microseconds>(steady_clock_w::now() - tmStart_);
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
	const steady_clock_w::time_point tmStart_;
	ReplicationStatsCollector& statsCollector_;
	Type type_;
};

}  // namespace cluster
}  // namespace reindexer
