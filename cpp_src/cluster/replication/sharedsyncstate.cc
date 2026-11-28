#include "sharedsyncstate.h"
#include "estl/lock.h"
#include "tools/catch_and_return.h"

namespace reindexer::cluster {

template <typename F>
void SharedSyncState::modify(F&& modifier) noexcept {
	bool permissionChanged = false;
	{
		lock_guard lck(mtx_);
		modifier();
		permissionChanged = updateAsyncReplPermission();
	}
	cond_.notify_all();
	if (permissionChanged) {
		asyncReplPermissionObservers_.Notify();
	}
}

void SharedSyncState::MarkSynchronized(NamespaceName name) {
	unique_lock lck(mtx_);
	assertrx_dbg(!name.empty());
	if (current_.role == RaftInfo::Role::Leader) {
		auto res = synchronized_.emplace(std::move(name));
		lck.unlock();
		if (res.second) {
			cond_.notify_all();
		}
	}
}

void SharedSyncState::MarkSynchronized() noexcept {
	modify([this]() RX_REQUIRES(mtx_) {
		if (current_.role == RaftInfo::Role::Leader) {
			++initialSyncDoneCnt_;
		}
	});
}

void SharedSyncState::Configure(const ClusterConfigData& config) {
	ContainerT requireSynchronization = config.namespaces;
	modify([&]() RX_REQUIRES(mtx_) {
		isRaftCluster_ = !config.nodes.empty();
		requireSynchronization_ = std::move(requireSynchronization);
	});
}

void SharedSyncState::Reset(size_t replThreadsCnt) noexcept {
	modify([&]() RX_REQUIRES(mtx_) {
		synchronized_.clear();
		running_ = true;
		initialSyncDoneCnt_ = 0;
		replThreadsCnt_ = replThreadsCnt;
		next_ = current_ = RaftInfo();
		rejectedAsyncReplConfig_.reset();
		assert(replThreadsCnt_);
	});
}

RaftInfo SharedSyncState::TryTransitRole(RaftInfo expected) noexcept {
	RaftInfo actual;
	modify([&]() RX_REQUIRES(mtx_) {
		if (expected != next_) {
			actual = next_;
			return;
		}
		if (current_.role == RaftInfo::Role::Leader && current_.role != next_.role) {
			synchronized_.clear();
			initialSyncDoneCnt_ = 0;
			rejectedAsyncReplConfig_.reset();
		}
		current_ = next_;
		actual = expected;
	});
	return actual;
}

void SharedSyncState::SetRole(RaftInfo info) noexcept {
	modify([&]() RX_REQUIRES(mtx_) { next_ = std::move(info); });
}

void SharedSyncState::SetTerminated() noexcept {
	modify([this]() RX_REQUIRES(mtx_) {
		running_ = false;
		next_ = current_ = RaftInfo();
		rejectedAsyncReplConfig_.reset();
	});
}

Error SharedSyncState::RejectedAsyncReplConfig::CauseWithContext() const noexcept {
	return Error(errParams, "Newest async replication config of the cluster is not applied: {}", cause.whatStr());
}

void SharedSyncState::SetRejectedAsyncReplConfig(RejectedAsyncReplConfig&& config) noexcept {
	modify([&]() RX_REQUIRES(mtx_) {
		if (current_.role == RaftInfo::Role::Leader) {
			rejectedAsyncReplConfig_ = std::move(config);
		}
	});
}

void SharedSyncState::ResetRejectedAsyncReplConfig() noexcept {
	modify([this]() RX_REQUIRES(mtx_) { rejectedAsyncReplConfig_.reset(); });
}

AsyncReplPermission SharedSyncState::GetAsyncReplPermission() const noexcept {
	shared_lock lck(mtx_);
	return {.error = checkAsyncReplicationAllowed(), .epoch = asyncReplPermissionEpoch_};
}

Error SharedSyncState::checkAsyncReplicationAllowed() const noexcept {
	try {
		if (current_.role != next_.role || (current_.role != RaftInfo::Role::Leader && current_.role != RaftInfo::Role::None)) {
			return Error(errParams, "Current node has roles '{}:{}', but role 'leader' (or 'none') is required to replicate",
						 RaftInfo::RoleToStr(current_.role), RaftInfo::RoleToStr(next_.role));
		}
		if (current_.role == RaftInfo::Role::None && isRaftCluster_) {
			return Error(
				errParams,
				"Node of the RAFT cluster has role 'none' (the cluster is not running), but role 'leader' is required to replicate");
		}
		// The config may be replaced during the leader's initial sync
		if (current_.role == RaftInfo::Role::Leader && !isInitialSyncDone()) {
			return Error(errParams, "Initial sync of the RAFT leader is not done yet");
		}
		if (rejectedAsyncReplConfig_) {
			return rejectedAsyncReplConfig_->CauseWithContext();
		}
	} CATCH_AND_RETURN;
	return Error();
}

bool SharedSyncState::updateAsyncReplPermission() noexcept {
	const bool allowed = checkAsyncReplicationAllowed().ok();
	if (allowed == asyncReplAllowed_) {
		return false;
	}
	asyncReplAllowed_ = allowed;
	++asyncReplPermissionEpoch_;
	return true;
}

void SharedSyncState::Observers::Add(const void* key, std::function<void()> observer) {
	lock_guard lck(mtx_);
	observers_[key] = std::move(observer);
}

void SharedSyncState::Observers::Remove(const void* key) noexcept {
	lock_guard lck(mtx_);
	observers_.erase(key);
}

void SharedSyncState::Observers::Notify() noexcept {
	lock_guard lck(mtx_);
	for (const auto& [key, observer] : observers_) {
		if (observer) {
			observer();
		}
	}
}

}  // namespace reindexer::cluster
