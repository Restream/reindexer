#pragma once

#include "cluster/config.h"
#include "core/namespace/namespacename.h"
#include "estl/contexted_cond_var.h"
#include "estl/fast_hash_set.h"
#include "estl/shared_mutex.h"

namespace reindexer {
namespace cluster {

static constexpr size_t k16kCoroStack = 16 * 1024;

template <typename MtxT = shared_timed_mutex>
class SharedSyncState {
public:
	using GetNameF = std::function<std::string()>;
	using ContainerT = NsNamesHashSetT;

	void MarkSynchronized(NamespaceName name) {
		std::unique_lock<MtxT> lck(mtx_);
		assertrx_dbg(!name.empty());
		if (current_.role == RaftInfo::Role::Leader) {
			auto res = synchronized_.emplace(std::move(name));
			lck.unlock();
			if (res.second) {
				cond_.notify_all();
			}
		}
	}
	void MarkSynchronized() {
		std::unique_lock<MtxT> lck(mtx_);
		if (current_.role == RaftInfo::Role::Leader) {
			++initialSyncDoneCnt_;
			lck.unlock();
			cond_.notify_all();
		}
	}
	void Reset(ContainerT requireSynchronization, size_t ReplThreadsCnt, bool enabled) {
		std::lock_guard<MtxT> lck(mtx_);
		requireSynchronization_ = std::move(requireSynchronization);
		synchronized_.clear();
		enabled_ = enabled;
		terminated_ = false;
		initialSyncDoneCnt_ = 0;
		ReplThreadsCnt_ = ReplThreadsCnt;
		next_ = current_ = RaftInfo();
		assert(ReplThreadsCnt_);
	}
	template <typename ContextT>
	void AwaitInitialSync(const NamespaceName& name, const ContextT& ctx) const {
		shared_lock<MtxT> lck(mtx_);
		assertrx_dbg(!name.empty());
		while (!isInitialSyncDone(name)) {
			if (terminated_) {
				throw Error(errTerminated, "Cluster was terminated");
			}
			if (next_.role == RaftInfo::Role::Follower) {
				throw Error(errWrongReplicationData, "Node role was changed to follower");
			}
			cond_.wait(
				lck, [this, &name]() noexcept { return isInitialSyncDone(name) || terminated_ || next_.role == RaftInfo::Role::Follower; },
				ctx);
		}
	}
	template <typename ContextT>
	void AwaitInitialSync(const ContextT& ctx) const {
		shared_lock<MtxT> lck(mtx_);
		while (!isInitialSyncDone()) {
			if (terminated_) {
				throw Error(errTerminated, "Cluster was terminated");
			}
			if (next_.role == RaftInfo::Role::Follower) {
				throw Error(errWrongReplicationData, "Node role was changed to follower");
			}
			cond_.wait(
				lck, [this]() noexcept { return isInitialSyncDone() || terminated_ || next_.role == RaftInfo::Role::Follower; }, ctx);
		}
	}
	bool IsInitialSyncDone(const NamespaceName& name) const {
		shared_lock<MtxT> lck(mtx_);
		return isInitialSyncDone(name);
	}
	bool IsInitialSyncDone() const {
		shared_lock<MtxT> lck(mtx_);
		return isInitialSyncDone();
	}
	RaftInfo TryTransitRole(RaftInfo expected) {
		std::unique_lock<MtxT> lck(mtx_);
		if (expected == next_) {
			if (current_.role == RaftInfo::Role::Leader && current_.role != next_.role) {
				synchronized_.clear();
				initialSyncDoneCnt_ = 0;
			}
			current_ = next_;
			lck.unlock();
			cond_.notify_all();
			return expected;
		}
		return next_;
	}
	template <typename ContextT>
	RaftInfo AwaitRole(bool allowTransitState, const ContextT& ctx) const {
		shared_lock<MtxT> lck(mtx_);
		if (allowTransitState) {
			cond_.wait(
				lck, [this] { return !isRunning() || next_ == current_; }, ctx);
		} else {
			cond_.wait(
				lck,
				[this] {
					return !isRunning() ||
						   (next_ == current_ && (current_.role == RaftInfo::Role::Leader || current_.role == RaftInfo::Role::Follower));
				},
				ctx);
		}
		return current_;
	}
	void SetRole(RaftInfo info) {
		std::lock_guard<MtxT> lck(mtx_);
		next_ = info;
	}
	std::pair<RaftInfo, RaftInfo> GetRolesPair() const {
		shared_lock<MtxT> lck(mtx_);
		return std::make_pair(current_, next_);
	}
	RaftInfo CurrentRole() const {
		shared_lock<MtxT> lck(mtx_);
		return current_;
	}
	void SetTerminated() {
		{
			std::lock_guard<MtxT> lck(mtx_);
			terminated_ = true;
			next_ = current_ = RaftInfo();
		}
		cond_.notify_all();
	}

private:
	bool isInitialSyncDone(const NamespaceName& name) const {
		assertrx_dbg(!name.empty());
		return !isRequireSync(name) || (current_.role == RaftInfo::Role::Leader && synchronized_.count(name));
	}
	bool isInitialSyncDone() const noexcept {
		return !enabled_ || (next_.role == RaftInfo::Role::Leader && initialSyncDoneCnt_ == ReplThreadsCnt_);
	}
	bool isRequireSync(const NamespaceName& name) const noexcept {
		assertrx_dbg(!name.empty());
		return enabled_ && (requireSynchronization_.empty() || requireSynchronization_.count(name));
	}
	bool isRunning() const noexcept { return enabled_ && !terminated_; }

	mutable MtxT mtx_;
	mutable contexted_cond_var cond_;
	ContainerT synchronized_;
	ContainerT requireSynchronization_;
	bool enabled_ = false;
	RaftInfo current_;
	RaftInfo next_;
	bool terminated_ = false;
	size_t initialSyncDoneCnt_ = 0;
	size_t ReplThreadsCnt_ = 0;
};
}  // namespace cluster
}  // namespace reindexer
