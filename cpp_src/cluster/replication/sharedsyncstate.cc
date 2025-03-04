#include "sharedsyncstate.h"

namespace reindexer::cluster {

void SharedSyncState::MarkSynchronized(NamespaceName name) {
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

void SharedSyncState::MarkSynchronized() {
	std::unique_lock<MtxT> lck(mtx_);
	if (current_.role == RaftInfo::Role::Leader) {
		++initialSyncDoneCnt_;
		lck.unlock();
		cond_.notify_all();
	}
}

void SharedSyncState::Reset(ContainerT requireSynchronization, size_t ReplThreadsCnt, bool enabled) {
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

RaftInfo SharedSyncState::TryTransitRole(RaftInfo expected) {
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

void SharedSyncState::SetRole(RaftInfo info) {
	std::lock_guard<MtxT> lck(mtx_);
	next_ = std::move(info);
}

void SharedSyncState::SetTerminated() {
	{
		std::lock_guard<MtxT> lck(mtx_);
		terminated_ = true;
		next_ = current_ = RaftInfo();
	}
	cond_.notify_all();
}

}  // namespace reindexer::cluster
