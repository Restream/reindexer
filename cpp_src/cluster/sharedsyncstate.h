#pragma once

#include <mutex>
#include "cluster/config.h"
#include "estl/contexted_cond_var.h"
#include "estl/fast_hash_set.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "estl/string_view.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace cluster {

template <typename MtxT = shared_timed_mutex>
class SharedSyncState {
public:
	using GetNameF = std::function<std::string()>;
	using ContainerT = fast_hash_set<std::string, nocase_hash_str, nocase_equal_str>;

	void MarkSynchronized(std::string name, bool isSynchronized) {
		std::lock_guard<MtxT> lck(mtx_);
		if (isSynchronized) {
			if (current_.role == RaftInfo::Role::Leader) {
				synchronized_.emplace(std::move(name));
				cond_.notify_all();
			}
		} else {
			synchronized_.erase(name);
		}
	}
	void Rename(string_view oldName, string_view newName) {
		std::string str(newName);
		std::lock_guard<MtxT> lck(mtx_);
		synchronized_.erase(oldName);
		synchronized_.emplace(std::move(str));
		cond_.notify_all();
	}
	void Reset(ContainerT&& requireSynchronization, bool enabled) {
		std::lock_guard<MtxT> lck(mtx_);
		requireSynchronization_ = std::move(requireSynchronization);
		synchronized_.clear();
		enabled_ = enabled;
		terminated_ = false;
		leaderId_ = -1;
		next_ = current_ = RaftInfo();
	}
	template <typename ContextT>
	void AwaitSynchronization(string_view name, const ContextT& ctx) const {
		nocase_hash_str h;
		std::size_t hash = h(name);
		shared_lock<MtxT> lck(mtx_);
		while (!isSynchronized(name, hash)) {
			if (terminated_) {
				throw Error(errTerminated, "Cluster was terimated");
			}
			cond_.wait(lck, ctx);
		}
	}
	bool IsSynchronized(string_view name) const {
		nocase_hash_str h;
		std::size_t hash = h(name);
		shared_lock<MtxT> lck(mtx_);
		return isSynchronized(name, hash);
	}
	RaftInfo TryTransitRole(RaftInfo expected) {
		std::lock_guard<MtxT> lck(mtx_);
		if (expected == next_) {
			if (current_.role == RaftInfo::Role::Leader && current_.role != next_.role) {
				synchronized_.clear();
			}
			current_ = next_;
			cond_.notify_all();
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
		std::lock_guard<MtxT> lck(mtx_);
		terminated_ = true;
		next_ = current_ = RaftInfo();
		cond_.notify_all();
	}

private:
	bool isSynchronized(string_view name, std::size_t hash) const {
		return !isRequireSync(name, hash) || (current_.role == RaftInfo::Role::Leader && synchronized_.count(name, hash));
	}
	bool isRequireSync(string_view name, size_t hash) const noexcept {
		return enabled_ && (requireSynchronization_.empty() || requireSynchronization_.count(name, hash));
	}
	bool isRunning() const noexcept { return enabled_ && !terminated_; }

	mutable MtxT mtx_;
	mutable contexted_cond_var cond_;
	ContainerT synchronized_;
	ContainerT requireSynchronization_;
	bool enabled_ = false;
	int leaderId_ = -1;
	RaftInfo current_;
	RaftInfo next_;
	bool terminated_ = false;
};
}  // namespace cluster
}  // namespace reindexer
