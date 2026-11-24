#pragma once

#include <functional>
#include <optional>
#include "cluster/config.h"
#include "core/namespace/namespacename.h"
#include "estl/contexted_cond_var.h"
#include "estl/fast_hash_map.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "estl/thread_annotation_attributes.h"

namespace reindexer::cluster {

static constexpr size_t k16kCoroStack = 16 * 1024;

struct [[nodiscard]] AsyncReplPermission {
	Error error;
	// Is incremented on each change of the permission
	uint64_t epoch = 0;
};

class [[nodiscard]] SharedSyncState {
	using MtxT = shared_timed_mutex;

public:
	using ContainerT = NsNamesHashSetT;
	// The newest config of the cluster, which the leader has failed to apply during its initial sync
	struct [[nodiscard]] RejectedAsyncReplConfig {
		Error CauseWithContext() const noexcept;

		int64_t sourceId;
		Error cause;
	};

	void MarkSynchronized(NamespaceName name) RX_REQUIRES(!mtx_);
	void MarkSynchronized() noexcept RX_REQUIRES(!mtx_);
	// The config is kept after the cluster's stop: until the next start the node rejects writes to the cluster's namespaces and does
	// not replicate asynchronously
	void Configure(const ClusterConfigData& config) RX_REQUIRES(!mtx_);
	void Reset(size_t replThreadsCnt) noexcept RX_REQUIRES(!mtx_);
	template <typename ContextT>
	void AwaitInitialSync(const NamespaceName& name, const ContextT& ctx) const RX_REQUIRES(!mtx_) {
		assertrx_dbg(!name.empty());
		awaitInitialSync([this, &name]() RX_REQUIRES_SHARED(mtx_) noexcept { return isInitialSyncDone(name); }, ctx);
	}
	template <typename ContextT>
	void AwaitInitialSync(const ContextT& ctx) const RX_REQUIRES(!mtx_) {
		awaitInitialSync([this]() RX_REQUIRES_SHARED(mtx_) noexcept { return isInitialSyncDone(); }, ctx);
	}
	bool IsInitialSyncDone(const NamespaceName& name) const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return isInitialSyncDone(name);
	}
	bool IsInitialSyncDone() const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return isInitialSyncDone();
	}
	bool IsClusterNamespace(std::string_view name) const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return isRequireSync(name);
	}
	RaftInfo TryTransitRole(RaftInfo expected) noexcept RX_REQUIRES(!mtx_);
	template <typename ContextT>
	RaftInfo AwaitRole(bool allowTransitState, const ContextT& ctx) const RX_REQUIRES(!mtx_) {
		assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());

		shared_lock lck(mtx_);
		if (allowTransitState) {
			cond_.wait(lck, [this]() RX_REQUIRES_SHARED(mtx_) { return !running_ || next_ == current_; }, ctx);
		} else {
			cond_.wait(
				lck,
				[this]() RX_REQUIRES_SHARED(mtx_) {
					return !running_ ||
						   (next_ == current_ && (current_.role == RaftInfo::Role::Leader || current_.role == RaftInfo::Role::Follower));
				},
				ctx);
		}
		return current_;
	}
	void SetRole(RaftInfo info) noexcept RX_REQUIRES(!mtx_);
	std::pair<RaftInfo, RaftInfo> GetRolesPair() const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return std::make_pair(current_, next_);
	}
	RaftInfo CurrentRole() const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return current_;
	}
	void SetTerminated() noexcept RX_REQUIRES(!mtx_);
	// Is reset on the leader's role switch and by the successful applying of the cluster's config: on the initial sync's retry or by the
	// action 'apply_async_replication_config'
	void SetRejectedAsyncReplConfig(RejectedAsyncReplConfig&& config) noexcept RX_REQUIRES(!mtx_);
	void ResetRejectedAsyncReplConfig() noexcept RX_REQUIRES(!mtx_);
	std::optional<RejectedAsyncReplConfig> GetRejectedAsyncReplConfig() const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return rejectedAsyncReplConfig_;
	}
	bool IsRaftCluster() const noexcept RX_REQUIRES(!mtx_) {
		shared_lock lck(mtx_);
		return isRaftCluster_;
	}
	AsyncReplPermission GetAsyncReplPermission() const noexcept RX_REQUIRES(!mtx_);
	// The observer is called by the thread, which has changed the async replication permission, and must not throw or lock anything
	void AddAsyncReplPermissionObserver(const void* key, std::function<void()> observer) {
		asyncReplPermissionObservers_.Add(key, std::move(observer));
	}
	// Awaits the observer's call in progress
	void RemoveAsyncReplPermissionObserver(const void* key) noexcept { asyncReplPermissionObservers_.Remove(key); }

private:
	class [[nodiscard]] Observers {
	public:
		void Add(const void* key, std::function<void()> observer) RX_REQUIRES(!mtx_);
		void Remove(const void* key) noexcept RX_REQUIRES(!mtx_);
		void Notify() noexcept RX_REQUIRES(!mtx_);

	private:
		mutex mtx_;
		fast_hash_map<const void*, std::function<void()>> observers_ RX_GUARDED_BY(mtx_);
	};

	template <typename PredicateT, typename ContextT>
	void awaitInitialSync(const PredicateT& isDone, const ContextT& ctx) const RX_REQUIRES(!mtx_) {
		assertrx_dbg(ctx.GetOriginLSN().isEmpty() || ctx.IsCancelable());

		shared_lock lck(mtx_);
		while (!isDone()) {
			if (!running_) {
				throw Error(errTerminated, "RAFT cluster is not running");
			}
			if (next_.role == RaftInfo::Role::Follower) {
				throw Error(errWrongReplicationData, "Node role was changed to follower");
			}
			cond_.wait(
				lck,
				[this, &isDone]()
					RX_REQUIRES_SHARED(mtx_) noexcept { return isDone() || !running_ || next_.role == RaftInfo::Role::Follower; },
				ctx);
		}
	}
	Error checkAsyncReplicationAllowed() const noexcept RX_REQUIRES_SHARED(mtx_);
	// Modifies the state under the lock, wakes up the waiters and notifies the observers, if the async replication permission is changed
	template <typename F>
	void modify(F&& modifier) noexcept RX_REQUIRES(!mtx_);
	// Has to be called under the unique lock by each modifier of the permission's inputs. Returns true, if the permission is changed
	bool updateAsyncReplPermission() noexcept RX_REQUIRES(mtx_);
	bool isInitialSyncDone(const NamespaceName& name) const noexcept RX_REQUIRES_SHARED(mtx_) {
		assertrx_dbg(!name.empty());
		return !isRequireSync(name) || (current_.role == RaftInfo::Role::Leader && synchronized_.count(name));
	}
	bool isInitialSyncDone() const noexcept RX_REQUIRES_SHARED(mtx_) {
		return !isRaftCluster_ || (next_.role == RaftInfo::Role::Leader && initialSyncDoneCnt_ == replThreadsCnt_);
	}
	template <typename NameT>
	bool isRequireSync(const NameT& name) const noexcept RX_REQUIRES_SHARED(mtx_) {
		assertrx_dbg(!name.empty());
		return isRaftCluster_ && (requireSynchronization_.empty() || requireSynchronization_.count(name));
	}

	mutable MtxT mtx_;
	mutable contexted_cond_var cond_;
	ContainerT synchronized_ RX_GUARDED_BY(mtx_);
	ContainerT requireSynchronization_ RX_GUARDED_BY(mtx_);
	bool isRaftCluster_ RX_GUARDED_BY(mtx_) = false;
	RaftInfo current_ RX_GUARDED_BY(mtx_);
	RaftInfo next_ RX_GUARDED_BY(mtx_);
	bool running_ RX_GUARDED_BY(mtx_) = false;
	size_t initialSyncDoneCnt_ RX_GUARDED_BY(mtx_) = 0;
	size_t replThreadsCnt_ RX_GUARDED_BY(mtx_) = 0;
	std::optional<RejectedAsyncReplConfig> rejectedAsyncReplConfig_ RX_GUARDED_BY(mtx_);
	bool asyncReplAllowed_ RX_GUARDED_BY(mtx_) = true;
	uint64_t asyncReplPermissionEpoch_ RX_GUARDED_BY(mtx_) = 0;
	Observers asyncReplPermissionObservers_;
};

}  // namespace reindexer::cluster
