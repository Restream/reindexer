#pragma once

#include "cluster/logger.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/namespace/namespacename.h"
#include "updates/updatesqueue.h"

namespace reindexer {
namespace cluster {

template <typename T, typename MtxT = read_write_spinlock>
class UpdatesQueuePair {
public:
	using HashT = nocase_hash_str;
	using CompareT = nocase_equal_str;
	using QueueT = updates::UpdatesQueue<T, ReplicationStatsCollector, Logger>;
	using UpdatesContainerT = h_vector<T, 2>;
	constexpr static uint64_t kMaxReplicas = QueueT::kMaxReplicas;

	struct Pair {
		std::shared_ptr<QueueT> sync;
		std::shared_ptr<QueueT> async;
	};

	UpdatesQueuePair(uint64_t maxDataSize)
		: syncQueue_(std::make_shared<QueueT>(maxDataSize)), asyncQueue_(std::make_shared<QueueT>(maxDataSize)) {}

	Pair GetQueue(const NamespaceName &token) const {
		const size_t hash = token.hash();
		Pair result;
		shared_lock<MtxT> lck(mtx_);
		if (syncQueue_->TokenIsInWhiteList(token, hash)) {
			result.sync = syncQueue_;
		}
		if (asyncQueue_->TokenIsInWhiteList(token, hash)) {
			result.async = asyncQueue_;
		}
		return result;
	}
	std::shared_ptr<QueueT> GetSyncQueue() const {
		shared_lock<MtxT> lck(mtx_);
		return syncQueue_;
	}
	std::shared_ptr<QueueT> GetAsyncQueue() const {
		shared_lock<MtxT> lck(mtx_);
		return asyncQueue_;
	}
	template <typename ContainerT>
	void ReinitSyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT> &&allowList, const Logger &l) {
		std::lock_guard<MtxT> lck(mtx_);
		const auto maxDataSize = syncQueue_->MaxDataSize;
		syncQueue_ = std::make_shared<QueueT>(maxDataSize, statsCollector);
		syncQueue_->Init(std::move(allowList), &l);
	}
	template <typename ContainerT>
	void ReinitAsyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT> &&allowList, const Logger &l) {
		std::lock_guard<MtxT> lck(mtx_);
		const auto maxDataSize = asyncQueue_->MaxDataSize;
		asyncQueue_ = std::make_shared<QueueT>(maxDataSize, statsCollector);
		asyncQueue_->Init(std::move(allowList), &l);
	}
	template <typename ContextT>
	std::pair<Error, bool> Push(UpdatesContainerT &&data, std::function<void()> beforeWait, const ContextT &ctx) {
		const auto shardPair = GetQueue(data[0].NsName());
		if (shardPair.sync) {
			if (shardPair.async) {
				shardPair.async->template PushAsync<true>(copyUpdatesContainer(data));
			}
			return shardPair.sync->PushAndWait(std::move(data), std::move(beforeWait), ctx);
		} else if (shardPair.async) {
			return shardPair.async->template PushAsync<true>(std::move(data));
		}
		return std::make_pair(Error(), false);
	}
	std::pair<Error, bool> PushNowait(UpdatesContainerT &&data) {
		const auto shardPair = GetQueue(data[0].NsName());
		if (shardPair.sync) {
			if (shardPair.async) {
				shardPair.async->template PushAsync<true>(copyUpdatesContainer(data));
			}
			return shardPair.sync->template PushAsync<false>(std::move(data));
		} else if (shardPair.async) {
			return shardPair.async->template PushAsync<true>(std::move(data));
		}
		return std::make_pair(Error(), false);
	}
	std::pair<Error, bool> PushAsync(UpdatesContainerT &&data) {
		std::shared_ptr<QueueT> shard;
		{
			std::string_view token(data[0].NsName());
			const HashT h;
			const size_t hash = h(token);
			shared_lock<MtxT> lck(mtx_);
			if (!asyncQueue_->TokenIsInWhiteList(token, hash)) {
				return std::make_pair(Error(), false);
			}
			shard = asyncQueue_;
		}
		return shard->template PushAsync<true>(std::move(data));
	}

private:
	UpdatesContainerT copyUpdatesContainer(const UpdatesContainerT &data) {
		UpdatesContainerT copy;
		copy.reserve(data.size());
		for (auto &d : data) {
			// async replication should not see emmiter
			copy.emplace_back(d.template Clone<T::ClonePolicy::WithoutEmmiter>());
		}
		return copy;
	}

	mutable MtxT mtx_;
	std::shared_ptr<QueueT> syncQueue_;
	std::shared_ptr<QueueT> asyncQueue_;
};

}  // namespace cluster
}  // namespace reindexer
