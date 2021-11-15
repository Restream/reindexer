#pragma once

#include "updatesqueue.h"

namespace reindexer {
namespace cluster {

template <typename T, typename MtxT = read_write_spinlock>
class UpdatesQueuePair {
public:
	using HashT = nocase_hash_str;
	using CompareT = nocase_equal_str;
	using QueueT = UpdatesQueue<T>;
	using UpdatesContainerT = h_vector<T, 2>;

	enum class TokenType { Sync, Async, None };

	UpdatesQueuePair(uint64_t maxDataSize)
		: syncQueue_(std::make_shared<QueueT>(maxDataSize)), asyncQueue_(std::make_shared<QueueT>(maxDataSize)) {}

	std::pair<std::shared_ptr<QueueT>, TokenType> GetQueue(const std::string &token) {
		// HashT h;
		// std::size_t hash = h(token);
		{
			shared_lock<MtxT> lck(mtx_);
			std::shared_ptr<QueueT> shard;
			const auto type = getTokenType(token);
			switch (getTokenType(token)) {	//, hash);
				case TokenType::Sync:
					return std::make_pair(syncQueue_, type);
				case TokenType::Async:
					return std::make_pair(asyncQueue_, type);
				default:;
			}
		}
		return std::make_pair(std::shared_ptr<UpdatesQueue<T>>(), TokenType::None);
	}
	std::shared_ptr<QueueT> GetSyncQueue() {
		shared_lock<MtxT> lck;
		return syncQueue_;
	}
	std::shared_ptr<QueueT> GetAsyncQueue() {
		shared_lock<MtxT> lck;
		return asyncQueue_;
	}
	template <typename ContainerT>
	void ReinitSyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT> &&allowList) {
		std::lock_guard<MtxT> lck(mtx_);
		const auto maxDataSize = syncQueue_->MaxDataSize;
		syncQueue_ = std::make_shared<QueueT>(maxDataSize, statsCollector);
		syncQueue_->Init(std::move(allowList));
	}
	template <typename ContainerT>
	void ReinitAsyncQueue(ReplicationStatsCollector statsCollector, std::optional<ContainerT> &&allowList) {
		std::lock_guard<MtxT> lck(mtx_);
		const auto maxDataSize = asyncQueue_->MaxDataSize;
		asyncQueue_ = std::make_shared<QueueT>(maxDataSize, statsCollector);
		asyncQueue_->Init(std::move(allowList));
	}
	template <typename ContextT>
	std::pair<Error, bool> Push(UpdatesContainerT &&data, std::function<void()> beforeWait, const ContextT &ctx) {
		auto shardPair = GetQueue(data[0].GetNsName());
		switch (shardPair.second) {
			case TokenType::Sync: {
				return shardPair.first->PushAndWait(std::move(data), std::move(beforeWait), ctx);
			}
			case TokenType::Async:
				return shardPair.first->template PushAsync<true>(std::move(data));
			default:
				return std::make_pair(Error(), false);
		}
	}
	std::pair<Error, bool> PushNowait(UpdatesContainerT &&data) {
		auto shardPair = GetQueue(data[0].GetNsName());
		switch (shardPair.second) {
			case TokenType::Sync:
				return shardPair.first->template PushAsync<false>(std::move(data));
			case TokenType::Async:
				return shardPair.first->template PushAsync<true>(std::move(data));
			default:
				return std::make_pair(Error(), false);
		}
	}
	std::pair<Error, bool> PushAsync(UpdatesContainerT &&data) {
		std::shared_ptr<QueueT> shard;
		{
			std::string_view token(data[0].GetNsName());
			shared_lock<MtxT> lck(mtx_);
			if (!asyncQueue_->TokenIsInWhiteList(token)) {
				return std::make_pair(Error(), false);
			}
			shard = asyncQueue_;
		}
		return shard->template PushAsync<true>(std::move(data));
	}

private:
	TokenType getTokenType(std::string_view token /*, std::size_t hash*/) const noexcept {
		if (syncQueue_->TokenIsInWhiteList(token)) {
			return TokenType::Sync;
		} else if (asyncQueue_->TokenIsInWhiteList(token)) {
			return TokenType::Async;
		}
		return TokenType::None;
	}

	MtxT mtx_;
	std::shared_ptr<QueueT> syncQueue_;
	std::shared_ptr<QueueT> asyncQueue_;
};

}  // namespace cluster
}  // namespace reindexer
