#pragma once

#include <unordered_map>
#include "core/rdxcontext.h"
#include "estl/contexted_cond_var.h"
#include "estl/fast_hash_set.h"
#include "estl/h_vector.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace cluster {
template <typename T, size_t kShardRecycleNodes = 20, typename MtxT = read_write_spinlock>
class ShardedQueue {
public:
	using HashT = nocase_hash_str;
	using CompareT = nocase_equal_str;

	class Shard {
	public:
		class QueueEntry {
		public:
			QueueEntry(T &&_data, Shard &owner, std::function<void(Error &&)> onResult)
				: data(std::move(_data)), owner_(owner), onResult_(std::move(onResult)) {}

			bool RequireResult() const noexcept { return onResult_ != nullptr; }
			void OnResult(Error &&err) { owner_.onResult(std::move(err), self_, false); }

			T data;

		private:
			typename std::list<QueueEntry>::iterator self_;
			Shard &owner_;
			std::function<void(Error &&)> onResult_;
			friend Shard;
		};
		using ListT = std::list<QueueEntry>;

		void SetDataNotifier(std::function<void()> n) {
			std::unique_lock<std::mutex> lck(mtx_);
			newDataNotifier_ = std::move(n);
		}
		std::pair<typename ListT::iterator, size_t> Read() {
			std::unique_lock<std::mutex> lck(mtx_);
			std::pair<typename ListT::iterator, size_t> range{nextRead_, newDataCount_};
			nextRead_ = queue_.end();
			newDataCount_ = 0;
			return range;
		}
		std::pair<typename ListT::iterator, size_t> ReadAndInvalidate() {
			std::unique_lock<std::mutex> lck(mtx_);
			std::pair<typename ListT::iterator, size_t> range{nextRead_, newDataCount_};
			nextRead_ = queue_.end();
			newDataCount_ = 0;
			invalidated_ = true;
			return range;
		}
		template <typename ContextT>
		std::pair<Error, bool> PushAndWait(h_vector<T, 1> &&data, std::function<void()> beforeWait, const ContextT &) {
			struct {
				size_t executedCnt = 0;
				Error err;
			} localData;
			auto onResult = [this, &localData](Error &&err) {
				if (!err.ok()) {
					localData.err = std::move(err);
				}
				++localData.executedCnt;
				condResultReady_.notify_all();
			};
			h_vector<typename ListT::iterator, 8> entries;

			std::string name(data[0].GetNsName());
			size_t dataSize = data.size();

			std::unique_lock<std::mutex> lck(mtx_);
			if (invalidated_) {
				return std::make_pair(Error(), false);
			}
			try {
				entries = addDataToQueue(std::move(data), std::move(onResult));

				std::cerr << "Push new sync updates (" << dataSize << ") for " << name << "; NextRead for " << nextRead_->data.GetNsName()
						  << std::endl;

				if (beforeWait) {
					beforeWait();  // FIXME: Think about better workaround
				}
				static RdxContext dummyCtx_;
				condResultReady_.wait(
					lck, [&localData, dataSize] { return localData.executedCnt == dataSize || !localData.err.ok(); },
					dummyCtx_);	 // Don't pass cancel context here, because data are already on the leader and we have to handle them
				return std::make_pair(std::move(localData.err), true);
			} catch (...) {
				for (auto &entry : entries) {
					entry->onResult_ = nullptr;
				}
				throw;
			}
		}
		std::pair<Error, bool> PushAsync(h_vector<T, 1> &&data) {
			std::string name(data[0].GetNsName());
			std::unique_lock<std::mutex> lck(mtx_);
			if (invalidated_) {
				return std::make_pair(Error(), false);
			}
			addDataToQueue(std::move(data), nullptr);

			std::cerr << "Push new async updates (" << data.size() << ") for " << name << "; NextRead for " << nextRead_->data.GetNsName()
					  << std::endl;
			return std::make_pair(Error(), true);
		}
		fast_hash_set<string_view, HashT, CompareT> GetTokens() const {
			std::lock_guard<std::mutex> lck(mtx_);
			return tokens_;
		}

	private:
		h_vector<typename ListT::iterator, 8> addDataToQueue(h_vector<T, 1> &&data, std::function<void(Error &&)> onResult) {
			h_vector<typename ListT::iterator, 8> entries;
			for (auto &&d : data) {
				entries.emplace_back();
				auto &entry = entries.back();
				if (recycle_.empty()) {
					queue_.emplace_back(std::move(d), *this, onResult);
					entry = --queue_.end();
				} else {
					entry = recycle_.begin();
					entry->data = std::move(d);
					entry->onResult_ = onResult;
					queue_.splice(queue_.end(), recycle_, recycle_.begin());
				}
				entry->self_ = entry;
				if (++newDataCount_ == 1) {
					nextRead_ = queue_.end();
					--nextRead_;
				}
			}

			if (newDataCount_ == data.size()) {
				if (newDataNotifier_) {
					newDataNotifier_();
				}
			}
			return entries;
		}
		void addToken(string_view token) {
			std::lock_guard<std::mutex> lck(mtx_);
			tokens_.emplace(token);
		}
		void onResult(Error err, typename ListT::iterator entry, bool nolock) {
			std::unique_lock<std::mutex> lck(mtx_, std::defer_lock);
			if (!nolock) {
				lck.lock();
			}
			if (entry->onResult_) {
				entry->onResult_(std::move(err));
				entry->onResult_ = nullptr;
			}
			if (recycle_.size() < kShardRecycleNodes) {
				recycle_.splice(recycle_.end(), queue_, entry);
			} else {
				queue_.erase(entry);
			}
		}

		mutable std::mutex mtx_;
		contexted_cond_var condResultReady_;
		std::function<void()> newDataNotifier_;
		std::list<QueueEntry> queue_;
		typename ListT::iterator nextRead_ = queue_.begin();
		size_t newDataCount_ = 0;
		std::list<QueueEntry> recycle_;
		fast_hash_set<string_view, HashT, CompareT> tokens_;
		bool invalidated_ = false;
		friend ShardedQueue;
	};
	enum class TokenType { Sync, Async, None };

	ShardedQueue(size_t shards) {
		assert(shards > 0);
		storage_.resize(shards);
	}

	std::pair<std::shared_ptr<Shard>, TokenType> GetShard(const std::string &token) {
		// HashT h;
		// std::size_t hash = h(token);
		{
			shared_lock<MtxT> lck(mtx_);
			auto type = getTokenType(token);  //, hash);
			if (type == TokenType::None) {
				return std::make_pair(std::shared_ptr<Shard>(), type);
			}
			{
				auto found = shards_.find(token);  //, hash);
				if (found != shards_.end()) {
					return std::make_pair(*(found->second), type);
				}
			}
		}

		std::lock_guard<MtxT> lck(mtx_);
		auto type = getTokenType(token);  //, hash);
		if (type == TokenType::None) {
			return std::make_pair(std::shared_ptr<Shard>(), type);
		}
		return std::make_pair(doEmplaceToken(token), type);
	}
	std::shared_ptr<Shard> GetShard(size_t id) {
		shared_lock<MtxT> lck;
		if (id < storage_.size()) {
			return storage_[id];
		}
		return std::shared_ptr<Shard>();
	}
	template <typename ContainerT1, typename ContainerT2>
	void RebuildShards(size_t shards, ContainerT1 &&tokens, std::unique_ptr<ContainerT2> &&syncWhiteList,
					   std::unique_ptr<ContainerT2> &&asyncWhiteList) {
		assert(shards > 0);
		std::lock_guard<MtxT> lck(mtx_);
		storage_.clear();
		storage_.reserve(shards);
		for (size_t i = 0; i < shards; ++i) {
			storage_.emplace_back(std::make_shared<Shard>());
		}
		shards_.clear();
		num_ = 0;
		for (auto &&token : tokens) {
			doEmplaceToken(token);
		}
		syncWhiteList_.reset();
		if (syncWhiteList) {
			syncWhiteList_.reset(new TokensHashSetT);
			for (auto &&token : *syncWhiteList) {
				syncWhiteList_->emplace(std::move(token));
			}
			syncWhiteList.reset();
		}
		asyncWhiteList_.reset();
		if (asyncWhiteList) {
			asyncWhiteList_.reset(new TokensHashSetT);
			for (auto &&token : *asyncWhiteList) {
				asyncWhiteList_->emplace(std::move(token));
			}
			asyncWhiteList.reset();
		}
	}
	template <typename ContextT>
	std::pair<Error, bool> Push(h_vector<T, 1> &&data, std::function<void()> beforeWait, const ContextT &ctx) {
		std::pair<Error, bool> res;
		do {
			auto shardPair = GetShard(data[0].GetNsName());
			switch (shardPair.second) {
				case TokenType::Sync:
					res = shardPair.first->PushAndWait(std::move(data), std::move(beforeWait), ctx);
					break;
				case TokenType::Async:
					res = shardPair.first->PushAsync(std::move(data));
					break;
				case TokenType::None:
					return std::make_pair(Error(), false);
			}
		} while (!res.second);
		return res;
	}
	std::pair<Error, bool> PushAsync(h_vector<T, 1> &&data) {
		std::pair<Error, bool> res;
		do {
			auto shardPair = GetShard(data[0].GetNsName());
			if (shardPair.second != TokenType::Async) {
				return std::make_pair(Error(), false);
			}
			res = shardPair.first->PushAsync(std::move(data));
		} while (!res.second);
		return res;
	}

private:
	using TokensHashSetT = fast_hash_set<std::string, HashT, CompareT>;

	template <typename TokenT>
	std::shared_ptr<Shard> doEmplaceToken(TokenT &&token) {
		// auto res = shards_.try_emplace(std::string(std::forward<TokenT>(token)), nullptr);
		auto res = shards_.emplace(std::string(std::forward<TokenT>(token)), nullptr);
		std::shared_ptr<Shard> *shard = res.first->second;
		if (res.second) {
			size_t id = num_++ % storage_.size();
			shard = &storage_[id];
			res.first->second = &storage_[id];
			(*shard)->addToken(string_view(res.first->first));
		}
		return *shard;
	}

	TokenType getTokenType(string_view token /*, std::size_t hash*/) const noexcept {
		if (syncWhiteList_) {
			// auto found = syncWhiteList_->find(token, hash);
			auto found = syncWhiteList_->find(token);
			if (found != syncWhiteList_->end() || syncWhiteList_->empty()) {
				return TokenType::Sync;
			}
		}
		if (asyncWhiteList_) {
			// auto found = asyncWhiteList_->find(token, hash);
			auto found = asyncWhiteList_->find(token);
			if (found != asyncWhiteList_->end() || asyncWhiteList_->empty()) {
				return TokenType::Async;
			}
		}
		return TokenType::None;
	}

	MtxT mtx_;
	std::vector<std::shared_ptr<Shard>> storage_;
	size_t num_ = 0;
	std::unordered_map<std::string, std::shared_ptr<Shard> *, HashT, CompareT>
		shards_;									 // Container should not invalidate refernces to keys
	std::unique_ptr<TokensHashSetT> syncWhiteList_;	 // TODO: Switch to optional
	std::unique_ptr<TokensHashSetT> asyncWhiteList_;
};
}  // namespace cluster
}  // namespace reindexer
