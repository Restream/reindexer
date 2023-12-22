#pragma once

#include <deque>
#include <optional>
#include <unordered_map>
#include "cluster/logger.h"
#include "cluster/stats/relicationstatscollector.h"
#include "core/rdxcontext.h"
#include "estl/contexted_cond_var.h"
#include "estl/fast_hash_set.h"
#include "estl/h_vector.h"
#include "estl/mutex.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace cluster {

enum class ReplicationResult { None, Approved, Error };

template <typename T>
class UpdatesQueue {
public:
	using HashT = nocase_hash_str;
	using CompareT = nocase_equal_str;
	using LessT = nocase_less_str;
	using UpdatesContainerT = h_vector<T, 2>;
	static constexpr auto kBatchSize = 500;
	template <typename U, uint16_t kBatch>
	class QueueEntry {
	public:
		struct DroppedUpdatesT {};
		struct UpdatesStatus {
			bool requireResult = false;
			bool requireErasure = false;
			bool hasEnoughApproves = false;
			Error result;
		};

		struct Value {
			struct Counters {
				uint32_t replicatedToEmmiter : 1;
				uint32_t requireResult : 1;
				uint32_t replicas : 10;
				uint32_t approves : 10;
				uint32_t errors : 10;
			};
			static_assert(std::atomic<Counters>().is_always_lock_free, "Expecting this struct to be lock-free");

		public:
			Counters GetCounters() const noexcept { return replication_.load(std::memory_order_acquire); }
			const U &Data() const noexcept { return data_; }

		private:
			UpdatesStatus replicated(uint32_t consensusCnt, uint32_t requiredReplicas, bool isEmmiter, Error &&err) {
				auto expected = replication_.load(std::memory_order_acquire);
				Counters repl;
				UpdatesStatus status;
				bool error = false;
				do {
					status = UpdatesStatus();
					repl = expected;
					repl.replicatedToEmmiter = repl.replicatedToEmmiter || isEmmiter;
					if (err.ok()) {
						++repl.approves;
						if (repl.requireResult) {
							status.requireResult = (repl.approves == consensusCnt && repl.replicatedToEmmiter) ||
												   (isEmmiter && repl.approves >= consensusCnt) ||
												   (isEmmiter && repl.errors >= consensusCnt);
						}
					} else {
						++repl.errors;
						error = true;
						if (repl.requireResult) {
							status.requireResult = (repl.errors == consensusCnt && repl.replicatedToEmmiter) ||
												   (isEmmiter && repl.errors >= consensusCnt) ||
												   (isEmmiter && repl.approves >= consensusCnt);
						}
					}
					if (++repl.replicas == requiredReplicas) {
						status.requireErasure = true;
						if (!repl.replicatedToEmmiter) {
							std::this_thread::sleep_for(std::chrono::seconds(2));
						}
						assertf(repl.replicatedToEmmiter, "Required replicas: %d", requiredReplicas);
					}
				} while (!replication_.compare_exchange_strong(expected, repl, std::memory_order_acquire));
				status.hasEnoughApproves = (repl.approves >= consensusCnt);
				status.result = error ? std::move(err) : Error();
				return status;
			}

			U data_;
			// std::chrono::high_resolution_clock::time_point deadline;  // TODO: Implement deadline logic
			std::atomic<Counters> replication_ = Counters{0, 0, 0, 1, 0};
			std::function<void(Error &&)> *onResult_ = nullptr;

			friend UpdatesQueue;
		};

		QueueEntry() = default;
		QueueEntry(uint64_t id, UpdatesQueue &owner, ReplicationStatsCollector stats) : id_(id), owner_(owner), stats_(stats) {}
		QueueEntry(uint64_t id, UpdatesQueue &owner, DroppedUpdatesT) : IsUpdatesDropBlock(true), id_(id), owner_(owner) {}
		QueueEntry(QueueEntry &&) = default;

		ReplicationResult OnUpdateReplicated(uint32_t nodeId, uint32_t consensusCnt, uint32_t requiredReplicas, uint16_t offset,
											 bool isEmmiter, Error err) {
			ReplicationResult res = ReplicationResult::None;
			if (offset >= count_.load(std::memory_order_acquire)) throw Error(errParams, "Unexpected offset: %d", offset);
			auto status = data_[offset].replicated(consensusCnt, requiredReplicas, isEmmiter, std::move(err));
			stats_.OnUpdateApplied(nodeId, id_ + offset);
			if (status.requireResult) {
				owner_.onResult(id_ + offset, std::move(status.result));
			}
			if (status.requireErasure) {
				erased_.fetch_add(1, std::memory_order_release);
				stats_.OnUpdateReplicated(id_ + offset);
				if (IsFullyErased()) {
					owner_.eraseReplicated();
				}
			}
			if (status.requireResult) {
				res = status.hasEnoughApproves ? ReplicationResult::Approved : ReplicationResult::Error;
			}
			return res;
		}
		const Value &GetUpdate(uint16_t offset) const {
			if (offset >= count_.load(std::memory_order_acquire)) throw Error(errParams, "Unexpected offset: %d", offset);
			return data_[offset];
		}
		uint64_t ID() const noexcept { return id_; }
		uint16_t Count() const noexcept { return count_.load(std::memory_order_acquire); }
		bool HasID(uint64_t id) const noexcept { return id >= ID() && id < ID() + Count(); }
		uint64_t TotalSizeBytes() const noexcept { return totalSizeBytes_; }
		bool IsFullyErased() const noexcept { return erased_.load(std::memory_order_acquire) == kBatch; }
		bool IsInvalidated() const noexcept { return isInvalidated_.load(std::memory_order_relaxed); }

		const bool IsUpdatesDropBlock = false;

	private:
		// Private methods should be invoked under queue's unique lock
		Value &value(uint16_t offset) noexcept {
			assert(offset < count_.load(std::memory_order_relaxed));
			return data_[offset];
		}
		bool hasSpace() const noexcept { return count_.load(std::memory_order_relaxed) < kBatch; }
		template <bool skipResultCounting>
		uint64_t append(U &&val, size_t sizeBytes, std::function<void(Error &&)> *onRes) noexcept {
			assert(hasSpace());
			auto count = count_.load(std::memory_order_relaxed);
			totalSizeBytes_ += sizeBytes;
			data_[count].data_ = std::move(val);
			if constexpr (skipResultCounting) {
				data_[count].replication_.store({val.HasEmmiterID() ? 0u : 1u, 0u, 0, 1, 0}, std::memory_order_relaxed);
				assert(!onRes);
				(void)onRes;
				data_[count].onResult_ = nullptr;
				count_.fetch_add(1, std::memory_order_release);
				addSentResult();
			} else {
				data_[count].replication_.store({val.HasEmmiterID() ? 0u : 1u, 1u, 0, 1, 0}, std::memory_order_relaxed);
				data_[count].onResult_ = onRes;
				count_.fetch_add(1, std::memory_order_release);
			}
			return count + ID();
		}
		void addSentResult() noexcept {
			assert(resultsSent_ <= count_.load(std::memory_order_relaxed));
			++resultsSent_;
		}
		bool isAllResultsSent() const noexcept { return resultsSent_ == count_.load(std::memory_order_relaxed); }
		void markInvalidated() noexcept { isInvalidated_.store(true, std::memory_order_relaxed); }

		Value data_[kBatch];
		uint64_t totalSizeBytes_ = sizeof(QueueEntry);
		uint64_t id_ = 0;
		UpdatesQueue<U> &owner_;
		std::atomic<uint16_t> count_ = {0};
		std::atomic<uint16_t> erased_ = {0};
		uint16_t resultsSent_ = 0;
		std::atomic<bool> isInvalidated_ = {false};
		ReplicationStatsCollector stats_;

		friend UpdatesQueue<U>;
	};

	using UpdateT = QueueEntry<T, kBatchSize>;
	using UpdatePtr = intrusive_ptr<intrusive_atomic_rc_wrapper<UpdateT>>;
	using TokensHashSetT = fast_hash_set<std::string, HashT, CompareT, LessT>;

	static constexpr uint64_t kMinUpdatesBound = 1024 * 1024;

	UpdatesQueue(uint64_t maxDataSize, ReplicationStatsCollector statsCollector = ReplicationStatsCollector()) noexcept
		: MaxDataSize((maxDataSize && (maxDataSize < kMinUpdatesBound)) ? kMinUpdatesBound : maxDataSize), stats_(statsCollector) {}

	void AddDataNotifier(std::thread::id id, std::function<void()> n) {
		std::unique_lock<std::mutex> lck(mtx_);
		newDataNotifiers_[id].n = std::move(n);
	}
	void RemoveDataNotifier(std::thread::id id) {
		std::unique_lock<std::mutex> lck(mtx_);
		newDataNotifiers_.erase(id);
	}
	UpdatePtr Read(uint64_t id, std::thread::id notifier) {
		std::unique_lock<std::mutex> lck(mtx_);
		if (updatedDropRecord_ && id <= updatedDropRecord_->ID()) {
			return updatedDropRecord_;
		}
		if (queue_.empty()) {
			setAwaitDataFlag(notifier);
			return UpdatePtr();
		}
		auto firstId = queue_.front()->ID();
		if (id < firstId) {
			return queue_.front();
		}
		auto idx = (id - firstId) / kBatchSize;
		if (idx >= queue_.size() || !queue_[idx]->HasID(id)) {
			setAwaitDataFlag(notifier);
			return UpdatePtr();
		}
		return queue_[idx];
	}
	void SetWritable(bool isWritable, Error &&err) {
		std::unique_lock<std::mutex> lck(mtx_);
		if (!isWritable) {
			invalidationErr_ = std::move(err);
			int64_t lastUpdateId = -1;
			if (queue_.size()) {
				lastUpdateId += queue_.back()->ID() + queue_.back()->Count();
			}
			for (auto &chunk : queue_) {
				for (size_t i = 0; i < chunk->Count(); ++i) {
					auto e = invalidationErr_;
					onResult(chunk->value(i), std::move(e));
					chunk->markInvalidated();
				}
			}
			if (lastUpdateId >= 0) {
				stats_.OnUpdatesDrop(lastUpdateId, dataSize_);
			}
			dataSize_ = 0;
			queue_.clear();
			invalidated_ = true;
		} else {
			invalidated_ = false;
			invalidationErr_ = Error();
		}
	}
	template <typename ContextT>
	std::pair<Error, bool> PushAndWait(UpdatesContainerT &&data, std::function<void()> beforeWait, const ContextT &) {
		struct {
			size_t dataSize = 0;
			size_t executedCnt = 0;
			Error err;
		} localData;
		std::function<void(Error &&)> onResult = [this, &localData](Error &&err) {
			if (!err.ok()) {
				localData.err = std::move(err);
			}
			if (++localData.executedCnt == localData.dataSize) {
				condResultReady_.notify_all();
			}
		};
		std::pair<uint64_t, uint64_t> entriesRange{0, 0};
		localData.dataSize = data.size();
		std::deque<UpdatePtr> dropped;

		std::unique_lock lck(mtx_);
		if (invalidated_) {
			return std::make_pair(invalidationErr_, false);
		}
		try {
			logTraceW([&] { rtfmt("Push new sync updates (%d) for %s", localData.dataSize, data[0].GetNsName()); });

			entriesRange = addDataToQueue(std::move(data), &onResult, dropped);

			if (beforeWait) {
				beforeWait();  // FIXME: Think about better workaround
			}
			if (dropped.size()) {
				lck.unlock();
				dropped.clear();  // Deallocate dropped outside of the lock scope and then await results;
				lck.lock();
			}
			static RdxContext dummyCtx_;
			condResultReady_.wait(
				lck, [&localData] { return localData.executedCnt == localData.dataSize; },
				dummyCtx_);	 // Don't pass cancel context here, because data are already on the leader and we have to handle them
			return std::make_pair(std::move(localData.err), true);
		} catch (...) {
			logInfoW([] { return "PushAndWait call has recieved an exception"; });
			for (auto i = entriesRange.first; i <= entriesRange.second; ++i) {
				const auto idx = tryGetIdx(i);
				if (idx >= 0) {
					const auto offset = i - queue_[idx]->ID();
					queue_[idx]->value(offset).onResult_ = nullptr;
				}
			}
			throw;
		}
	}
	template <bool skipResultCounting>
	std::pair<Error, bool> PushAsync(UpdatesContainerT &&data) {
		std::deque<UpdatePtr> dropped;
		{
			std::lock_guard lck(mtx_);
			if (invalidated_) {
				return std::make_pair(invalidationErr_, false);
			}

			logTraceW([&] { rtfmt("Push new async updates (%d) for %s", data.size(), data[0].GetNsName()); });

			addDataToQueue<skipResultCounting>(std::move(data), dropped);
		}
		// Deallocate dropped outside of the lock scope

		return std::make_pair(Error(), true);
	}
	bool TokenIsInWhiteList(std::string_view token, std::size_t hash) const noexcept {
		if (allowList_.has_value()) {
			const auto found = allowList_->find(token, hash);
			if (found != allowList_->end() || allowList_->empty()) {
				return true;
			}
		}
		return false;
	}
	template <typename ContainerT>
	void Init(std::optional<ContainerT> &&allowList, const Logger &l) {
		log_ = &l;
		allowList_.reset();
		if (allowList.has_value()) {
			allowList_.emplace();
			for (auto &&token : *allowList) {
				allowList_->emplace(std::move(token));
			}
			allowList.reset();
		}
	}

	const uint64_t MaxDataSize = 0;

private:
	struct DataNotifier {
		std::function<void()> n;
		bool awaitsData = false;
	};

	constexpr static std::string_view logModuleName() noexcept { return std::string_view("queue"); }
	std::pair<uint64_t, uint64_t> addDataToQueue(UpdatesContainerT &&data, std::function<void(Error &&)> *onResult,
												 std::deque<UpdatePtr> &dropped) {
		assert(onResult);
		std::pair<uint64_t, uint64_t> res;
		for (size_t i = 0; i < data.size(); ++i) {
			res.second = addDataImpl<false>(std::move(data[i]), onResult);
			if (i == 0) {
				res.first = res.second;
			}
		}
		dropOverflowingUpdates(dropped);
		notifyAll();
		return res;
	}
	template <bool skipResultCounting>
	void addDataToQueue(UpdatesContainerT &&data, std::deque<UpdatePtr> &dropped) {
		for (auto &&d : data) {
			addDataImpl<skipResultCounting>(std::move(d), nullptr);
		}
		dropOverflowingUpdates(dropped);
		notifyAll();
	}
	void notifyAll() {
		for (auto &notifier : newDataNotifiers_) {
			if (notifier.second.awaitsData) {
				notifier.second.awaitsData = false;
				notifier.second.n();
			}
		}
	}
	void setAwaitDataFlag(std::thread::id id) {
		auto found = newDataNotifiers_.find(id);
		assert(found != newDataNotifiers_.end());
		if (found != newDataNotifiers_.end()) {
			found->second.awaitsData = true;
		}
	}
	template <bool skipResultCounting>
	uint64_t addDataImpl(T &&d, std::function<void(Error &&)> *onResult) {
		uint64_t storageSize = 0;
		if (!queue_.size() || !queue_.back()->hasSpace()) {
			queue_.emplace_back(make_intrusive<intrusive_atomic_rc_wrapper<UpdateT>>(nextChunkID_, *this, stats_));
			nextChunkID_ += kBatchSize;
			storageSize = queue_.back()->TotalSizeBytes();
			dataSize_ += storageSize;
		}
		const auto size = d.DataSize();
		const auto id = queue_.back()->template append<skipResultCounting>(std::move(d), size, onResult);
		dataSize_ += size;
		stats_.OnUpdatePushed(id, size + storageSize);
		return id;
	}
	void onResult(uint64_t id, Error &&err) {
		std::unique_lock<std::mutex> lck(mtx_);
		const auto idx = tryGetIdx(id);
		if (idx < 0) {
			return;
		}
		auto updPtr = queue_[idx];
		const auto offset = id - updPtr->ID();
		auto &entry = updPtr->value(offset);
		updPtr->addSentResult();
		logTraceW([&] {
			if (entry.onResult_) {
				rtfmt("Sending result for update with ID %d", id);
			} else {
				rtfmt("Trying to send result for update with ID %d, but it doesn't have result handler", id);
			}
		});
		onResult(entry, std::move(err));
		eraseReplicated(lck);
	}
	void onResult(typename QueueEntry<T, kBatchSize>::Value &v, Error &&err) {
		if (v.onResult_) {
			(*v.onResult_)(std::move(err));
			v.onResult_ = nullptr;
		}
	}
	void eraseReplicated() {
		std::unique_lock lck(mtx_);
		eraseReplicated(lck);
	}
	void eraseReplicated(std::unique_lock<std::mutex> &lck) {
		while (queue_.size() && queue_.front()->isAllResultsSent() && queue_.front()->IsFullyErased()) {
			auto updPtr = queue_.front();
			queue_.pop_front();
			const auto updTotalSizeBytes = updPtr->TotalSizeBytes();
			dataSize_ -= updTotalSizeBytes;
			lck.unlock();
			stats_.OnUpdateErased(updPtr->ID() + updPtr->Count() - 1, updTotalSizeBytes);
			updPtr.reset();
			lck.lock();
		}
	}
	int64_t tryGetIdx(uint64_t id) const noexcept {
		if (!queue_.size()) {
			return -1;
		}
		const auto idx = (id - queue_.front()->ID()) / kBatchSize;
		if (idx >= queue_.size()) {
			return -1;
		}
		return idx;
	}
	void dropOverflowingUpdates(std::deque<UpdatePtr> &dropped) {
		if (MaxDataSize && dataSize_ > MaxDataSize && queue_.size() > 1 && queue_.front()->isAllResultsSent()) {
			int64_t lastChunckId = -1;
			uint64_t droppedUpdatesSize = 0;
			std::swap(dropped, queue_);
			uint64_t firstUnskipableIdx = 0;
			for (; firstUnskipableIdx < dropped.size(); ++firstUnskipableIdx) {
				auto &upd = dropped[firstUnskipableIdx];
				if (!upd->isAllResultsSent() || upd->hasSpace()) {
					break;
				}
				upd->markInvalidated();
				lastChunckId = upd->ID();
				droppedUpdatesSize += upd->TotalSizeBytes();
			}
			while (dropped.size() > firstUnskipableIdx) {
				queue_.emplace_front(std::move(dropped.back()));
				dropped.pop_back();
			}

			if (lastChunckId >= 0) {
				const auto updateId = lastChunckId + kBatchSize - 1;
				logWarnW([&] { rtfmt("Dropping updates: %d-%d. %d bytes", dropped.front()->ID(), updateId, droppedUpdatesSize); });
				updatedDropRecord_ =
					make_intrusive<intrusive_atomic_rc_wrapper<UpdateT>>(updateId, *this, typename UpdateT::DroppedUpdatesT{});
				stats_.OnUpdatesDrop(updateId, droppedUpdatesSize);
				dataSize_ -= droppedUpdatesSize;
			}
		}
	}
	template <typename F>
	void logWarnW(F &&f) const {
		if (log_) {
			log_->Warn(std::forward<F>(f));
		}
	}
	template <typename F>
	void logInfoW(F &&f) const {
		if (log_) {
			log_->Info(std::forward<F>(f));
		}
	}
	template <typename F>
	void logTraceW(F &&f) const {
		if (log_) {
			log_->Trace(std::forward<F>(f));
		}
	}

	mutable std::mutex mtx_;
	contexted_cond_var condResultReady_;
	std::unordered_map<std::thread::id, DataNotifier> newDataNotifiers_;
	bool invalidated_ = false;
	Error invalidationErr_;
	UpdatePtr updatedDropRecord_;
	std::deque<UpdatePtr> queue_;
	std::optional<TokensHashSetT> allowList_;
	uint64_t nextChunkID_ = 0;
	ReplicationStatsCollector stats_;
	uint64_t dataSize_ = 0;
	const Logger *log_ = nullptr;
};

}  // namespace cluster
}  // namespace reindexer
