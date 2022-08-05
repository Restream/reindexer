#pragma once

#include <condition_variable>
#include "namespaceimpl.h"

namespace reindexer {

class ItemsLoader {
public:
	constexpr static unsigned kBufferSize = 3000;
	constexpr static unsigned kReadSize = kBufferSize / 2;

	struct LoadData {
		int64_t maxLSN = -1;
		int64_t minLSN = std::numeric_limits<int64_t>::max();
		Error lastErr;
		unsigned errCount = 0;
		size_t ldcount = 0;
		std::exception_ptr ex;
	};

	ItemsLoader(unsigned indexInsertionThreads, NamespaceImpl& ns)
		: ns_(ns),
		  items_(kBufferSize, ns_.payloadType_, ns_.tagsMatcher_),
		  slices_(kBufferSize),
		  indexInsertionThreads_(indexInsertionThreads) {
		assertrx(indexInsertionThreads_);
	}
	LoadData Load();

private:
	template <typename T>
	class InplaceRingBuf {
	public:
		template <typename... Args>
		InplaceRingBuf(size_t cap, const Args&... args) noexcept {
			ring_.reserve(cap);
			for (size_t i = 0; i < cap; ++i) {
				ring_.emplace_back(args...);
			}
		}
		T& PlaceItem() {
			const auto newHead = (placedHead_ + 1) % ring_.size();
			if (newHead == tail_) {
				throw Error(errLogic, "Error in items loader ring buffer: unable to place a new item");
			}
			auto& t = ring_[placedHead_];
			placedHead_ = newHead;
			return t;
		}
		void WritePlaced() {
			if (placedHead_ == writtenHead_) {
				throw Error(errLogic, "Error in items loader ring buffer: unable to write placed item");
			}
			writtenHead_ = (writtenHead_ + 1) % ring_.size();
		}
		void ErasePlaced() {
			if (placedHead_ == writtenHead_) {
				throw Error(errLogic, "Error in items loader ring buffer: unable to erase placed item");
			}
			placedHead_ = ((placedHead_ + ring_.size()) - 1) % ring_.size();
		}
		bool IsFull() const noexcept { return ((placedHead_ + 1) % ring_.size()) == tail_; }
		bool HasNoPlacedItems() const noexcept { return placedHead_ == tail_; }
		bool HasNoWrittenItems() const noexcept { return writtenHead_ == tail_; }
		span<T> Tail(size_t maxCount) noexcept {
			const size_t cnt = std::min(maxCount, ((tail_ > writtenHead_) ? ring_.size() : writtenHead_) - tail_);
			return span<T>(ring_.data() + tail_, cnt);
		}
		void Erase(size_t cnt) noexcept {
			const size_t actualCnt = std::min(cnt, WrittenSize());
			tail_ = (tail_ + actualCnt) % ring_.size();
		}
		size_t WrittenSize() const noexcept { return (writtenHead_ - tail_ + ring_.size()) % ring_.size(); }

	protected:
		size_t writtenHead_ = 0, placedHead_ = 0, tail_ = 0;
		std::vector<T> ring_;
	};

	struct SliceStorage {
		unsigned len = 0;
		std::unique_ptr<char[]> data;
	};
	struct ItemData {
		ItemData(const PayloadType& type, const TagsMatcher& tagsMatcher) : impl(type, tagsMatcher) {}

		ItemImpl impl;
		PayloadValue preallocPl;  // Payload, which will be emplaced into namespace
	};

	void reading();
	void insertion();
	void clearIndexCache();
	template <typename MutexT>
	static void doInsertField(NamespaceImpl::IndexesStorage& indexes, unsigned field, IdType id, Payload& pl, Payload& plNew,
							  VariantArray& krefs, VariantArray& skrefs, MutexT& mtx);

	friend class IndexInserters;

	NamespaceImpl& ns_;
	std::mutex mtx_;
	std::condition_variable cv_;
	InplaceRingBuf<ItemData> items_;
	std::vector<SliceStorage> slices_;
	bool terminated_ = false;
	LoadData loadingData_;
	const unsigned indexInsertionThreads_;
};

class IndexInserters {
public:
	IndexInserters(NamespaceImpl::IndexesStorage& indexes, PayloadType pt);
	~IndexInserters() { Stop(); }

	void Run(unsigned threadsCnt);
	void Stop();
	void AwaitIndexesBuild();
	void BuildSimpleIndexesAsync(unsigned startId, span<ItemsLoader::ItemData> newItems, span<PayloadValue> nsItems);
	void BuildCompositeIndexesAsync();

private:
	struct SharedData {
		span<ItemsLoader::ItemData> newItems;
		span<PayloadValue> nsItems;
		unsigned startId = 0;
		h_vector<unsigned, 8> threadsWithNewData;
		bool terminate = false;
		bool composite = false;
	};

	void insertionLoop(unsigned threadId) noexcept;
	void onItemsHandled() noexcept {
		if ((readyThreads_.fetch_add(1, std::memory_order_acq_rel) + 1) == threads_.size()) {
			std::lock_guard lck(mtx_);
			cv_.notify_all();
		}
	}
	void onException(Error e) {
		std::lock_guard lck(mtx_);
		status_ = std::move(e);
		if ((readyThreads_.fetch_add(1, std::memory_order_acq_rel) + 1) == threads_.size()) {
			cv_.notify_all();
		}
	}

	std::mutex mtx_;
	std::condition_variable cv_;
	NamespaceImpl::IndexesStorage& indexes_;
	const PayloadType pt_;
	SharedData shared_;
	std::atomic<unsigned> readyThreads_ = {0};
	std::vector<std::thread> threads_;
	Error status_;
	bool hasArrayIndexes_ = false;
	constexpr static unsigned kTIDOffset = 1;  // Thread ID offset to handle fields [1,n] based on TID
	std::array<shared_timed_mutex, 10> plArrayMtxs_;
};

}  // namespace reindexer
