#pragma once

#include "core/itemimpl.h"
#include "estl/condition_variable.h"
#include "estl/mutex.h"
#include "namespaceimpl.h"

namespace reindexer {

class [[nodiscard]] ItemsLoader {
public:
	constexpr static unsigned kBufferSize = 3000;
	constexpr static unsigned kReadSize = kBufferSize / 2;

	struct [[nodiscard]] LoadData {
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
	class [[nodiscard]] InplaceRingBuf {
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
		std::span<T> Tail(size_t maxCount) noexcept {
			const size_t cnt = std::min(maxCount, ((tail_ > writtenHead_) ? ring_.size() : writtenHead_) - tail_);
			return std::span<T>(ring_.data() + tail_, cnt);
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

	struct [[nodiscard]] SliceStorage {
		unsigned len = 0;
		std::unique_ptr<char[]> data;
	};
	struct [[nodiscard]] ItemData {
		ItemData(const PayloadType& type, const TagsMatcher& tagsMatcher) : impl(type, tagsMatcher) {}

		ItemImpl impl;
		PayloadValue preallocPl;  // Payload, which will be emplaced into namespace
	};
	struct [[nodiscard]] ANNIndexInfo {
		size_t field;
		size_t dims;
	};

	void prepareANNData();
	void reading();
	void insertion();
	void loadCachedANNIndexes();
	void loadCachedANNIndexesFallback(const std::vector<unsigned>& indexes);
	void clearIndexCache();
	template <typename MutexT>
	static void doInsertField(NamespaceImpl::IndexesStorage& indexes, unsigned field, IdType id, Payload& pl, Payload& plNew,
							  VariantArray& krefs, VariantArray& skrefs, MutexT& mtx, const ann_storage_cache::Reader* annCache);

	friend class IndexInserters;

	NamespaceImpl& ns_;
	mutex mtx_;
	condition_variable cv_;
	InplaceRingBuf<ItemData> items_;
	std::vector<SliceStorage> slices_;
	bool terminated_ = false;
	LoadData loadingData_;
	const unsigned indexInsertionThreads_;

	// ANN-helpers
	std::unique_ptr<ann_storage_cache::Reader> annCacheReader_;
	std::vector<ANNIndexInfo> annIndexes_;
	fast_hash_map<size_t, std::vector<std::unique_ptr<uint8_t[]>>> vectorsData_;
};

class [[nodiscard]] IndexInserters {
public:
	IndexInserters(NamespaceImpl::IndexesStorage& indexes, PayloadType pt, const ann_storage_cache::Reader* annCache);
	~IndexInserters() { Stop(); }

	void Run(unsigned threadsCnt);
	void Stop();
	void AwaitIndexesBuild();
	void BuildSimpleIndexesAsync(unsigned startId, std::span<ItemsLoader::ItemData> newItems, std::span<PayloadValue> nsItems);
	void BuildCompositeIndexesAsync();

private:
	struct [[nodiscard]] SharedData {
		std::span<ItemsLoader::ItemData> newItems;
		std::span<PayloadValue> nsItems;
		unsigned startId = 0;
		bool terminate = false;
		bool composite = false;
	};

	void insertionLoop(unsigned threadId) noexcept;
	void onItemsHandled() noexcept {
		lock_guard lck(mtx_);
		if (++readyThreads_ == threads_.size()) {
			cvDone_.notify_one();
		}
	}
	void onException(Error e) {
		lock_guard lck(mtx_);
		status_ = std::move(e);
		if (++readyThreads_ == threads_.size()) {
			cvDone_.notify_one();
		}
	}

	mutex mtx_;
	condition_variable cvReady_;
	condition_variable cvDone_;
	unsigned iteration_{0};
	NamespaceImpl::IndexesStorage& indexes_;
	const PayloadType pt_;
	SharedData shared_;
	unsigned readyThreads_ = {0};
	std::vector<std::thread> threads_;
	Error status_;
	bool hasArrayIndexes_ = false;
	constexpr static unsigned kTIDOffset = 1;  // Thread ID offset to handle fields [1,n] based on TID
	std::array<shared_timed_mutex, 10> plArrayMtxs_;
	const ann_storage_cache::Reader* annCache_ = {nullptr};
};

}  // namespace reindexer
