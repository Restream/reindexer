#pragma once

#include <deque>
#include <mutex>
#include "core/storage/idatastorage.h"
#include "estl/h_vector.h"
#include "tools/flagguard.h"

namespace reindexer {

class AsyncStorage {
public:
	static constexpr uint32_t kLimitToAdviceBatching = 1000;

	using AdviceGuardT = CounterGuardAIRL32;
	class FullLockT {
	public:
		FullLockT(std::mutex& flushMtx, std::mutex& updatesMtx) : flushLck_(flushMtx), updatesLck_(updatesMtx) {}
		~FullLockT() {
			// Specify unlock order
			updatesLck_.unlock();
			flushLck_.unlock();
		}
		bool OwnsThisFlushMutex(std::mutex& mtx) const noexcept { return flushLck_.owns_lock() && flushLck_.mutex() == &mtx; }
		bool OwnsThisUpdatesMutex(std::mutex& mtx) const noexcept { return updatesLck_.owns_lock() && updatesLck_.mutex() == &mtx; }

	private:
		std::unique_lock<std::mutex> flushLck_;
		std::unique_lock<std::mutex> updatesLck_;
	};

	AsyncStorage() = default;
	AsyncStorage(const AsyncStorage& o, AsyncStorage::FullLockT& storageLock);

	Error Open(datastorage::StorageType storageType, const std::string& nsName, const std::string& path, const StorageOpts& opts);
	void Destroy();
	std::unique_ptr<datastorage::Cursor> GetCursor(StorageOpts& opts) const;
	void WriteSync(const StorageOpts& opts, std::string_view key, std::string_view value);
	std::weak_ptr<datastorage::IDataStorage> GetStoragePtr() const;
	void Remove(std::string_view key) {
		std::lock_guard lck(updatesMtx_);
		if (storage_) {
			totalUpdatesCount_.fetch_add(1, std::memory_order_release);
			curUpdatesChunck_->Remove(key);
			if (++curUpdatesChunck_.updatesCount == kFlushChunckSize) {
				beginNewUpdatesChunk();
			}
		}
	}
	void Write(std::string_view key, std::string_view data) {
		std::lock_guard lck(updatesMtx_);
		write(key, data);
	}
	Error Read(const StorageOpts& opts, std::string_view key, std::string& value) const {
		std::lock_guard lck(updatesMtx_);
		if (storage_) {
			return storage_->Read(opts, key, value);
		}
		return Error();
	}
	void Close();
	void Flush();
	void TryForceFlush() {
		const auto forceFlushLimit = forceFlushLimit_.load(std::memory_order_relaxed);
		if (forceFlushLimit && totalUpdatesCount_.load(std::memory_order_acquire) >= forceFlushLimit) {
			// Flush must be performed in single thread
			std::lock_guard flushLck(flushMtx_);
			if (totalUpdatesCount_.load(std::memory_order_acquire) >= forceFlushLimit) {
				flush();
			}
		}
	}
	bool IsValid() const {
		std::lock_guard lck(updatesMtx_);
		return storage_.get();
	}
	FullLockT FullLock() { return FullLockT{flushMtx_, updatesMtx_}; }
	std::string Path() const noexcept;
	datastorage::StorageType Type() const noexcept;
	void InheritUpdatesFrom(AsyncStorage& src, AsyncStorage::FullLockT& storageLock);
	AdviceGuardT AdviceBatching() noexcept { return AdviceGuardT(batchingAdvices_); }
	void SetForceFlushLimit(uint32_t limit) noexcept { forceFlushLimit_.store(limit, std::memory_order_relaxed); }

private:
	constexpr static uint32_t kFlushChunckSize = 11000;
	constexpr static uint32_t kMaxRecycledChunks = 3;
	class UpdatesPtrT : public datastorage::UpdatesCollection::Ptr {
	public:
		using BaseT = datastorage::UpdatesCollection::Ptr;

		UpdatesPtrT() = default;
		UpdatesPtrT(UpdatesPtrT&& p, uint32_t cnt) : BaseT(std::move(p)), updatesCount(cnt) {}
		UpdatesPtrT(const UpdatesPtrT&) = delete;
		UpdatesPtrT(UpdatesPtrT&& o) noexcept : BaseT(std::move(o)), updatesCount(o.updatesCount) { o.updatesCount = 0; }
		UpdatesPtrT& operator=(const UpdatesPtrT&) = delete;
		UpdatesPtrT& operator=(UpdatesPtrT&& o) {
			if (this != &o) {
				BaseT::operator=(std::move(o));
				updatesCount = o.updatesCount;
				o.updatesCount = 0;
			}
			return *this;
		}

		template <typename P>
		void reset(P* p) noexcept {
			BaseT::reset(p);
			updatesCount = 0;
		}
		void reset() noexcept {
			BaseT::reset();
			updatesCount = 0;
		}

		uint32_t updatesCount = 0;
	};

	void clearUpdates();
	void flush();
	void beginNewUpdatesChunk();
	void write(std::string_view key, std::string_view data) {
		if (storage_) {
			totalUpdatesCount_.fetch_add(1, std::memory_order_release);
			curUpdatesChunck_->Put(key, data);
			if (++curUpdatesChunck_.updatesCount == kFlushChunckSize) {
				beginNewUpdatesChunk();
			}
		}
	}

	UpdatesPtrT createUpdatesCollection() noexcept;
	void recycleUpdatesCollection(UpdatesPtrT&& p) noexcept;

	std::deque<UpdatesPtrT> finishedUpdateChuncks_;
	UpdatesPtrT curUpdatesChunck_;
	std::atomic<uint32_t> totalUpdatesCount_ = {0};
	shared_ptr<datastorage::IDataStorage> storage_;
	mutable std::mutex updatesMtx_;
	mutable std::mutex flushMtx_;
	std::string path_;
	bool isCopiedNsStorage_ = false;
	h_vector<UpdatesPtrT, kMaxRecycledChunks> recycled_;
	std::atomic<int32_t> batchingAdvices_ = {0};
	std::atomic<uint32_t> forceFlushLimit_ = {0};
};

}  // namespace reindexer
