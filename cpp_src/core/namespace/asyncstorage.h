#pragma once

#include <deque>
#include <mutex>
#include "core/storage/idatastorage.h"
#include "estl/h_vector.h"
#include "estl/mutex.h"
#include "tools/assertrx.h"
#include "tools/clock.h"
#include "tools/flagguard.h"

namespace reindexer {

class StorageFlushOpts {
public:
	StorageFlushOpts& WithImmediateReopen(bool v = true) noexcept {
		opts_ = v ? opts_ | kOptTypeImmediateReopen : opts_ & ~(kOptTypeImmediateReopen);
		return *this;
	}
	bool IsWithImmediateReopen() const noexcept { return opts_ & kOptTypeImmediateReopen; }
	StorageFlushOpts& WithIgnoreFlushError(bool v = true) noexcept {
		opts_ = v ? opts_ | kOptTypeIgnoreFlushError : opts_ & ~(kOptTypeIgnoreFlushError);
		return *this;
	}
	bool IsWithIgnoreFlushError() const noexcept { return opts_ & kOptTypeIgnoreFlushError; }

private:
	enum OptType {
		kOptTypeImmediateReopen = 0x1,
		kOptTypeIgnoreFlushError = 0x2,
	};

	uint8_t opts_;
};

class AsyncStorage {
public:
	static constexpr uint32_t kLimitToAdviceBatching = 1000;
	static constexpr auto kStorageReopenPeriod = std::chrono::seconds(15);

	using AdviceGuardT = CounterGuardAIRL32;
	using ClockT = system_clock_w;
	using TimepointT = ClockT::time_point;
	using Mutex = MarkedMutex<std::mutex, MutexMark::AsyncStorage>;

	struct Status {
		bool isEnabled = false;
		Error err;
	};

	class Cursor {
	public:
		Cursor(std::unique_lock<Mutex>&& lck, std::unique_ptr<datastorage::Cursor>&& c) noexcept : lck_(std::move(lck)), c_(std::move(c)) {
			assertrx(lck_.owns_lock());
			assertrx(c_);
		}
		datastorage::Cursor* operator->() noexcept { return c_.get(); }

	private:
		// NOTE: Cursor owns unique storage lock. I.e. nobody is able to read stroage or write into it, while cursor exists.
		// Currently the only place, where it matter is EnumMeta method. However, we should to consider switching to shared_mutex, if
		// the number of such concurrent Cursors will grow.
		std::unique_lock<Mutex> lck_;
		std::unique_ptr<datastorage::Cursor> c_;
	};

	class FullLockT {
	public:
		using MutexType = Mutex;
		FullLockT(Mutex& flushMtx, Mutex& updatesMtx) : flushLck_(flushMtx), storageLck_(updatesMtx) {}
		bool OwnsThisFlushMutex(Mutex& mtx) const noexcept { return flushLck_.owns_lock() && flushLck_.mutex() == &mtx; }
		bool OwnsThisStorageMutex(Mutex& mtx) const noexcept { return storageLck_.owns_lock() && storageLck_.mutex() == &mtx; }

	private:
		std::unique_lock<Mutex> flushLck_;
		std::unique_lock<Mutex> storageLck_;
	};

	AsyncStorage() = default;
	AsyncStorage(const AsyncStorage& o, AsyncStorage::FullLockT& storageLock);

	Error Open(datastorage::StorageType storageType, const std::string& nsName, const std::string& path, const StorageOpts& opts);
	void Destroy();
	Cursor GetCursor(StorageOpts& opts) const;
	// Tries to write synchronously, hovewer will perform an async write for copied namespace and in case of storage errors
	void WriteSync(const StorageOpts& opts, std::string_view key, std::string_view value);
	// Tries to remove synchronously, hovewer will perform an async deletion for copied namespace and in case of storage errors
	void RemoveSync(const StorageOpts& opts, std::string_view key);
	void Remove(std::string_view key) {
		std::lock_guard lck(storageMtx_);
		remove(false, key);
	}
	void Write(std::string_view key, std::string_view data) {
		std::lock_guard lck(storageMtx_);
		write(false, key, data);
	}
	Error Read(const StorageOpts& opts, std::string_view key, std::string& value) const {
		std::lock_guard lck(storageMtx_);
		if (storage_) {
			return storage_->Read(opts, key, value);
		}
		return Error();
	}
	void Close();
	void Flush(const StorageFlushOpts& opts);
	void TryForceFlush() {
		const auto forceFlushLimit = forceFlushLimit_.load(std::memory_order_relaxed);
		if (forceFlushLimit && totalUpdatesCount_.load(std::memory_order_acquire) >= forceFlushLimit) {
			// Flush must be performed in single thread
			std::lock_guard flushLck(flushMtx_);
			if (totalUpdatesCount_.load(std::memory_order_acquire) >= forceFlushLimit) {
				flush(StorageFlushOpts().WithIgnoreFlushError());
			}
		}
	}
	bool IsValid() const noexcept {
		std::lock_guard lck(storageMtx_);
		return storage_.get();
	}
	Status GetStatusCached() const noexcept { return statusCache_.GetStatus(); }
	FullLockT FullLock() { return FullLockT{flushMtx_, storageMtx_}; }
	std::string GetPathCached() const noexcept { return statusCache_.GetPath(); }
	std::string GetPath() const noexcept;
	datastorage::StorageType GetType() const noexcept;
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
	void flush(const StorageFlushOpts& opts);
	void beginNewUpdatesChunk();
	void write(bool fromSyncCall, std::string_view key, std::string_view value) {
		asyncOp(
			fromSyncCall, [this](std::string_view k, std::string_view v) { curUpdatesChunck_->Put(k, v); }, key, value);
	}
	void remove(bool fromSyncCall, std::string_view key) {
		asyncOp(
			fromSyncCall, [this](std::string_view k) { curUpdatesChunck_->Remove(k); }, key);
	}
	template <typename StorageCall, typename... Args>
	void asyncOp(bool fromSyncCall, StorageCall&& call, const Args&... args) {
		if (storage_) {
			totalUpdatesCount_.fetch_add(1, std::memory_order_release);
			call(args...);
			if (fromSyncCall) {
				lastBatchWithSyncUpdates_ = finishedUpdateChuncks_.size();
			}
			if (++curUpdatesChunck_.updatesCount == kFlushChunckSize) {
				beginNewUpdatesChunk();
			}
		}
	}
	template <typename AsyncCall, typename SyncCall, typename... Args>
	void syncOp(SyncCall&& syncCall, AsyncCall&& asyncCall, const Args&... args) {
		if (!isCopiedNsStorage_ && lastFlushError_.ok()) {
			if (storage_) {
				Error err;
				try {
					err = syncCall(args...);
				} catch (Error& e) {
					err = std::move(e);
				}
				if (!err.ok()) {
					scheduleFilesReopen(std::move(err));
					asyncCall(args...);
				}
			}
			return;
		}
		asyncCall(args...);
	}
	void throwOnStorageCopy() const {
		if (isCopiedNsStorage_) {
			throw Error(errLogic, "Unable to perform this operation with copied storage");
		}
	}

	UpdatesPtrT createUpdatesCollection() noexcept;
	void recycleUpdatesCollection(UpdatesPtrT&& p) noexcept;
	void scheduleFilesReopen(Error&& e) noexcept {
		setLastFlushError(std::move(e));
		reopenTs_ = ClockT::now_coarse() + kStorageReopenPeriod;
	}
	void reset() noexcept {
		storage_.reset();
		path_.clear();
		lastFlushError_ = Error();
		reopenTs_ = TimepointT();
		lastBatchWithSyncUpdates_ = -1;
		updateStatusCache();
	}
	void tryReopenStorage();
	void updateStatusCache() noexcept { statusCache_.Update(Status{bool(storage_.get()), lastFlushError_}, path_); }
	void setLastFlushError(Error&& e) {
		lastFlushError_ = std::move(e);
		updateStatusCache();
	}

	class StatusCache {
	public:
		void Update(Status&& st, std::string path) noexcept {
			std::lock_guard lck(mtx_);
			status_ = std::move(st);
			path_ = std::move(path);
		}
		void UpdatePart(bool isEnabled, std::string path) noexcept {
			std::lock_guard lck(mtx_);
			status_.isEnabled = isEnabled;
			path_ = std::move(path);
		}
		Status GetStatus() const noexcept {
			std::lock_guard lck(mtx_);
			return status_;
		}
		std::string GetPath() const noexcept {
			std::lock_guard lck(mtx_);
			return path_;
		}

	private:
		mutable std::mutex mtx_;
		Status status_;
		std::string path_;
	};

	StatusCache statusCache_;  // Status cache to avoid any long locks
	std::deque<UpdatesPtrT> finishedUpdateChuncks_;
	UpdatesPtrT curUpdatesChunck_;
	std::atomic<uint32_t> totalUpdatesCount_ = {0};
	// path_ value and storage_ pointer may be changed under full lock only, so it's valid to read their values under any of flushMtx_ or
	// storageMtx_ locks
	shared_ptr<datastorage::IDataStorage> storage_;
	std::string path_;
	mutable Mutex storageMtx_;
	mutable Mutex flushMtx_;
	bool isCopiedNsStorage_ = false;
	h_vector<UpdatesPtrT, kMaxRecycledChunks> recycled_;
	std::atomic<int32_t> batchingAdvices_ = {0};
	std::atomic<uint32_t> forceFlushLimit_ = {0};
	int32_t lastBatchWithSyncUpdates_ = -1;	 // This is required to avoid reordering between sync and async records
	Error lastFlushError_;
	TimepointT reopenTs_;
};

}  // namespace reindexer
