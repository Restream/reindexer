#include "asyncstorage.h"
#include "core/storage/storagefactory.h"
#include "tools/logger.h"

namespace reindexer {

void AsyncStorage::Close() {
	std::lock_guard flushLck(flushMtx_);
	flush(StorageFlushOpts().WithImmediateReopen());

	std::lock_guard lck(storageMtx_);
	clearUpdates();
	reset();
}

AsyncStorage::AsyncStorage(const AsyncStorage& o, AsyncStorage::FullLockT& storageLock) : isCopiedNsStorage_{true} {
	if (!storageLock.OwnsThisFlushMutex(o.flushMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (flush mutex)");
	}
	if (!storageLock.OwnsThisStorageMutex(o.storageMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (updates mutex)");
	}
	storage_ = o.storage_;
	path_ = o.path_;
	curUpdatesChunck_ = createUpdatesCollection();
	updateStatusCache();
	// Do not copying lastFlushError_ and reopenTs_, because copied storage does not performs actual writes
}

Error AsyncStorage::Open(datastorage::StorageType storageType, const std::string& nsName, const std::string& path,
						 const StorageOpts& opts) {
	auto lck = FullLock();

	throwOnStorageCopy();

	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", nsName, path_);
	}
	storage_.reset(datastorage::StorageFactory::create(storageType));
	auto err = storage_->Open(path, opts);
	if (err.ok()) {
		path_ = path;
		curUpdatesChunck_ = createUpdatesCollection();
	}
	updateStatusCache();
	return err;
}

void AsyncStorage::Destroy() {
	auto lck = FullLock();

	throwOnStorageCopy();

	if (storage_) {
		tryReopenStorage();
		clearUpdates();
		storage_->Destroy(path_);
		reset();
	}
}

AsyncStorage::Cursor AsyncStorage::GetCursor(StorageOpts& opts) const {
	std::unique_lock lck(storageMtx_);
	throwOnStorageCopy();
	return Cursor(std::move(lck), std::unique_ptr<datastorage::Cursor>(storage_->GetCursor(opts)));
}

void AsyncStorage::WriteSync(const StorageOpts& opts, std::string_view key, std::string_view value) {
	std::lock_guard lck(storageMtx_);
	syncOp(
		[this, &opts](std::string_view k, std::string_view v) {
			throwOnStorageCopy();
			return storage_->Write(opts, k, v);
		},
		[this](std::string_view k, std::string_view v) { write(true, k, v); }, key, value);
}

void AsyncStorage::RemoveSync(const StorageOpts& opts, std::string_view key) {
	std::lock_guard lck(storageMtx_);
	syncOp(
		[this, &opts](std::string_view k) {
			throwOnStorageCopy();
			return storage_->Delete(opts, k);
		},
		[this](std::string_view k) { remove(true, k); }, key);
}

void AsyncStorage::Flush(const StorageFlushOpts& opts) {
	// Flush must be performed in single thread
	std::lock_guard flushLck(flushMtx_);
	statusCache_.UpdatePart(bool(storage_.get()), path_);  // Actualize cache part. Just in case
	flush(opts);
}

std::string AsyncStorage::GetPath() const noexcept {
	std::lock_guard lck(storageMtx_);
	return path_;
}

datastorage::StorageType AsyncStorage::GetType() const noexcept {
	std::lock_guard lck(storageMtx_);
	if (storage_) {
		return storage_->Type();
	}
	return datastorage::StorageType::LevelDB;
}

void AsyncStorage::InheritUpdatesFrom(AsyncStorage& src, AsyncStorage::FullLockT& storageLock) {
	if (!storageLock.OwnsThisFlushMutex(src.flushMtx_)) {
		throw Error(errLogic, "Storage must be locked during updates inheritance (flush mutex)");
	}
	if (!storageLock.OwnsThisStorageMutex(src.storageMtx_)) {
		throw Error(errLogic, "Storage must be locked during updates inheritance (updates mutex)");
	}

	std::lock_guard lck(storageMtx_);

	if (!isCopiedNsStorage_) {
		throw Error(errLogic, "Updates inheritance is supposed to work with copied storages");
	}
	if (src.storage_) {
		if (src.storage_.get() != storage_.get()) {
			throw Error(errLogic, "Unable to inherit storage updates from another underlying storage");
		}
		if (src.curUpdatesChunck_) {
			totalUpdatesCount_.fetch_add(src.curUpdatesChunck_.updatesCount, std::memory_order_release);
			src.totalUpdatesCount_.fetch_sub(src.curUpdatesChunck_.updatesCount, std::memory_order_release);
			finishedUpdateChuncks_.push_front(std::move(src.curUpdatesChunck_));
			if (lastBatchWithSyncUpdates_ >= 0) ++lastBatchWithSyncUpdates_;
		}
		while (src.finishedUpdateChuncks_.size()) {
			auto& upd = src.finishedUpdateChuncks_.back();
			totalUpdatesCount_.fetch_add(upd.updatesCount, std::memory_order_release);
			src.totalUpdatesCount_.fetch_sub(upd.updatesCount, std::memory_order_release);
			finishedUpdateChuncks_.push_front(std::move(upd));
			src.finishedUpdateChuncks_.pop_back();
			if (lastBatchWithSyncUpdates_ >= 0) ++lastBatchWithSyncUpdates_;
		}
		src.storage_.reset();
		// Do not update lockfree status here to avoid status flickering on ns copying
	}
	isCopiedNsStorage_ = false;
}

void AsyncStorage::clearUpdates() {
	finishedUpdateChuncks_.clear();
	curUpdatesChunck_.reset();
	recycled_.clear();
	totalUpdatesCount_.store(0, std::memory_order_release);
}

void AsyncStorage::flush(const StorageFlushOpts& opts) {
	if (isCopiedNsStorage_) {
		return;
	}
	if (!storage_) {
		return;
	}
	UpdatesPtrT uptr;
	try {
		if (totalUpdatesCount_.load(std::memory_order_acquire)) {
			std::unique_lock lck(storageMtx_, std::defer_lock_t());
			if (!lastFlushError_.ok()) {
				if (reopenTs_ > ClockT::now() && !opts.IsWithImmediateReopen()) {
					throw lastFlushError_;
				} else {
					tryReopenStorage();
				}
			}

			auto flushChunk = [this, &lck](UpdatesPtrT&& uptr) {
				assertrx(lck.owns_lock());
				lck.unlock();

				Error status;
				try {
					status = storage_->Write(StorageOpts(), *uptr);
				} catch (Error& e) {
					status = std::move(e);
				} catch (...) {
					status = Error(errLogic, "Unhandled exception");
				}
				if (!status.ok()) {
					lck.lock();

					totalUpdatesCount_.fetch_add(uptr.updatesCount, std::memory_order_release);
					finishedUpdateChuncks_.emplace_front(std::move(uptr));
					scheduleFilesReopen(Error(errLogic, "Error write to storage in '%s': %s", path_, status.what()));
					throw lastFlushError_;
				}

				uptr->Clear();
				uptr.updatesCount = 0;

				lck.lock();
				if (lastBatchWithSyncUpdates_ >= 0) {
					--lastBatchWithSyncUpdates_;
				}
				recycleUpdatesCollection(std::move(uptr));
			};

			// Flush finished chunks
			lck.lock();
			while (finishedUpdateChuncks_.size()) {
				uptr = std::move(finishedUpdateChuncks_.front());
				finishedUpdateChuncks_.pop_front();
				totalUpdatesCount_.fetch_sub(uptr.updatesCount, std::memory_order_release);

				flushChunk(std::move(uptr));
			}

			// Flush last chunk
			if (batchingAdvices_.load(std::memory_order_acquire) <= 0) {
				const auto currentUpdates = totalUpdatesCount_.load(std::memory_order_relaxed);
				if (currentUpdates) {
					uptr = std::move(curUpdatesChunck_);
					curUpdatesChunck_ = createUpdatesCollection();
					totalUpdatesCount_.store(0, std::memory_order_release);

					flushChunk(std::move(uptr));
				}
			}
		}
	} catch (const Error&) {
		if (opts.IsWithIgnoreFlushError()) {
			return;
		}
		throw;
	}
}

void AsyncStorage::beginNewUpdatesChunk() {
	finishedUpdateChuncks_.emplace_back(std::move(curUpdatesChunck_));
	curUpdatesChunck_ = createUpdatesCollection();
}

AsyncStorage::UpdatesPtrT AsyncStorage::createUpdatesCollection() noexcept {
	UpdatesPtrT ret;
	if (storage_) {
		if (recycled_.size()) {
			ret = std::move(recycled_.back());
			recycled_.pop_back();
		} else {
			ret.reset(storage_->GetUpdatesCollection());
		}
	}
	return ret;
}

void AsyncStorage::recycleUpdatesCollection(AsyncStorage::UpdatesPtrT&& uptr) noexcept {
	assertrx(uptr.updatesCount == 0);
	if (storage_ && recycled_.size() < kMaxRecycledChunks) {
		recycled_.emplace_back(std::move(uptr));
		return;
	}
	uptr.reset();
}

void AsyncStorage::tryReopenStorage() {
	throwOnStorageCopy();
	if (!lastFlushError_.ok()) {
		auto err = storage_->Reopen();
		if (!err.ok()) {
			logPrintf(LogInfo, "Atempt to reopen storage for '%s' failed: %s", path_, err.what());
			scheduleFilesReopen(std::move(lastFlushError_));
			throw lastFlushError_;
		}
		logPrintf(LogInfo, "Storage was reopened for '%s'", path_);
		setLastFlushError(Error());
		reopenTs_ = TimepointT();
	}
}

}  // namespace reindexer
