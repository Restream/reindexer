#include "asyncstorage.h"
#include "core/storage/storagefactory.h"
#include "tools/logger.h"

namespace reindexer {

void AsyncStorage::Close() {
	lock_guard flushLck(flushMtx_);
	flush(StorageFlushOpts().WithImmediateReopen());

	lock_guard lck(storageMtx_);
	clearUpdates();
	reset();
}

AsyncStorage::AsyncStorage(const AsyncStorage& o, AsyncStorage::FullLock& storageLock) : isCopiedNsStorage_{true}, proxy_(*this) {
	if (!storageLock.OwnsThis(o.flushMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (flush mutex)");
	}
	if (!storageLock.OwnsThis(o.storageMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (updates mutex)");
	}
	storage_ = o.storage_;
	path_ = o.path_;
	curUpdatesChunck_ = createUpdatesCollection();
	updateStatusCache();
	// Do not copying lastFlushError_ and reopenTs_, because copied storage does not performs actual writes
}

Error AsyncStorage::Open(datastorage::StorageType storageType, std::string_view nsName, const std::string& path, const StorageOpts& opts) {
	FullLock lck{flushMtx_, storageMtx_};

	throwOnStorageCopy();

	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '{}' on path '{}'", nsName, path_);
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
	FullLock lck{flushMtx_, storageMtx_};

	throwOnStorageCopy();

	if (storage_) {
		clearUpdates();
		storage_->Destroy(path_);
		reset();
	}
}

AsyncStorage::Cursor AsyncStorage::GetCursor(StorageOpts& opts) {
	unique_lock lck(storageMtx_);
	throwOnStorageCopy();
	return Cursor(std::move(lck), std::unique_ptr<datastorage::Cursor>(storage_->GetCursor(opts)), *this);
}

AsyncStorage::ConstCursor AsyncStorage::GetCursor(StorageOpts& opts) const {
	unique_lock lck(storageMtx_);
	throwOnStorageCopy();
	return ConstCursor(std::move(lck), std::unique_ptr<datastorage::Cursor>(storage_->GetCursor(opts)));
}

void AsyncStorage::Flush(const StorageFlushOpts& opts) {
	// Flush must be performed in single thread
	lock_guard flushLck(flushMtx_);
	statusCache_.UpdatePart(bool(storage_.get()), path_);  // Actualize cache part. Just in case
	flush(opts);
}

std::string AsyncStorage::GetPath() const noexcept {
	lock_guard lck(storageMtx_);
	return path_;
}

datastorage::StorageType AsyncStorage::GetType() const noexcept {
	lock_guard lck(storageMtx_);
	if (storage_) {
		return storage_->Type();
	}
	return datastorage::StorageType::LevelDB;
}

void AsyncStorage::InheritUpdatesFrom(AsyncStorage& src, AsyncStorage::FullLock& storageLock) {
	if (!storageLock.OwnsThis(src.flushMtx_)) {
		throw Error(errLogic, "Storage must be locked during updates inheritance (flush mutex)");
	}
	if (!storageLock.OwnsThis(src.storageMtx_)) {
		throw Error(errLogic, "Storage must be locked during updates inheritance (updates mutex)");
	}

	lock_guard lck(storageMtx_);

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
		}
		while (src.finishedUpdateChuncks_.size()) {
			auto& upd = src.finishedUpdateChuncks_.back();
			totalUpdatesCount_.fetch_add(upd.updatesCount, std::memory_order_release);
			src.totalUpdatesCount_.fetch_sub(upd.updatesCount, std::memory_order_release);
			finishedUpdateChuncks_.push_front(std::move(upd));
			src.finishedUpdateChuncks_.pop_back();
		}
		batchIdCounter_.store(src.batchIdCounter_, std::memory_order_release);
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
	batchIdCounter_.store(0, std::memory_order_release);
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
			unique_lock lck(storageMtx_, std::defer_lock);
			if (!lastFlushError_.ok()) {
				if (reopenTs_ > ClockT::now_coarse() && !opts.IsWithImmediateReopen()) {
					throw lastFlushError_;
				} else {
					tryReopenStorage();
				}
			}

			auto flushChunk = [this, &lck](UpdatesPtrT&& uptr) RX_REQUIRES(storageMtx_) RX_NO_THREAD_SAFETY_ANALYSIS {
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
					scheduleFilesReopen(Error(errLogic, "Error write to storage in '{}': {}", path_, status.what()));
					throw lastFlushError_;
				}

				uptr->Clear();
				uptr.updatesCount = 0;

				lck.lock();
				proxy_.Erase(uptr->Id());
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
		ret->Id(batchIdCounter_.fetch_add(1, std::memory_order_acq_rel));
	}
	return ret;
}

void AsyncStorage::recycleUpdatesCollection(AsyncStorage::UpdatesPtrT&& uptr) noexcept {
	assertrx(uptr.updatesCount == 0);
	if (storage_ && recycled_.size() < kMaxRecycledChunks) {
		try {
			recycled_.emplace_back(std::move(uptr));
			// NOLINTBEGIN(bugprone-empty-catch)
		} catch (...) {
			assertrx_dbg(false);
		}
		// NOLINTEND(bugprone-empty-catch)
		return;
	}
	uptr.reset();
}

void AsyncStorage::tryReopenStorage() {
	throwOnStorageCopy();
	if (!lastFlushError_.ok()) {
		auto err = storage_->Reopen();
		if (!err.ok()) {
			logFmt(LogInfo, "Atempt to reopen storage for '{}' failed: {}", path_, err.what());
			scheduleFilesReopen(std::move(lastFlushError_));
			throw lastFlushError_;
		}
		logFmt(LogInfo, "Storage was reopened for '{}'", path_);
		setLastFlushError(Error());
		reopenTs_ = TimepointT();
	}
}

}  // namespace reindexer
