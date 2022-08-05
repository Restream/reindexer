#include "asyncstorage.h"
#include "core/storage/storagefactory.h"

namespace reindexer {

void AsyncStorage::Close() {
	std::lock_guard flushLck(flushMtx_);
	flush();

	std::lock_guard lck(updatesMtx_);
	clearUpdates();
	storage_.reset();
	path_.clear();
}

AsyncStorage::AsyncStorage(const AsyncStorage& o, AsyncStorage::FullLockT& storageLock) : isCopiedNsStorage_{true} {
	if (!storageLock.OwnsThisFlushMutex(o.flushMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (flush mutex)");
	}
	if (!storageLock.OwnsThisUpdatesMutex(o.updatesMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (updates mutex)");
	}
	storage_ = o.storage_;
	path_ = o.path_;
	curUpdatesChunck_ = createUpdatesCollection();
}

Error AsyncStorage::Open(datastorage::StorageType storageType, const std::string& nsName, const std::string& path,
						 const StorageOpts& opts) {
	auto lck = FullLock();

	if (storage_) {
		throw Error(errLogic, "Storage already enabled for namespace '%s' on path '%s'", nsName, path_);
	}
	storage_.reset(datastorage::StorageFactory::create(storageType));
	auto err = storage_->Open(path, opts);
	if (err.ok()) {
		path_ = path;
		curUpdatesChunck_ = createUpdatesCollection();
	}
	return err;
}

void AsyncStorage::Destroy() {
	auto lck = FullLock();

	if (storage_) {
		clearUpdates();
		storage_->Destroy(path_);
		storage_.reset();
		path_.clear();
	}
}

std::unique_ptr<datastorage::Cursor> AsyncStorage::GetCursor(StorageOpts& opts) const {
	std::lock_guard lck(updatesMtx_);
	return std::unique_ptr<datastorage::Cursor>(storage_->GetCursor(opts));
}

void AsyncStorage::WriteSync(const StorageOpts& opts, std::string_view key, std::string_view value) {
	std::lock_guard lck(updatesMtx_);
	if (!isCopiedNsStorage_) {
		if (storage_) {
			storage_->Write(opts, key, value);
		}
		return;
	}
	write(key, value);	// write async for copied namespace
}

std::weak_ptr<datastorage::IDataStorage> AsyncStorage::GetStoragePtr() const {
	std::lock_guard lck(updatesMtx_);
	if (isCopiedNsStorage_) {
		throw Error(errLogic, "Unable to get direct storage pointer from copied namespace");
	}
	return storage_;
}

void AsyncStorage::Flush() {
	// Flush must be performed in single thread
	std::lock_guard flushLck(flushMtx_);
	flush();
}

std::string AsyncStorage::Path() const noexcept {
	std::lock_guard lck(updatesMtx_);
	return path_;
}

datastorage::StorageType AsyncStorage::Type() const noexcept {
	std::lock_guard lck(updatesMtx_);
	if (storage_) {
		return storage_->Type();
	}
	return datastorage::StorageType::LevelDB;
}

void AsyncStorage::InheritUpdatesFrom(AsyncStorage& src, AsyncStorage::FullLockT& storageLock) {
	if (!storageLock.OwnsThisFlushMutex(src.flushMtx_)) {
		throw Error(errLogic, "Storage must be locked during updates inheritance (flush mutex)");
	}
	if (!storageLock.OwnsThisUpdatesMutex(src.updatesMtx_)) {
		throw Error(errLogic, "Storage must be locked during updates inheritance (updates mutex)");
	}

	std::lock_guard lck(updatesMtx_);
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
		src.storage_.reset();
	}
	isCopiedNsStorage_ = false;
}

void AsyncStorage::clearUpdates() {
	finishedUpdateChuncks_.clear();
	curUpdatesChunck_.reset();
	recycled_.clear();
	totalUpdatesCount_.store(0, std::memory_order_release);
}

void AsyncStorage::flush() {
	if (isCopiedNsStorage_) {
		return;
	}
	if (!storage_) {
		return;
	}
	UpdatesPtrT uptr;
	if (totalUpdatesCount_.load(std::memory_order_acquire)) {
		std::unique_lock lck(updatesMtx_, std::defer_lock_t());

		auto flushChunk = [this, &lck](UpdatesPtrT&& uptr) {
			assertrx(lck.owns_lock());
			lck.unlock();
			const auto status = storage_->Write(StorageOpts(), *uptr);
			if (!status.ok()) {
				lck.lock();
				totalUpdatesCount_.fetch_add(uptr.updatesCount, std::memory_order_release);
				finishedUpdateChuncks_.emplace_front(std::move(uptr));
				throw Error(errLogic, "Error write to storage in '%s': %s", path_, status.what());
			}
			uptr->Clear();
			uptr.updatesCount = 0;

			lck.lock();
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

}  // namespace reindexer
