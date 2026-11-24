#include "asyncstorage.h"
#include "core/storage/storagefactory.h"
#include "tools/catch_and_return.h"
#include "tools/logger.h"

namespace reindexer {

void AsyncStorage::Close() {
	lock_guard flushLck(flushMtx_);
	flush(StorageFlushOpts().WithImmediateReopen());

	lock_guard lck(storageMtx_);
	clearUpdates();
	reset();
}

AsyncStorage::AsyncStorage(shared_ptr<datastorage::IDataStorage> storage) : storage_{std::move(storage)}, proxy_(*this) {
	if (!storage_) {
		throw Error(errParams, "Cannot adopt an empty storage");
	}
	curUpdatesChunck_ = createUpdatesCollection();
	updateStatusCache();
}

AsyncStorage::AsyncStorage(const AsyncStorage& o, AsyncStorage::FullLock& storageLock) : isCopiedNsStorage_{true}, proxy_(*this) {
	if (!storageLock.OwnsThis(o.flushMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (flush mutex)");
	}
	if (!storageLock.OwnsThis(o.storageMtx_)) {
		throw Error(errLogic, "Storage must be locked during copying (updates mutex)");
	}
	pendingSysRecords_ = o.pendingSysRecords_;
	storage_ = o.storage_;
	path_ = o.path_;
	withProxy_.store(o.withProxy_.load());
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
	if (!storageLock.OwnsThis(src.flushMtx_) || !storageLock.OwnsThis(src.storageMtx_)) {
		throw Error(errLogic, "Storage must be fully locked during updates inheritance");
	}
	FullLock lck{flushMtx_, storageMtx_};
	if (!isCopiedNsStorage_) {
		throw Error(errLogic, "Updates inheritance is supposed to work with copied storages");
	}
	if (src.storage_) {
		if (src.storage_.get() != storage_.get()) {
			throw Error(errLogic, "Unable to inherit storage updates from another underlying storage");
		}

		// Allocate everything before the write or ownership transfer. A failure must leave src usable.
		std::deque<UpdatesPtrT> inherited;
		auto append = [&inherited](const UpdatesPtrT& updates) {
			if (updates && updates.updatesCount) {
				inherited.emplace_back(datastorage::UpdatesCollection::Ptr{updates}, updates.updatesCount);
			}
		};
		for (const auto& updates : src.finishedUpdateChuncks_) {
			append(updates);
		}
		append(src.curUpdatesChunck_);
		for (const auto& updates : finishedUpdateChuncks_) {
			append(updates);
		}
		auto sysRecords = src.pendingSysRecords_;
		for (const auto& [key, value] : pendingSysRecords_) {
			sysRecords.insert_or_assign(key, value);
		}
		if (!sysRecords.empty()) {
			src.writeSysRecords(sysRecords);
		}

		finishedUpdateChuncks_.swap(inherited);
		totalUpdatesCount_.fetch_add(src.totalUpdatesCount_.load(std::memory_order_relaxed), std::memory_order_release);
		batchIdCounter_.store(std::max(batchIdCounter_.load(), src.batchIdCounter_.load()), std::memory_order_release);
		pendingSysRecords_.clear();
		src.clearUpdates();
		src.storage_.reset();
		// Do not update lockfree status here to avoid status flickering on ns copying.
	}
	isCopiedNsStorage_ = false;
}

void AsyncStorage::WriteSysRecords(std::span<const SysRecordUpdate> updates) {
	lock_guard lck(storageMtx_);
	stageSysRecords(updates);
	if (!isCopiedNsStorage_) {
		flushPendingSysRecords();
	}
}

void AsyncStorage::WriteSysRecords(std::span<const SysRecordUpdate> updates, Error& deferredError) {
	lock_guard lck(storageMtx_);
	stageSysRecords(updates);
	if (!isCopiedNsStorage_) {
		try {
			flushPendingSysRecords();
		} catch (Error& err) {
			if (deferredError.ok()) {
				deferredError = std::move(err);
			}
		}
	}
}

void AsyncStorage::stageSysRecords(std::span<const SysRecordUpdate> updates) {
	if (!storage_) {
		throw Error(errLogic, "Cannot write system records to a disabled storage");
	}
	// Updating a group must be exception-safe even if copying one of the keys/values fails.
	auto pending = pendingSysRecords_;
	for (const auto& update : updates) {
		pending.insert_or_assign(std::string{update.key}, update.value ? std::optional<std::string>{*update.value} : std::nullopt);
	}
	pendingSysRecords_.swap(pending);
}

Error AsyncStorage::ReadSysRecord(std::string_view key, std::string& value) const noexcept {
	try {
		lock_guard lck(storageMtx_);
		if (auto it = pendingSysRecords_.find(key); it != pendingSysRecords_.end()) {
			if (!isCopiedNsStorage_) {
				return lastFlushError_.ok() ? Error(errLogic, "System record is awaiting a durable write") : lastFlushError_;
			}
			if (!it->second) {
				return Error(errNotFound, "System record was removed from async storage");
			}
			value = *it->second;
			return Error();
		}
		if (!storage_) {
			return Error(errLogic, "Cannot read system records from a disabled storage");
		}
		return storage_->Read(StorageOpts{}.FillCache(), key, value);
	} CATCH_AND_RETURN
}

void AsyncStorage::writeSysRecords(const SysRecordUpdates& updates) {
	if (updates.empty()) {
		return;
	}
	if (!lastFlushError_.ok()) {
		throw lastFlushError_;
	}
	std::unique_ptr<datastorage::UpdatesCollection> batch{storage_->GetUpdatesCollection()};
	for (const auto& [key, value] : updates) {
		if (value) {
			batch->Put(key, *value);
		} else {
			batch->Remove(key);
		}
	}
	try {
		auto err = storage_->Write(StorageOpts{}.Sync(), *batch);
		if (!err.ok()) {
			throw err;
		}
	} catch (const Error& err) {
		scheduleFilesReopen(Error{err});
		throw;
	}
}

void AsyncStorage::flushPendingSysRecords() {
	writeSysRecords(pendingSysRecords_);
	pendingSysRecords_.clear();
}

void AsyncStorage::clearUpdates() {
	pendingSysRecords_.clear();
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
		unique_lock lck(storageMtx_);
		if (!lastFlushError_.ok()) {
			if (reopenTs_ > ClockT::now_coarse() && !opts.IsWithImmediateReopen()) {
				throw lastFlushError_;
			}
			tryReopenStorage();
		}
		// Persist pending system metadata before any item batch that may depend on it.
		flushPendingSysRecords();
		if (totalUpdatesCount_.load(std::memory_order_acquire)) {
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
				// System metadata may have failed while this batch was in flight. Stop before any further item batch.
				flushPendingSysRecords();
			};

			// Flush finished chunks
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
