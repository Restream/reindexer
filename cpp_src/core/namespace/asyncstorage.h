#pragma once

#include <deque>
#include <numeric>
#include <optional>
#include "core/storage/idatastorage.h"
#include "estl/fast_hash_map.h"
#include "estl/h_vector.h"
#include "estl/lock.h"
#include "estl/marked_mutex.h"
#include "estl/multi_unique_lock.h"
#include "estl/mutex.h"
#include "estl/shared_mutex.h"
#include "estl/tagged_mutex.h"
#include "tools/assertrx.h"
#include "tools/clock.h"
#include "tools/flagguard.h"
#include "tools/stringstools.h"

namespace reindexer {

class [[nodiscard]] StorageFlushOpts {
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
	enum [[nodiscard]] OptType {
		kOptTypeImmediateReopen = 0x1,
		kOptTypeIgnoreFlushError = 0x2,
	};

	uint8_t opts_;
};

class [[nodiscard]] AsyncStorage {
	using Mutex = MarkedMutex<mutex, MutexMark::AsyncStorage>;
	using FlushMutex = TaggedMutex<Mutex, 0>;
	using StorageMutex = TaggedMutex<Mutex, 1>;
	friend class Namespace;

public:
	static constexpr uint32_t kLimitToAdviceBatching = 1000;
	static constexpr auto kStorageReopenPeriod = std::chrono::seconds(15);
	constexpr static uint32_t kFlushChunckSize = 11000;

	using AdviceGuardT = CounterGuardAIRL32;
	using ClockT = system_clock_w;
	using TimepointT = ClockT::time_point;
	using FullLock = DoubleUniqueLock<FlushMutex, StorageMutex>;

	struct [[nodiscard]] Status {
		bool isEnabled = false;
		Error err;
	};

	class [[nodiscard]] ConstCursor {
	public:
		ConstCursor(unique_lock<StorageMutex>&& lck, std::unique_ptr<datastorage::Cursor>&& c) noexcept
			: lck_(std::move(lck)), c_(std::move(c)) {
			assertrx(lck_.owns_lock());
			assertrx(c_);
		}
		datastorage::Cursor* operator->() noexcept { return c_.get(); }

	protected:
		// NOTE: Cursor owns unique storage lock. I.e. nobody is able to read stroage or write into it, while cursor exists.
		// Currently the only place, where it matter is EnumMeta method. However, we should to consider switching to shared_mutex, if
		// the number of such concurrent Cursors will grow.
		unique_lock<StorageMutex> lck_;
		std::unique_ptr<datastorage::Cursor> c_;
	};

	class [[nodiscard]] Cursor : public ConstCursor {
	public:
		Cursor(unique_lock<StorageMutex>&& lck, std::unique_ptr<datastorage::Cursor>&& c, AsyncStorage& storage) noexcept
			: ConstCursor(std::move(lck), std::move(c)), storage_(&storage) {}

		void RemoveThisKey(const StorageOpts& opts) {
			assertrx(ConstCursor::lck_.owns_lock());
			storage_->modifySync<ModifyOp::Remove>(opts, ConstCursor::c_->Key());
		}

	private:
		AsyncStorage* storage_ = nullptr;
	};

	AsyncStorage() : proxy_(*this) {}
	AsyncStorage(const AsyncStorage& o, AsyncStorage::FullLock& storageLock);

	Error Open(datastorage::StorageType storageType, std::string_view nsName, const std::string& path, const StorageOpts& opts)
		RX_REQUIRES(!flushMtx_, !storageMtx_);
	void Destroy() RX_REQUIRES(!flushMtx_, !storageMtx_);
	Cursor GetCursor(StorageOpts& opts);
	ConstCursor GetCursor(StorageOpts& opts) const;
	// Tries to write synchronously, hovewer will perform an async write for copied namespace and in case of storage errors
	void WriteSync(const StorageOpts& opts, std::string_view key, std::string_view value) {
		lock_guard lck(storageMtx_);
		modifySync<ModifyOp::Write>(opts, key, value);
	}
	// Tries to remove synchronously, hovewer will perform an async deletion for copied namespace and in case of storage errors
	void RemoveSync(const StorageOpts& opts, std::string_view key) {
		lock_guard lck(storageMtx_);
		modifySync<ModifyOp::Remove>(opts, key);
	}

	void Remove(std::string_view key) {
		lock_guard lck(storageMtx_);
		modifyAsync<ModifyOp::Remove>(key);
	}

	void Write(std::string_view key, std::string_view data) {
		lock_guard lck(storageMtx_);
		modifyAsync<ModifyOp::Write>(key, data);
	}

	Error Read(const StorageOpts& opts, std::string_view key, std::string& value) const {
		auto [proxyValue, err] = proxy_.Find(key);
		if (!err.ok()) {
			return err;
		}
		if (proxyValue) {
			value = std::move(*proxyValue);
			return err;
		}

		lock_guard lck(storageMtx_);
		if (storage_) {
			return storage_->Read(opts, key, value);
		}
		return Error();
	}
	void Close() RX_REQUIRES(!flushMtx_, !storageMtx_);
	void Flush(const StorageFlushOpts& opts) RX_REQUIRES(!flushMtx_, !storageMtx_);
	void TryForceFlush() RX_REQUIRES(!flushMtx_, !storageMtx_) {
		const auto forceFlushLimit = forceFlushLimit_.load(std::memory_order_relaxed);
		if (forceFlushLimit && totalUpdatesCount_.load(std::memory_order_acquire) >= forceFlushLimit) {
			// Flush must be performed in single thread
			lock_guard flushLck(flushMtx_);
			if (totalUpdatesCount_.load(std::memory_order_acquire) >= forceFlushLimit) {
				flush(StorageFlushOpts().WithIgnoreFlushError());
			}
		}
	}
	bool IsValid() const noexcept {
		lock_guard lck(storageMtx_);
		return storage_.get();
	}
	Status GetStatusCached() const noexcept { return statusCache_.GetStatus(); }
	std::string GetPathCached() const noexcept { return statusCache_.GetPath(); }
	std::string GetPath() const noexcept;
	datastorage::StorageType GetType() const noexcept;
	void InheritUpdatesFrom(AsyncStorage& src, AsyncStorage::FullLock& storageLock);
	AdviceGuardT AdviceBatching() noexcept { return AdviceGuardT(batchingAdvices_); }
	void SetForceFlushLimit(uint32_t limit) noexcept { forceFlushLimit_.store(limit, std::memory_order_relaxed); }

	void WithProxy(bool withProxy) noexcept {
		withProxy_ = withProxy;
		if (!withProxy) {
			proxy_.Clear();
		}
	}
	bool WithProxy() const noexcept { return withProxy_; }
	size_t GetProxyMemStat() const noexcept { return proxy_.AllocatedSize(); }

private:
	enum class [[nodiscard]] ModifyOp { Write, Remove };
	constexpr static uint32_t kMaxRecycledChunks = 3;
	class [[nodiscard]] UpdatesPtrT : public datastorage::UpdatesCollection::Ptr {
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
	void flush(const StorageFlushOpts& opts) RX_REQUIRES(flushMtx_, !storageMtx_);
	void beginNewUpdatesChunk();

	template <ModifyOp op>
	void modifyAsyncImpl(std::string_view k, std::optional<std::string_view> v = std::nullopt) {
		if constexpr (op == ModifyOp::Write) {
			curUpdatesChunck_->Put(k, *v);
		} else {
			curUpdatesChunck_->Remove(k);
		}

		proxy_.Add(k, v);
	}

	template <ModifyOp op>
	Error modifySyncImpl(const StorageOpts& opts, std::string_view k, std::optional<std::string_view> v = std::nullopt) {
		throwOnStorageCopy();

		Error err;
		if constexpr (op == ModifyOp::Write) {
			err = storage_->Write(opts, k, *v);
		} else {
			err = storage_->Delete(opts, k);
		}

		if (err.ok()) {
			proxy_.Delete(k);
		}
		return err;
	}

	template <ModifyOp op>
	void modifyAsync(std::string_view k, std::optional<std::string_view> v = std::nullopt) {
		if (storage_) {
			totalUpdatesCount_.fetch_add(1, std::memory_order_release);
			modifyAsyncImpl<op>(k, v);
			if (++curUpdatesChunck_.updatesCount == kFlushChunckSize) {
				beginNewUpdatesChunk();
			}
		}
	}

	template <ModifyOp op>
	void modifySync(const StorageOpts& opts, std::string_view k, std::optional<std::string_view> v = std::nullopt) {
		if (!isCopiedNsStorage_ && lastFlushError_.ok()) {
			if (storage_) {
				Error err;
				try {
					err = modifySyncImpl<op>(opts, k, v);
				} catch (Error& e) {
					err = std::move(e);
				}
				if (!err.ok()) {
					scheduleFilesReopen(std::move(err));
					modifyAsync<op>(k, v);
				}
			}
			return;
		}
		modifyAsync<op>(k, v);
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
		updateStatusCache();
	}
	void tryReopenStorage();
	void updateStatusCache() noexcept { statusCache_.Update(Status{bool(storage_.get()), lastFlushError_}, path_); }
	void setLastFlushError(Error&& e) {
		lastFlushError_ = std::move(e);
		updateStatusCache();
	}

	class [[nodiscard]] StatusCache {
	public:
		void Update(Status&& st, std::string path) noexcept {
			lock_guard lck(mtx_);
			status_ = std::move(st);
			path_ = std::move(path);
		}
		void UpdatePart(bool isEnabled, std::string path) noexcept {
			lock_guard lck(mtx_);
			status_.isEnabled = isEnabled;
			path_ = std::move(path);
		}
		Status GetStatus() const noexcept {
			lock_guard lck(mtx_);
			return status_;
		}
		std::string GetPath() const noexcept {
			lock_guard lck(mtx_);
			return path_;
		}

	private:
		mutable mutex mtx_;
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
	mutable StorageMutex storageMtx_;
	mutable FlushMutex flushMtx_;
	bool isCopiedNsStorage_ = false;
	h_vector<UpdatesPtrT, kMaxRecycledChunks> recycled_;
	std::atomic<int32_t> batchingAdvices_ = {0};
	std::atomic<uint32_t> forceFlushLimit_ = {0};
	Error lastFlushError_;
	TimepointT reopenTs_;

	std::atomic<size_t> batchIdCounter_ = 0;
	struct [[nodiscard]] Proxy {
		Proxy(AsyncStorage& storage) noexcept : storage_(storage) {}

		void Add(std::string_view k, std::optional<std::string_view> v) {
			if (!storage_.withProxy_.load(std::memory_order_relaxed)) {
				return;
			}

			auto key = std::string(k);
			auto value = v ? std::optional<std::string>{*v} : std::nullopt;

			lock_guard lck(mtx_);
			map_[storage_.curUpdatesChunck_->Id()].insert(std::move(key), std::move(value));
		}

		void Delete(std::string_view k) {
			if (!storage_.withProxy_.load(std::memory_order_relaxed)) {
				return;
			}

			auto hash = hash_str{}(k);
			lock_guard lck(mtx_);
			for (auto& [batchId, map] : map_) {
				map.erase(k, hash);
			}
		}

		void Erase(size_t batchId) {
			if (!storage_.withProxy_.load(std::memory_order_relaxed)) {
				return;
			}

			lock_guard lck(mtx_);
			map_.erase(batchId);
		}

		void Clear() noexcept {
			lock_guard lck(mtx_);
			map_ = {};
		}

		std::pair<std::optional<std::string>, Error> Find(std::string_view key) const {
			if (!storage_.withProxy_.load(std::memory_order_relaxed)) {
				return {};
			}

			shared_lock lck(mtx_);

			std::optional<std::string> res;
			auto find = [&, hash = hash_str{}(key)](const auto& map) {
				if (auto it = map.find(key, hash); it != map.end()) {
					res = it.value();
					return true;
				}
				return false;
			};

			if (auto it = std::find_if(map_.rbegin(), map_.rend(), [&find](const auto& pair) { return find(pair.second); });
				it != map_.rend()) {
				return {res, res ? Error() : Error(errNotFound, "Key not found in async storage")};
			}
			return {};
		}

		size_t AllocatedSize() const noexcept {
			shared_lock lck(mtx_);
			return std::accumulate(map_.begin(), map_.end(), map_.size() * sizeof(decltype(map_)::node_type),
								   [](auto res, const auto& pair) { return res + pair.second.allocated_mem_size(); });
		}

	private:
		mutable shared_timed_mutex mtx_;
		using ProxyBatchBase = fast_hash_map<std::string, std::optional<std::string>, hash_str, equal_str, less_str>;
		class [[nodiscard]] ProxyBatch : ProxyBatchBase {
		public:
			using ProxyBatchBase::find;
			using ProxyBatchBase::end;

			void insert(std::string&& key, std::optional<std::string>&& value) {
				auto keyCapacity = key.capacity();
				auto valCapacity = value ? value->capacity() : 0;
				if (auto it = ProxyBatchBase::find(key); it != ProxyBatchBase::end()) {
					auto oldValCapacity = it->second ? it->second->capacity() : 0;
					it->second = std::move(value);
					allocated_ += valCapacity - oldValCapacity;
				} else {
					ProxyBatchBase::operator[](std::move(key)) = std::move(value);
					allocated_ += keyCapacity + valCapacity;
				}
			}
			void erase(const std::string_view& key, size_t precalcHash) {
				if (auto it = ProxyBatchBase::find(key, precalcHash); it != ProxyBatchBase::end()) {
					auto capacityToDecrease = it->first.capacity() + (it->second ? it->second->capacity() : 0);
					ProxyBatchBase::erase(it);
					allocated_ -= capacityToDecrease;
				}
			}

			size_t allocated_mem_size() const noexcept { return allocated_ + ProxyBatchBase::allocated_mem_size(); }

		private:
			size_t allocated_ = 0;
		};

		std::map<size_t, ProxyBatch> map_;
		AsyncStorage& storage_;
	} proxy_;
	std::atomic_bool withProxy_ = false;
};

}  // namespace reindexer
