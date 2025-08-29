#pragma once

#include <chrono>
#include <vector>
#include "core/queryresults/queryresults.h"
#include "core/type_consts.h"
#include "estl/lock.h"
#include "loggerwrapper.h"
#include "net/ev/ev.h"
#include "tools/assertrx.h"

namespace reindexer_server {

using namespace reindexer;

class [[nodiscard]] RPCQrWatcher {
public:
	constexpr static int64_t kUninitialized = -1;
	constexpr static int64_t kDisabled = -2;
	constexpr static uint32_t kMaxConcurrentQRCount = 65536;
	constexpr static uint32_t kChuncksCount = 32;
	static_assert(kMaxConcurrentQRCount % kChuncksCount == 0, "Each chunck will must have the same size");
	constexpr static uint32_t kChunkSize = kMaxConcurrentQRCount / kChuncksCount;

	RPCQrWatcher(std::chrono::seconds idleTimeout)
		: idleTimeout_(idleTimeout.count() > 0 ? (idleTimeout + std::chrono::seconds(1)) : idleTimeout) {}

	class [[nodiscard]] Ref {
	public:
		Ref() = default;
		Ref(const Ref&) = delete;
		Ref& operator=(const Ref&) = delete;
		Ref(Ref&& o) noexcept : d_(o.d_) {
			o.d_.owner = nullptr;
			o.d_.qr = nullptr;
		}
		Ref& operator=(Ref&& o) noexcept {
			if (this != &o) {
				d_ = o.d_;
				o.d_.owner = nullptr;
				o.d_.qr = nullptr;
			}
			return *this;
		}
		~Ref() {
			if (d_.owner) {
				assertrx(d_.qr);
				d_.owner->onRefDestroyed(d_.id);
			}
		}
		QueryResults& operator*() {
			if (!d_.qr) {
				throw Error(errLogic, "Query results' pointer is nullptr");
			}
			return *d_.qr;
		}
		uint32_t ID() const noexcept { return d_.id; }

	private:
		Ref(uint32_t id, QueryResults& qr, RPCQrWatcher& owner) noexcept : d_{id, &qr, &owner} {}

		friend class RPCQrWatcher;

		struct [[nodiscard]] Data {
			uint32_t id = 0;
			QueryResults* qr = nullptr;
			RPCQrWatcher* owner = nullptr;
		};

		Data d_;
	};

	Ref GetQueryResults(RPCQrId& id, int flags) {
		if (id.main < 0) {
			if (id.uid != kDisabled) {
				id.uid = uidCounter_.fetch_add(1, std::memory_order_relaxed) & kUIDValueBitmask;
			}

			Ref ref = createQueryResults(id.uid, flags);
			id.main = ref.ID();
			return ref;
		}
		return getQueryResults(uint32_t(id.main), id.uid);
	}
	bool AreQueryResultsValid(RPCQrId id) const noexcept {
		if (!isMainIDValid(id.main) || !isUIDValid(id.uid)) {
			return false;
		}
		const auto& qrs = qrs_[uint32_t(id.main)];
		const UID curUID = qrs.uid.load(std::memory_order_acquire);
		return (id.uid >= 0 && curUID.state == UID::InitializedUIDEnabled && uint64_t(id.uid) == curUID.val) ||
			   (id.uid == kDisabled && curUID.state == UID::InitializedUIDDisabled);
	}
	void FreeQueryResults(RPCQrId id, bool strictCheck) {
		checkIDs(id.main, id.uid);
		auto& qrs = qrs_[uint32_t(id.main)];
		UID curUID = qrs.uid.load(std::memory_order_acquire);
		if (curUID.freed) {
			if (strictCheck) {
				throw Error(errNotFound, "Unexpected Query Results ID: {} (it was already freed)", id.main);
			} else {
				return;
			}
		}
		bool shouldClearQRs;
		UID newUID;
		do {
			shouldClearQRs = false;
			if ((id.uid >= 0 && curUID.state == UID::InitializedUIDEnabled && uint64_t(id.uid) == curUID.val) ||
				(id.uid == kDisabled && curUID.state == UID::InitializedUIDDisabled)) {
				newUID = curUID;
				newUID.freed = 1;

				if (shouldClearQRs) {
					newUID.state = UID::ClearingInProgress;
				}
				if (curUID.refs == 0) {
					shouldClearQRs = true;
					newUID.state = UID::ClearingInProgress;
				} else {
					shouldClearQRs = false;
				}
			} else if (strictCheck) {
				throw Error(errQrUIDMissmatch,
							"Unexpected Query Results UID (most likely those query results were reset by idle timeout): {} vs {}(state:{})",
							id.uid, uint64_t(curUID.val), uint64_t(curUID.state));
			} else {
				return;
			}
		} while (!qrs.uid.compare_exchange_strong(curUID, newUID, std::memory_order_acq_rel));
		if (shouldClearQRs) {
			qrs.qr = QueryResults();
			newUID.SetUnitialized();
			qrs.uid.store(newUID, std::memory_order_release);

			lock_guard lck(mtx_);
			putFreeID(uint32_t(id.main));
		}
	}
	void Register(net::ev::dynamic_loop& loop, LoggerWrapper logger);
	void Stop();

private:
	constexpr static int64_t kUIDValueBitmask = int64_t(0x1FFFFFFFFFFFFF);
	constexpr static uint32_t kMaxQRRefsCount = 127;
	struct [[nodiscard]] UID {
		enum [[nodiscard]] State {
			Uninitialized = 0,
			InitializedUIDEnabled = 1,
			InitializedUIDDisabled = 2,
			ClearingInProgress = 3,
		};

		UID() noexcept : freed(0), state(Uninitialized), refs(0), val(0) {}
		UID(int64_t uid, bool addRef) noexcept
			: freed(0), state(uid >= 0 ? InitializedUIDEnabled : InitializedUIDDisabled), refs(addRef ? 1 : 0), val(uid >= 0 ? uid : 0) {
			assertf(uid == kDisabled || val == (uid & kUIDValueBitmask), "UID: {}, val: {}", uint64_t(uid), uint64_t(val));
		}
		void SetUnitialized() noexcept {
			state = UID::Uninitialized;
			freed = 0;
		}

		uint64_t freed : 1;
		uint64_t state : 3;
		uint64_t refs : 7;
		uint64_t val : 53;
	};
	static_assert(std::atomic<UID>::is_always_lock_free, "Expection UID to be lockfree");
	struct [[nodiscard]] QrStorage {
		QrStorage() = default;
		QrStorage(const QrStorage&) = delete;
		QrStorage(QrStorage&& o)
			: uid(o.uid.load(std::memory_order_relaxed)),
			  lastAccessTime(o.lastAccessTime.load(std::memory_order_relaxed)),
			  qr(std::move(o.qr)) {}

		bool IsExpired(int64_t now, int64_t idleTimeout) const noexcept {
			return IsExpired(uid.load(std::memory_order_acquire), now, idleTimeout);
		}
		bool IsExpired(UID curUID, int64_t now, int64_t idleTimeout) const noexcept {
			if (curUID.refs == 0 && curUID.state == UID::InitializedUIDEnabled) {
				const auto lastAccess = lastAccessTime.load(std::memory_order_relaxed);
				return lastAccess >= 0 && (now > lastAccess) && (now - lastAccess > idleTimeout);
			}
			return false;
		}

		std::atomic<UID> uid;
		std::atomic<int32_t> lastAccessTime = {kUninitialized};
		QueryResults qr;
	};
	template <typename T>
	class [[nodiscard]] PartitionedArray {
	public:
		PartitionedArray() { array_[0].reserve(kChunkSize); }
		T& operator[](uint32_t n) noexcept {
			const uint32_t vidx = n / kChunkSize;
			const uint32_t idx = n % kChunkSize;
			return array_[vidx][idx];
		}
		const T& operator[](uint32_t n) const noexcept {
			const uint32_t vidx = n / kChunkSize;
			const uint32_t idx = n % kChunkSize;
			return array_[vidx][idx];
		}
		template <typename... Args>
		void emplace_back(Args&&... args) {
			uint32_t chunkId = size_ / kChunkSize;
			if (array_[chunkId].capacity() < kChunkSize) {
				if (chunkId >= kChuncksCount - 1) {
					throw Error(errParams, "Too many concurrent query results. Limit is: {}", kChuncksCount * kChunkSize);
				}
				array_[chunkId].reserve(kChunkSize);
			}
			array_[chunkId].emplace_back(std::forward<Args>(args)...);
			++size_;
		}
		uint32_t size() const noexcept { return size_; }

	private:
		std::array<std::vector<T>, kChuncksCount> array_;
		uint32_t size_ = 0;
	};

	void checkIDs(int32_t id, int64_t uid) const {
		if (!isMainIDValid(id)) {
			throw Error(errLogic, "Unexpected Query Results ID: {}", id);
		}
		if (!isUIDValid(uid)) {
			throw Error(errLogic, "Unexpected Query Results UID: {}", uid);
		}
	}
	bool isMainIDValid(int32_t id) const noexcept { return id < int32_t(allocated_.load(std::memory_order_acquire)) && id >= 0; }
	bool isUIDValid(int64_t uid) const noexcept { return uid == (uid & kUIDValueBitmask) || uid == kDisabled || uid == kUninitialized; }
	void onRefDestroyed(uint32_t id) {
		[[maybe_unused]] const auto allocated = allocated_.load(std::memory_order_acquire);
		assertf(id < allocated, "id: {}, allocated: {}", id, allocated);
		auto& qrs = qrs_[id];
		UID curUID = qrs.uid.load(std::memory_order_acquire);
		// QR can not be removed, while 1 or more Refs exist
		assertrx(curUID.state == UID::InitializedUIDEnabled || curUID.state == UID::InitializedUIDDisabled);
		bool shouldClearQRs;
		UID newUID;
		do {
			assertrx(curUID.refs > 0);
			shouldClearQRs = curUID.freed && (curUID.refs == 1);
			newUID = curUID;
			--newUID.refs;

			if (shouldClearQRs) {
				newUID.state = UID::ClearingInProgress;
			} else {
				qrs.lastAccessTime.store(now(), std::memory_order_relaxed);
			}
		} while (!qrs.uid.compare_exchange_strong(curUID, newUID, std::memory_order_acq_rel));
		if (shouldClearQRs) {
			qrs.qr = QueryResults();
			newUID.SetUnitialized();
			qrs.uid.store(newUID, std::memory_order_release);

			lock_guard lck(mtx_);
			putFreeID(id);
		}
	}
	Ref createQueryResults(int64_t uid, int flags) {
		std::pair<uint32_t, bool> freeIDP;
		{
			lock_guard lck(mtx_);
			freeIDP = tryPopFreeID();
			if (!freeIDP.second) {
				freeIDP.first = uint32_t(qrs_.size());
				qrs_.emplace_back();
				allocated_.store(qrs_.size(), std::memory_order_release);
			}
		}

		auto& qrs = qrs_[freeIDP.first];
		qrs.lastAccessTime.store(now(), std::memory_order_relaxed);
		qrs.uid.store(UID(uid, true), std::memory_order_release);
		qrs.qr.setFlags(flags);
		return Ref(freeIDP.first, qrs.qr, *this);
	}
	Ref getQueryResults(uint32_t id, int64_t uid) {
		checkIDs(int32_t(id), uid);
		auto& qrs = qrs_[id];
		UID curUID = qrs.uid.load(std::memory_order_acquire);
		UID newUID;
		do {
			newUID = curUID;
			if ((uid >= 0 && curUID.state == UID::InitializedUIDEnabled && uint64_t(uid) == curUID.val) ||
				(uid == kDisabled && curUID.state == UID::InitializedUIDDisabled)) {
				if (newUID.refs == kMaxQRRefsCount) {
					throw Error(errLogic, "Unexpected Query Results refs count. It must be less than {}", kMaxQRRefsCount);
				}
				++newUID.refs;
			} else {
				throw Error(errQrUIDMissmatch,
							"Unexpected Query Results UID (most likely those query results were reset by idle timeout): {} vs {}(state:{})",
							uid, uint64_t(curUID.val), uint64_t(curUID.state));
			}
		} while (!qrs.uid.compare_exchange_strong(curUID, newUID, std::memory_order_acq_rel));
		qrs.lastAccessTime.store(now(), std::memory_order_relaxed);
		return Ref(id, qrs.qr, *this);
	}
	uint32_t removeExpired(uint32_t now, uint32_t from, uint32_t to);
	uint32_t now() const noexcept { return nowSeconds_.load(std::memory_order_relaxed); }

	void putFreeID(uint32_t id) noexcept { freeIDs_[freeIDsCnt_++] = id; }
	std::pair<uint32_t, bool> tryPopFreeID() noexcept {
		if (freeIDsCnt_) {
			return std::make_pair(freeIDs_[--freeIDsCnt_], true);
		}
		return std::make_pair(0, false);
	}

	const std::chrono::seconds idleTimeout_;
	std::array<uint32_t, kMaxConcurrentQRCount> freeIDs_ = {};
	uint32_t freeIDsCnt_ = 0;
	PartitionedArray<QrStorage> qrs_;
	reindexer::mutex mtx_;
	std::atomic<uint32_t> allocated_ = {0};
	std::atomic<int64_t> uidCounter_ = 1;

	net::ev::timer timer_;
	std::atomic<uint32_t> nowSeconds_ = {0};
};

}  // namespace reindexer_server
