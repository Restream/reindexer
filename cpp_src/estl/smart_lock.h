#pragma once

#include "contexted_locks.h"
#include "core/enums.h"
#include "thread_annotation_attributes.h"

using std::chrono::milliseconds;

namespace reindexer {

constexpr struct UniqueT {
} Unique;
constexpr struct NonUniqueT {
} NonUnique;

template <typename Mutex>
class [[nodiscard]] RX_SCOPED_CAPABILITY smart_lock {
public:
	smart_lock() noexcept : mtx_(nullptr), unique_(false), locked_(false) {}

	smart_lock(Mutex& mtx, UniqueT) RX_ACQUIRE(mtx) : mtx_(&mtx), unique_(true), locked_(true) { mtx_->lock(); }
	smart_lock(Mutex& mtx, NonUniqueT) RX_ACQUIRE_SHARED(mtx) : mtx_(&mtx), unique_(false), locked_(true) { mtx_->lock_shared(); }
	smart_lock(Mutex& mtx, LockUniquely unique) RX_ACQUIRE(mtx)
		: smart_lock{unique ? smart_lock(mtx, Unique) : smart_lock(mtx, NonUnique)} {}
	template <typename Context>
	smart_lock(Mutex& mtx, Context& context, UniqueT, milliseconds chkTimeout = kDefaultCondChkTime) RX_ACQUIRE(mtx)
		: mtx_(&mtx), unique_(true), locked_(false) {
		using namespace std::string_view_literals;
		const auto lockWard = context.BeforeLock(Mutex::mark);
		if (chkTimeout.count() > 0 && context.IsCancelable()) {
			do {
				ThrowOnCancel(context, "Write lock (smart_lock) was canceled or timed out (mutex)"sv);
			} while (!mtx_->try_lock_for(chkTimeout));
		} else {
			mtx_->lock();
		}
		locked_ = true;
	}
	template <typename Context>
	smart_lock(Mutex& mtx, Context& context, NonUniqueT, milliseconds chkTimeout = kDefaultCondChkTime) RX_ACQUIRE_SHARED(mtx)
		: mtx_(&mtx), unique_(false), locked_(false) {
		using namespace std::string_view_literals;
		const auto lockWard = context.BeforeLock(Mutex::mark);
		if (chkTimeout.count() > 0 && context.IsCancelable()) {
			do {
				ThrowOnCancel(context, "Read lock (smart_lock) was canceled or timed out (mutex)"sv);
			} while (!mtx_->try_lock_shared_for(chkTimeout));
		} else {
			mtx_->lock_shared();
		}
		locked_ = true;
	}
	smart_lock(const smart_lock&) = delete;
	smart_lock& operator=(const smart_lock&) = delete;

	smart_lock(smart_lock&& other) noexcept {
		mtx_ = other.mtx_;
		unique_ = other.unique_;
		locked_ = other.locked_;
		other.mtx_ = nullptr;
	}
	smart_lock& operator=(smart_lock&& other) noexcept {
		if (this != &other) {
			unlock();
			mtx_ = other.mtx_;
			unique_ = other.unique_;
			locked_ = other.locked_;
			other.mtx_ = nullptr;
		}
		return *this;
	}
	~smart_lock() RX_RELEASE() { unlock(); }

	void unlock() noexcept RX_RELEASE() {
		if (mtx_ && locked_) {
			if (unique_) {
				mtx_->unlock();
			} else {
				mtx_->unlock_shared();
			}
		}
		locked_ = false;
	}

private:
	Mutex* mtx_;
	bool unique_;
	bool locked_;
};

}  // namespace reindexer
