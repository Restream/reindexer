#pragma once

#include "contexted_locks.h"

using std::chrono::milliseconds;

namespace reindexer {

template <typename Mutex>
class smart_lock {
public:
	smart_lock() : mtx_(nullptr), unique_(false), locked_(false) {}

	smart_lock(Mutex& mtx, bool unique = false) : mtx_(&mtx), unique_(unique), locked_(true) {
		if (unique_)
			mtx_->lock();
		else
			mtx_->lock_shared();
	}
	template <typename Context>
	smart_lock(Mutex& mtx, Context& context, bool unique = false, milliseconds chkTimeout = kDefaultCondChkTime)
		: mtx_(&mtx), unique_(unique), locked_(false) {
		const auto lockWard = context.BeforeLock(Mutex::mark);
		if (chkTimeout.count() > 0 && context.isCancelable()) {
			do {
				ThrowOnCancel(context, "Lock was canceled on condition"_sv);
			} while (unique_ ? (!mtx_->try_lock_for(chkTimeout)) : (!mtx_->try_lock_shared_for(chkTimeout)));
		} else {
			if (unique_) {
				mtx_->lock();
			} else {
				mtx_->lock_shared();
			}
		}
		locked_ = true;
	}
	smart_lock(const smart_lock&) = delete;
	smart_lock& operator=(const smart_lock&) = delete;

	smart_lock(smart_lock&& other) {
		mtx_ = other.mtx_;
		unique_ = other.unique_;
		locked_ = other.locked_;
		other.mtx_ = nullptr;
	}
	smart_lock& operator=(smart_lock&& other) {
		if (this != &other) {
			unlock();
			mtx_ = other.mtx_;
			unique_ = other.unique_;
			locked_ = other.locked_;
			other.mtx_ = nullptr;
		}
		return *this;
	}
	~smart_lock() { unlock(); }

	void unlock() {
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
