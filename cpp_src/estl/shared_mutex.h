
#pragma once

#ifdef _MSC_VER
#define REINDEX_USE_STD_SHARED_MUTEX 1
#elif __cplusplus >= 201402
// refuse to use std::shared_timed_mutex - it's much slower, than pthread_rwlock implementaion
// disable
#define REINDEX_USE_STD_SHARED_MUTEX 0
#else
#define REINDEX_USE_STD_SHARED_MUTEX 0
#endif

#ifdef __APPLE__
#define PTHREAD_TIMED_LOCK_AVAILABLE 0
#else  // __APPLE__
#define PTHREAD_TIMED_LOCK_AVAILABLE 1
#endif  // __APPLE__

#include <chrono>

#if REINDEX_USE_STD_SHARED_MUTEX
#include <shared_mutex>
using std::shared_timed_mutex;
using std::shared_lock;
#else
#include <assert.h>
#include <errno.h>
#include <pthread.h>

namespace reindexer {

class __shared_mutex_pthread {
#ifdef PTHREAD_RWLOCK_INITIALIZER
	pthread_rwlock_t _M_rwlock = PTHREAD_RWLOCK_INITIALIZER;

public:
	__shared_mutex_pthread() = default;
	~__shared_mutex_pthread() = default;
#else
	pthread_rwlock_t _M_rwlock;

public:
	__shared_mutex_pthread() {
		int __ret = pthread_rwlock_init(&_M_rwlock, NULL);
		assert(__ret == 0);
	}

	~__shared_mutex_pthread() {
		int __ret __attribute((__unused__)) = pthread_rwlock_destroy(&_M_rwlock);
		assert(__ret == 0);
	}
#endif

	__shared_mutex_pthread(const __shared_mutex_pthread&) = delete;
	__shared_mutex_pthread& operator=(const __shared_mutex_pthread&) = delete;

	void lock() {
		int __ret = pthread_rwlock_wrlock(&_M_rwlock);
		assert(__ret == 0);
		(void)__ret;
	}

	bool try_lock() {
		int __ret = pthread_rwlock_trywrlock(&_M_rwlock);
		if (__ret == EBUSY) return false;
		assert(__ret == 0);
		return true;
	}

	void unlock() {
		int __ret = pthread_rwlock_unlock(&_M_rwlock);
		assert(__ret == 0);
		(void)__ret;
	}

	void lock_shared() {
		int __ret;
		do
			__ret = pthread_rwlock_rdlock(&_M_rwlock);
		while (__ret == EAGAIN || __ret == EBUSY);
		assert(__ret == 0);
	}

	bool try_lock_shared() {
		int __ret = pthread_rwlock_tryrdlock(&_M_rwlock);
		if (__ret == EBUSY || __ret == EAGAIN) return false;
		assert(__ret == 0);
		return true;
	}

	void unlock_shared() { unlock(); }

	void* native_handle() { return &_M_rwlock; }
};

template <typename _Mutex>
class shared_lock {
public:
	typedef _Mutex mutex_type;

	shared_lock() noexcept : _M_pm(nullptr), _M_owns(false) {}

	explicit shared_lock(mutex_type& __m) : _M_pm(&__m), _M_owns(true) { __m.lock_shared(); }

	~shared_lock() {
		if (_M_owns) _M_pm->unlock_shared();
	}

	shared_lock(shared_lock const&) = delete;
	shared_lock& operator=(shared_lock const&) = delete;

	shared_lock(shared_lock&& __sl) noexcept : shared_lock() { swap(__sl); }

	shared_lock& operator=(shared_lock&& __sl) noexcept {
		shared_lock(std::move(__sl)).swap(*this);
		return *this;
	}

	void lock() {
		_M_lockable();
		_M_pm->lock_shared();
		_M_owns = true;
	}

	bool try_lock() {
		_M_lockable();
		return _M_owns = _M_pm->try_lock_shared();
	}

	void unlock() {
		if (!_M_owns) assert(0);
		_M_pm->unlock_shared();
		_M_owns = false;
	}

	void swap(shared_lock& __u) noexcept {
		std::swap(_M_pm, __u._M_pm);
		std::swap(_M_owns, __u._M_owns);
	}

	mutex_type* release() noexcept {
		_M_owns = false;
		auto ret = _M_pm;
		_M_pm = nullptr;
		return ret;
	}

	bool owns_lock() const noexcept { return _M_owns; }
	explicit operator bool() const noexcept { return _M_owns; }
	mutex_type* mutex() const noexcept { return _M_pm; }

private:
	void _M_lockable() const {
		if (_M_pm == nullptr) assert(0);
		if (_M_owns) assert(0);
	}

	mutex_type* _M_pm;
	bool _M_owns;
};

/// Swap specialization for shared_lock
template <typename _Mutex>
void swap(shared_lock<_Mutex>& __x, shared_lock<_Mutex>& __y) noexcept {
	__x.swap(__y);
}

class shared_timed_mutex : public __shared_mutex_pthread {
public:
	template <class Rep, class Period>
	bool try_lock_for(const std::chrono::duration<Rep, Period>& duration) {
		return try_lock_until(__clock::now() + duration);
	}

	template <class Rep, class Period>
	bool try_lock_until(const std::chrono::time_point<Rep, Period>& absTime) {
		int __ret;
#if PTHREAD_TIMED_LOCK_AVAILABLE
		auto __sec = std::chrono::time_point_cast<std::chrono::seconds>(absTime);
		auto __nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(absTime - __sec);
		struct timespec __ts = {static_cast<time_t>(__sec.time_since_epoch().count()), static_cast<long>(__nsec.count())};

		do {
			__ret = pthread_rwlock_timedwrlock(static_cast<pthread_rwlock_t*>(native_handle()), &__ts);
		} while (__ret == EAGAIN || __ret == EBUSY);
		if (ETIMEDOUT == __ret || EDEADLK == __ret) {
			return false;
		}
#else   // PTHREAD_TIMED_LOCK_AVAILABLE
		(void)absTime;
		__ret = pthread_rwlock_wrlock(static_cast<pthread_rwlock_t*>(native_handle()));
#endif  // PTHREAD_TIMED_LOCK_AVAILABLE
		assert(__ret == 0);
		return true;
	}

	template <class Rep, class Period>
	bool try_lock_shared_for(const std::chrono::duration<Rep, Period>& duration) {
		return try_lock_shared_until(__clock::now() + duration);
	}

	template <class Clock, class Duration>
	bool try_lock_shared_until(const std::chrono::time_point<Clock, Duration>& absTime) {
		int __ret;
#if PTHREAD_TIMED_LOCK_AVAILABLE
		auto __sec = std::chrono::time_point_cast<std::chrono::seconds>(absTime);
		auto __nsec = std::chrono::duration_cast<std::chrono::nanoseconds>(absTime - __sec);
		struct timespec __ts = {static_cast<time_t>(__sec.time_since_epoch().count()), static_cast<long>(__nsec.count())};

		do {
			__ret = pthread_rwlock_timedrdlock(static_cast<pthread_rwlock_t*>(native_handle()), &__ts);
		} while (__ret == EAGAIN || __ret == EBUSY);
		if (ETIMEDOUT == __ret || EDEADLK == __ret) {
			return false;
		}
#else   // PTHREAD_TIMED_LOCK_AVAILABLE
		(void)absTime;
		__ret = pthread_rwlock_rdlock(static_cast<pthread_rwlock_t*>(native_handle()));
#endif  // PTHREAD_TIMED_LOCK_AVAILABLE
		assert(__ret == 0);
		return true;
	}

private:
	using __clock = std::chrono::system_clock;
};
}  // namespace reindexer

#endif
