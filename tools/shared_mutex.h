
#pragma once

#if __cplusplus >= 201402
#define REINDEX_USE_STD_SHARED_MUTEX 1
#else
#define REINDEX_USE_STD_SHARED_MUTEX 0
#endif

#if REINDEX_USE_STD_SHARED_MUTEX
#include <shared_mutex>
using std::shared_timed_mutex;
#else

namespace reindexer {

#include <assert.h>
#include <errno.h>
#include <pthread.h>

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
		while (__ret == EAGAIN);
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
	shared_lock(mutex_type& __m, defer_lock_t) noexcept : _M_pm(&__m), _M_owns(false) {}
	shared_lock(mutex_type& __m, try_to_lock_t) : _M_pm(&__m), _M_owns(__m.try_lock_shared()) {}
	shared_lock(mutex_type& __m, adopt_lock_t) : _M_pm(&__m), _M_owns(true) {}

	template <typename _Clock, typename _Duration>
	shared_lock(mutex_type& __m, const chrono::time_point<_Clock, _Duration>& __abs_time)
		: _M_pm(&__m), _M_owns(__m.try_lock_shared_until(__abs_time)) {}

	template <typename _Rep, typename _Period>
	shared_lock(mutex_type& __m, const chrono::duration<_Rep, _Period>& __rel_time)
		: _M_pm(&__m), _M_owns(__m.try_lock_shared_for(__rel_time)) {}

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

	template <typename _Rep, typename _Period>
	bool try_lock_for(const chrono::duration<_Rep, _Period>& __rel_time) {
		_M_lockable();
		return _M_owns = _M_pm->try_lock_shared_for(__rel_time);
	}

	template <typename _Clock, typename _Duration>
	bool try_lock_until(const chrono::time_point<_Clock, _Duration>& __abs_time) {
		_M_lockable();
		return _M_owns = _M_pm->try_lock_shared_until(__abs_time);
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

class shared_timed_mutex : public __shared_mutex_pthread {};
}

#endif
