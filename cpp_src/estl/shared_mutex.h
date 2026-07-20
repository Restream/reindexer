#pragma once

#ifdef _MSC_VER
#define REINDEX_USE_STD_SHARED_MUTEX 1
#elif __cplusplus >= 201402
// refuse to use std::shared_timed_mutex - it's much slower, than pthread_rwlock implementaion on systems without VDSO
// disable
#define REINDEX_USE_STD_SHARED_MUTEX 0
#else
#define REINDEX_USE_STD_SHARED_MUTEX 0
#endif

#ifdef __APPLE__
#define PTHREAD_TIMED_LOCK_AVAILABLE 0
#else  // __APPLE__
#define PTHREAD_TIMED_LOCK_AVAILABLE 1
#endif	// __APPLE__

#include <chrono>
#include "thread_annotation_attributes.h"

#include <shared_mutex>
#include "estl/mutex_details.h"

namespace reindexer {

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <typename>
class lock_guard;
template <typename>
class unique_lock;
template <typename>
class shared_lock;
template <typename>
class smart_lock;
template <typename, typename>
class contexted_unique_lock;
template <typename, typename>
class contexted_shared_lock;
template <typename...>
class scoped_lock;

namespace details {
template <typename>
struct BaseMutexImpl;
}  // namespace details

class [[nodiscard]] RX_CAPABILITY("mutex") shared_mutex : private std::shared_mutex {
	using Base = std::shared_mutex;

	friend shared_lock<shared_mutex>;
	friend lock_guard<shared_mutex>;
	friend unique_lock<shared_mutex>;
	friend details::BaseMutexImpl<shared_mutex>;
	template <typename>
	friend class smart_lock;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;
	template <typename...>
	friend class scoped_lock;

public:
	const shared_mutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;
};

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

using std::shared_mutex;

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

}  // namespace reindexer

#if REINDEX_USE_STD_SHARED_MUTEX

namespace reindexer {

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

class [[nodiscard]] RX_CAPABILITY("mutex") shared_timed_mutex : private std::shared_timed_mutex {
	using Base = std::shared_timed_mutex;

	friend shared_lock<shared_timed_mutex>;
	friend lock_guard<shared_timed_mutex>;
	friend unique_lock<shared_timed_mutex>;
	friend details::BaseMutexImpl<shared_timed_mutex>;
	template <typename>
	friend class smart_lock;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;

public:
	const shared_timed_mutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;
};

template <typename Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY shared_lock
	: private std::conditional_t<concepts::EstlMutex<Mtx>, shared_lock<details::BaseMutex<Mtx>>, std::shared_lock<Mtx>> {
	using Base = std::conditional_t<concepts::EstlMutex<Mtx>, shared_lock<details::BaseMutex<Mtx>>, std::shared_lock<Mtx>>;

public:
	explicit shared_lock(Mtx& mtx) RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
	explicit shared_lock(Mtx& mtx, std::defer_lock_t) RX_EXCLUDES(mtx) : Base{mtx, std::defer_lock} {}
	shared_lock(shared_lock&& other) noexcept : Base{std::move(other)} {}
	~shared_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE_SHARED() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
};

template <typename Mtx>
shared_lock(Mtx&) -> shared_lock<Mtx>;

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

using std::shared_timed_mutex;
using std::shared_lock;

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

}  // namespace reindexer

#else
#include <errno.h>
#include <pthread.h>
#include <mutex>
#include "tools/assertrx.h"
#include "tools/clock.h"

namespace reindexer {

class [[nodiscard]] RX_CAPABILITY("mutex") __shared_mutex_pthread {
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
		assertrx(__ret == 0);
	}

	~__shared_mutex_pthread() {
		int __ret __attribute((__unused__)) = pthread_rwlock_destroy(&_M_rwlock);
		assertrx(__ret == 0);
	}
#endif

	__shared_mutex_pthread(const __shared_mutex_pthread&) = delete;
	__shared_mutex_pthread& operator=(const __shared_mutex_pthread&) = delete;

	void lock() noexcept {
		int __ret = pthread_rwlock_wrlock(&_M_rwlock);
		assertrx(__ret == 0);
		(void)__ret;
	}

	bool try_lock() noexcept {
		int __ret = pthread_rwlock_trywrlock(&_M_rwlock);
		if (__ret == EBUSY) {
			return false;
		}
		assertrx(__ret == 0);
		return true;
	}

	void unlock() noexcept {
		int __ret = pthread_rwlock_unlock(&_M_rwlock);
		assertrx(__ret == 0);
		(void)__ret;
	}

	void lock_shared() noexcept {
		int __ret;
		do {
			__ret = pthread_rwlock_rdlock(&_M_rwlock);
		} while (__ret == EAGAIN || __ret == EBUSY);
		assertrx(__ret == 0);
	}

	bool try_lock_shared() noexcept {
		int __ret = pthread_rwlock_tryrdlock(&_M_rwlock);
		if (__ret == EBUSY || __ret == EAGAIN) {
			return false;
		}
		assertrx(__ret == 0);
		return true;
	}

	void unlock_shared() noexcept { unlock(); }

	void* native_handle() noexcept { return &_M_rwlock; }
};

class [[nodiscard]] RX_CAPABILITY("mutex") shared_timed_mutex : RX_MUTEX_ACCESS __shared_mutex_pthread {
	using Base = __shared_mutex_pthread;

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

	friend shared_lock<shared_timed_mutex>;
	friend lock_guard<shared_timed_mutex>;
	friend unique_lock<shared_timed_mutex>;
	friend details::BaseMutexImpl<shared_timed_mutex>;
	template <typename>
	friend class smart_lock;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;
	template <typename...>
	friend class scoped_lock;

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

	RX_MUTEX_ACCESS:
	template <class Rep, class Period>
	bool try_lock_for(const std::chrono::duration<Rep, Period>& duration) {
		return try_lock_until(__clock::now_coarse() + duration);
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
#else	// PTHREAD_TIMED_LOCK_AVAILABLE
		(void)absTime;
		__ret = pthread_rwlock_wrlock(static_cast<pthread_rwlock_t*>(native_handle()));
#endif	// PTHREAD_TIMED_LOCK_AVAILABLE
		assertrx(__ret == 0);
		return true;
	}

	template <class Rep, class Period>
	bool try_lock_shared_for(const std::chrono::duration<Rep, Period>& duration) {
		return try_lock_shared_until(__clock::now_coarse() + duration);
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
#else	// PTHREAD_TIMED_LOCK_AVAILABLE
		(void)absTime;
		__ret = pthread_rwlock_rdlock(static_cast<pthread_rwlock_t*>(native_handle()));
#endif	// PTHREAD_TIMED_LOCK_AVAILABLE
		assertrx(__ret == 0);
		return true;
	}

public:
	const shared_timed_mutex& operator!() const& { return *this; }
	auto operator!() const&& = delete;

private:
	using __clock = system_clock_w;
};

template <typename _Mutex>
class [[nodiscard]] RX_SCOPED_CAPABILITY shared_lock {
public:
	typedef _Mutex mutex_type;

	shared_lock() noexcept : _M_pm(nullptr), _M_owns(false) {}

	explicit shared_lock(mutex_type& __m) noexcept RX_ACQUIRE_SHARED(__m) : _M_pm(&__m), _M_owns(true) { __m.lock_shared(); }
	explicit shared_lock(mutex_type& __m, std::defer_lock_t) noexcept RX_EXCLUDES(__m) : _M_pm(&__m), _M_owns(false) {}

	~shared_lock() RX_RELEASE() {
		if (_M_owns) {
			_M_pm->unlock_shared();
		}
	}

	shared_lock(const shared_lock&) = delete;
	shared_lock& operator=(const shared_lock&) = delete;

	shared_lock(shared_lock&& __sl) noexcept : shared_lock() { swap(__sl); }

	shared_lock& operator=(shared_lock&& __sl) noexcept {
		shared_lock(std::move(__sl)).swap(*this);
		return *this;
	}

	void lock() noexcept RX_ACQUIRE_SHARED() {
		_M_lockable();
		_M_pm->lock_shared();
		_M_owns = true;
	}

	bool try_lock() noexcept RX_TRY_ACQUIRE_SHARED(true) {
		_M_lockable();
		return _M_owns = _M_pm->try_lock_shared();
	}

	void unlock() noexcept RX_RELEASE() {
		assertrx(_M_owns);
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
	void _M_lockable() const noexcept {
		assertrx(_M_pm != nullptr);
		assertrx(!_M_owns);
	}

	mutex_type* _M_pm;
	bool _M_owns;
};

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <concepts::EstlMutex _Mutex>
	requires(!std::same_as<_Mutex, shared_timed_mutex>)
class [[nodiscard]] RX_SCOPED_CAPABILITY shared_lock<_Mutex> : private shared_lock<details::BaseMutex<_Mutex>> {
	using Base = shared_lock<details::BaseMutex<_Mutex>>;

public:
	explicit shared_lock(_Mutex& mtx) RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
	explicit shared_lock(_Mutex& mtx, std::defer_lock_t) RX_EXCLUDES(mtx) : Base{mtx, std::defer_lock} {}
	shared_lock(shared_lock&& other) noexcept : Base{std::move(other)} {}
	~shared_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE_SHARED() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
	using Base::swap;
};

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

/// Swap specialization for shared_lock
template <typename _Mutex>
void swap(shared_lock<_Mutex>& __x, shared_lock<_Mutex>& __y) noexcept {
	__x.swap(__y);
}

}  // namespace reindexer

#endif
