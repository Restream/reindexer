#pragma once

#ifdef _MSC_VER
#define REINDEX_USE_STD_TIMED_MUTEX 1
#elif __cplusplus >= 201402
// refuse to use std::timed_mutex - it's much slower, than pthread_rwlock implementaion on systems without VDSO
// disable
#define REINDEX_USE_STD_TIMED_MUTEX 0
#else
#define REINDEX_USE_STD_TIMED_MUTEX 0
#endif

#ifdef __APPLE__
#define PTHREAD_TIMED_LOCK_AVAILABLE 0
#else  // __APPLE__
#define PTHREAD_TIMED_LOCK_AVAILABLE 1
#endif	// __APPLE__

#include <chrono>
#include "thread_annotation_attributes.h"

#if REINDEX_USE_STD_TIMED_MUTEX
#include <mutex>

namespace reindexer {

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <typename>
class lock_guard;
template <typename>
class unique_lock;
template <typename>
class shared_lock;
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

class [[nodiscard]] RX_CAPABILITY("mutex") timed_mutex : private std::timed_mutex {
	using Base = std::timed_mutex;

	friend shared_lock<timed_mutex>;
	friend lock_guard<timed_mutex>;
	friend unique_lock<timed_mutex>;
	friend details::BaseMutexImpl<timed_mutex>;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;
	template <typename...>
	friend class scoped_lock;

public:
	const timed_mutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;
};

#else	// RX_THREAD_SAFETY_ANALYSIS_ENABLE
using std::timed_mutex;
#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

}  // namespace reindexer

#else
#include <errno.h>
#include <pthread.h>
#include "tools/assertrx.h"
#include "tools/clock.h"

namespace reindexer {

class [[nodiscard]] __timed_mutex_pthread {
#ifdef PTHREAD_MUTEX_INITIALIZER
	pthread_mutex_t _M_lock = PTHREAD_MUTEX_INITIALIZER;

public:
	__timed_mutex_pthread() = default;
	~__timed_mutex_pthread() = default;
#else
	pthread_mutex_t _M_lock;

public:
	__timed_mutex_pthread() {
		int __ret = pthread_mutex_init(&_M_lock, NULL);
		assertrx(__ret == 0);
	}

	~__shared_mutex_pthread() {
		int __ret __attribute((__unused__)) = pthread_mutex_destroy(&_M_lock);
		assertrx(__ret == 0);
	}
#endif

	__timed_mutex_pthread(const __timed_mutex_pthread&) = delete;
	__timed_mutex_pthread& operator=(const __timed_mutex_pthread&) = delete;

	void lock() noexcept {
		int __ret = pthread_mutex_lock(&_M_lock);
		assertrx(__ret == 0);
		(void)__ret;
	}

	bool try_lock() noexcept {
		int __ret = pthread_mutex_trylock(&_M_lock);
		if (__ret == EBUSY) {
			return false;
		}
		assertrx(__ret == 0);
		return true;
	}

	void unlock() noexcept {
		int __ret = pthread_mutex_unlock(&_M_lock);
		assertrx(__ret == 0);
		(void)__ret;
	}

	void* native_handle() noexcept { return &_M_lock; }
};

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

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

class [[nodiscard]] RX_CAPABILITY("mutex") timed_mutex : RX_MUTEX_ACCESS __timed_mutex_pthread {

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

	friend shared_lock<timed_mutex>;
	friend lock_guard<timed_mutex>;
	friend unique_lock<timed_mutex>;
	friend details::BaseMutexImpl<timed_mutex>;
	template <typename>
	friend class smart_lock;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;

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
			__ret = pthread_mutex_timedlock(static_cast<pthread_mutex_t*>(native_handle()), &__ts);
		} while (__ret == EAGAIN || __ret == EBUSY);
		if (ETIMEDOUT == __ret || EDEADLK == __ret) {
			return false;
		}
#else	// PTHREAD_TIMED_LOCK_AVAILABLE
		(void)absTime;
		__ret = pthread_mutex_lock(static_cast<pthread_mutex_t*>(native_handle()));
#endif	// PTHREAD_TIMED_LOCK_AVAILABLE
		assertrx(__ret == 0);
		return true;
	}

private:
	using __clock = system_clock_w;
};

}  // namespace reindexer

#endif
