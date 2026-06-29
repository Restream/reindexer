#pragma once

#include <mutex>
#include "mutex_details.h"
#include "thread_annotation_attributes.h"

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

namespace reindexer {

template <typename Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY lock_guard
	: private std::conditional_t<concepts::EstlMutex<Mtx>, lock_guard<details::BaseMutex<Mtx>>, std::lock_guard<Mtx>> {
	using Base = std::conditional_t<concepts::EstlMutex<Mtx>, lock_guard<details::BaseMutex<Mtx>>, std::lock_guard<Mtx>>;

public:
	explicit lock_guard(Mtx& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	~lock_guard() RX_RELEASE() = default;
};

template <typename Mtx>
lock_guard(Mtx&) -> lock_guard<Mtx>;

template <typename Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY unique_lock
	: private std::conditional_t<concepts::EstlMutex<Mtx>, unique_lock<details::BaseMutex<Mtx>>, std::unique_lock<Mtx>> {
	using Base = std::conditional_t<concepts::EstlMutex<Mtx>, unique_lock<details::BaseMutex<Mtx>>, std::unique_lock<Mtx>>;

	friend class condition_variable;

public:
	explicit unique_lock(Mtx& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	explicit unique_lock(Mtx& mtx, std::try_to_lock_t) RX_ACQUIRE(mtx) : Base{mtx, std::try_to_lock} {}
	explicit unique_lock(Mtx& mtx, std::defer_lock_t) RX_EXCLUDES(mtx) : Base{mtx, std::defer_lock} {}
	unique_lock(unique_lock&& other) noexcept : Base{std::move(other)} {}
	~unique_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
	using Base::owns_lock;
	using Base::operator bool;
	RX_ALWAYS_INLINE Mtx* mutex() const noexcept { return static_cast<Mtx*>(Base::mutex()); }
};

template <typename Mtx>
unique_lock(Mtx&) -> unique_lock<Mtx>;
template <typename Mtx>
unique_lock(Mtx&, std::try_to_lock_t) -> unique_lock<Mtx>;

template <typename... Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY scoped_lock;

template <typename... Mtx>
scoped_lock(Mtx&...) -> scoped_lock<Mtx...>;

template <typename... Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY scoped_lock : private std::scoped_lock<details::BaseMutex<Mtx>...> {
	using Base = std::scoped_lock<details::BaseMutex<Mtx>...>;

public:
	template <typename Mtx1, typename Mtx2>
	explicit scoped_lock(Mtx1& mtx1, Mtx2& mtx2) RX_ACQUIRE(mtx1, mtx2) : Base{mtx1, mtx2} {
		static_assert(std::is_same_v<scoped_lock<Mtx...>, scoped_lock<Mtx1, Mtx2>>);
	}
	~scoped_lock() RX_RELEASE() = default;
};

}  // namespace reindexer

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

namespace reindexer {

using std::lock_guard;
using std::unique_lock;
using std::scoped_lock;

}  // namespace reindexer

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE
