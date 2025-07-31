#pragma once

#include <shared_mutex>
#include "marked_mutex.h"
#include "tagged_mutex.h"
#include "thread_annotation_attributes.h"

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

#include "estl/concepts.h"

namespace reindexer {

template <typename Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY shared_lock : private std::shared_lock<Mtx> {
	using Base = std::shared_lock<Mtx>;

public:
	explicit shared_lock(Mtx& mtx) RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
	explicit shared_lock(Mtx& mtx, std::defer_lock_t) RX_REQUIRES(!mtx) : Base{mtx, std::defer_lock} {}
	shared_lock(shared_lock&& other) noexcept : Base{std::move(other)} {}
	~shared_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE_SHARED() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
};

template <typename Mtx>
shared_lock(Mtx&) -> shared_lock<Mtx>;

class shared_mutex;
class shared_timed_mutex;

template <concepts::OneOf<shared_mutex, shared_timed_mutex> Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY shared_lock<Mtx> : private std::shared_lock<typename Mtx::Base> {
	using Base = std::shared_lock<typename Mtx::Base>;

public:
	explicit shared_lock(Mtx& mtx) RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
	explicit shared_lock(Mtx& mtx, std::defer_lock_t) RX_REQUIRES(!mtx) : Base{mtx, std::defer_lock} {}
	shared_lock(shared_lock&& other) noexcept : Base{std::move(other)} {}
	~shared_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE_SHARED() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
};

template <typename Mtx, auto tag>
class [[nodiscard]] RX_SCOPED_CAPABILITY shared_lock<TaggedMutex<Mtx, tag>> : private shared_lock<typename TaggedMutex<Mtx, tag>::Base> {
	using Mutex = TaggedMutex<Mtx, tag>;
	using Base = shared_lock<typename Mutex::Base>;

public:
	explicit shared_lock(Mutex& mtx) RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
	explicit shared_lock(Mutex& mtx, std::defer_lock_t) RX_REQUIRES(!mtx) : Base{mtx, std::defer_lock} {}
	shared_lock(shared_lock&& other) noexcept : Base{std::move(other)} {}
	~shared_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE_SHARED() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
};

template <typename Mtx, MutexMark m>
class [[nodiscard]] RX_SCOPED_CAPABILITY shared_lock<MarkedMutex<Mtx, m>> : private shared_lock<typename MarkedMutex<Mtx, m>::Base> {
	using Mutex = MarkedMutex<Mtx, m>;
	using Base = shared_lock<typename Mutex::Base>;

public:
	explicit shared_lock(Mutex& mtx) RX_ACQUIRE_SHARED(mtx) : Base{mtx} {}
	explicit shared_lock(Mutex& mtx, std::defer_lock_t) RX_REQUIRES(!mtx) : Base{mtx, std::defer_lock} {}
	shared_lock(shared_lock&& other) noexcept : Base{std::move(other)} {}
	~shared_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE_SHARED() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
};

}  // namespace reindexer

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

namespace reindexer {
using std::shared_lock;
}  // namespace reindexer

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE
