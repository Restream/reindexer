#pragma once

#include <mutex>
#include "estl/marked_mutex.h"
#include "tagged_mutex.h"
#include "thread_annotation_attributes.h"

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

#include "estl/concepts.h"

namespace reindexer {

class condition_variable;

template <typename Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY unique_lock : private std::unique_lock<Mtx> {
	using Base = std::unique_lock<Mtx>;
	friend condition_variable;

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

class mutex;
class shared_mutex;
class shared_timed_mutex;
class recursive_mutex;

template <concepts::OneOf<mutex, shared_mutex, shared_timed_mutex, recursive_mutex> Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY unique_lock<Mtx> : private std::unique_lock<typename Mtx::Base> {
	using Base = std::unique_lock<typename Mtx::Base>;
	friend condition_variable;

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

template <typename Mtx, auto tag>
class [[nodiscard]] RX_SCOPED_CAPABILITY unique_lock<TaggedMutex<Mtx, tag>> : private unique_lock<typename TaggedMutex<Mtx, tag>::Base> {
	using Mutex = TaggedMutex<Mtx, tag>;
	using Base = unique_lock<typename Mutex::Base>;
	friend condition_variable;

public:
	explicit unique_lock(Mutex& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	explicit unique_lock(Mutex& mtx, std::try_to_lock_t) RX_ACQUIRE(mtx) : Base{mtx, std::try_to_lock} {}
	explicit unique_lock(Mutex& mtx, std::defer_lock_t) RX_EXCLUDES(mtx) : Base{mtx, std::defer_lock} {}
	unique_lock(unique_lock&& other) noexcept : Base{std::move(other)} {}
	~unique_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
	using Base::owns_lock;
	using Base::operator bool;
	RX_ALWAYS_INLINE Mutex* mutex() const noexcept { return static_cast<Mutex*>(Base::mutex()); }
};

template <typename Mtx, MutexMark m>
class [[nodiscard]] RX_SCOPED_CAPABILITY unique_lock<MarkedMutex<Mtx, m>> : private unique_lock<typename MarkedMutex<Mtx, m>::Base> {
	using Mutex = MarkedMutex<Mtx, m>;
	using Base = unique_lock<typename Mutex::Base>;
	friend condition_variable;

public:
	explicit unique_lock(Mutex& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	explicit unique_lock(Mutex& mtx, std::try_to_lock_t) RX_ACQUIRE(mtx) : Base{mtx, std::try_to_lock} {}
	explicit unique_lock(Mutex& mtx, std::defer_lock_t) RX_EXCLUDES(mtx) : Base{mtx, std::defer_lock} {}
	unique_lock(unique_lock&& other) noexcept : Base{std::move(other)} {}
	~unique_lock() RX_RELEASE() = default;
	RX_ALWAYS_INLINE void lock() RX_ACQUIRE() { Base::lock(); }
	RX_ALWAYS_INLINE void unlock() RX_RELEASE() { Base::unlock(); }
	using Base::owns_lock;
	using Base::operator bool;
	RX_ALWAYS_INLINE Mutex* mutex() const noexcept { return static_cast<Mutex*>(Base::mutex()); }
};

}  // namespace reindexer

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

namespace reindexer {

using std::unique_lock;

}  // namespace reindexer

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE
