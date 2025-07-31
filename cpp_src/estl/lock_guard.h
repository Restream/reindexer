#pragma once

#include <mutex>
#include "marked_mutex.h"
#include "tagged_mutex.h"
#include "thread_annotation_attributes.h"

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

#include "estl/concepts.h"

namespace reindexer {

template <typename Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY lock_guard : private std::lock_guard<Mtx> {
	using Base = std::lock_guard<Mtx>;

public:
	explicit lock_guard(Mtx& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	~lock_guard() RX_RELEASE() = default;
};

template <typename Mtx>
lock_guard(Mtx&) -> lock_guard<Mtx>;

class mutex;
class shared_mutex;
class shared_timed_mutex;
class recursive_mutex;

template <concepts::OneOf<mutex, shared_mutex, shared_timed_mutex, recursive_mutex> Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY lock_guard<Mtx> : private std::lock_guard<typename Mtx::Base> {
	using Base = std::lock_guard<typename Mtx::Base>;

public:
	explicit lock_guard(Mtx& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	~lock_guard() RX_RELEASE() = default;
};

template <typename Mtx, auto tag>
class [[nodiscard]] RX_SCOPED_CAPABILITY lock_guard<TaggedMutex<Mtx, tag>> : private lock_guard<typename TaggedMutex<Mtx, tag>::Base> {
	using Mutex = TaggedMutex<Mtx, tag>;
	using Base = lock_guard<typename Mutex::Base>;

public:
	explicit lock_guard(Mutex& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	~lock_guard() RX_RELEASE() = default;
};

template <typename Mtx, MutexMark m>
class [[nodiscard]] RX_SCOPED_CAPABILITY lock_guard<MarkedMutex<Mtx, m>> : private lock_guard<typename MarkedMutex<Mtx, m>::Base> {
	using Mutex = MarkedMutex<Mtx, m>;
	using Base = lock_guard<typename Mutex::Base>;

public:
	explicit lock_guard(Mutex& mtx) RX_ACQUIRE(mtx) : Base{mtx} {}
	~lock_guard() RX_RELEASE() = default;
};

}  // namespace reindexer

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

namespace reindexer {
using std::lock_guard;
}  // namespace reindexer

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE
