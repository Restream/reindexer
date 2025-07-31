#pragma once

#include <mutex>
#include "thread_annotation_attributes.h"

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

#include "estl/concepts.h"

namespace reindexer {

template <typename... Mtx>
class [[nodiscard]] RX_SCOPED_CAPABILITY scoped_lock;

template <typename... Mtx>
scoped_lock(Mtx&...) -> scoped_lock<Mtx...>;

template <typename Mtx1, typename Mtx2>
class [[nodiscard]] RX_SCOPED_CAPABILITY scoped_lock<Mtx1, Mtx2> : private std::scoped_lock<Mtx1, Mtx2> {
	using Base = std::scoped_lock<Mtx1, Mtx2>;

public:
	explicit scoped_lock(Mtx1& mtx1, Mtx2& mtx2) RX_ACQUIRE(mtx1, mtx2) : Base{mtx1, mtx2} {}
	~scoped_lock() RX_RELEASE() = default;
};

class mutex;
class shared_mutex;
class shared_timed_mutex;

template <concepts::OneOf<mutex, shared_mutex, shared_timed_mutex> Mtx1, concepts::OneOf<mutex, shared_mutex, shared_timed_mutex> Mtx2>
class [[nodiscard]] RX_SCOPED_CAPABILITY scoped_lock<Mtx1, Mtx2> : private std::scoped_lock<typename Mtx1::Base, typename Mtx2::Base> {
	using Base = std::scoped_lock<typename Mtx1::Base, typename Mtx2::Base>;

public:
	explicit scoped_lock(Mtx1& mtx1, Mtx2& mtx2) RX_ACQUIRE(mtx1, mtx2) : Base{mtx1, mtx2} {}
	~scoped_lock() RX_RELEASE() = default;
};

}  // namespace reindexer

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

namespace reindexer {
using std::scoped_lock;
}  // namespace reindexer

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE
