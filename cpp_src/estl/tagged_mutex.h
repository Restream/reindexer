#pragma once

#include "estl/thread_annotation_attributes.h"

namespace reindexer {

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <typename>
class unique_lock;
template <typename>
class shared_lock;
template <typename>
class lock_guard;

namespace details {
template <typename>
struct BaseMutexImpl;
}  // namespace details

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <typename Mtx, auto>
class [[nodiscard]] RX_CAPABILITY("mutex") TaggedMutex : RX_MUTEX_ACCESS Mtx {
	using Base = Mtx;

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE
	friend unique_lock<TaggedMutex>;
	friend shared_lock<TaggedMutex>;
	friend lock_guard<TaggedMutex>;
	friend details::BaseMutexImpl<TaggedMutex>;
#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

public:
	const TaggedMutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;

	using Mtx::mark;
};

}  // namespace reindexer
