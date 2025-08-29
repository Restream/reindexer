#pragma once

#include <condition_variable>
#include "thread_annotation_attributes.h"

namespace reindexer {

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <typename>
class unique_lock;

class [[nodiscard]] condition_variable : private std::condition_variable {
	using Base = std::condition_variable;

public:
	template <typename Mutex>
	RX_ALWAYS_INLINE void wait(unique_lock<Mutex>& lock) {
		return Base::wait(lock);
	}
	template <typename Mutex, typename Predicate>
	RX_ALWAYS_INLINE void wait(unique_lock<Mutex>& lock, Predicate predicate) {
		return Base::wait(lock, std::move(predicate));
	}
	template <typename Mutex, typename Rep, typename Period>
	RX_ALWAYS_INLINE std::cv_status wait_for(unique_lock<Mutex>& lock, const std::chrono::duration<Rep, Period>& rel_time) {
		return Base::wait_for(lock, rel_time);
	}
	template <typename Mutex, typename Rep, typename Period, typename Predicate>
	RX_ALWAYS_INLINE bool wait_for(unique_lock<Mutex>& lock, const std::chrono::duration<Rep, Period>& rel_time, Predicate pred) {
		return Base::wait_for(lock, rel_time, pred);
	}
	using Base::notify_one;
	using Base::notify_all;
};

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

using std::condition_variable;

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

}  // namespace reindexer
