#pragma once

#include <mutex>
#include "estl/thread_annotation_attributes.h"

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

#define WRAP_MUTEX(MutexName)                                                       \
	class [[nodiscard]] RX_CAPABILITY("mutex") MutexName : private std::MutexName { \
		using Base = std::MutexName;                                                \
                                                                                    \
		friend shared_lock<MutexName>;                                              \
		friend lock_guard<MutexName>;                                               \
		friend unique_lock<MutexName>;                                              \
		friend details::BaseMutexImpl<MutexName>;                                   \
		template <typename, typename>                                               \
		friend class contexted_unique_lock;                                         \
		template <typename, typename>                                               \
		friend class contexted_shared_lock;                                         \
		template <typename...>                                                      \
		friend class scoped_lock;                                                   \
                                                                                    \
	public:                                                                         \
		const MutexName& operator!() const& noexcept { return *this; }              \
		auto operator!() const&& = delete;                                          \
	};

WRAP_MUTEX(mutex)
WRAP_MUTEX(recursive_mutex)

#undef WRAP_MUTEX

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

using std::mutex;
using std::recursive_mutex;

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

}  // namespace reindexer
