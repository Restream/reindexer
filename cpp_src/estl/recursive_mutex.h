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

class [[nodiscard]] RX_CAPABILITY("mutex") recursive_mutex : private std::recursive_mutex {
	using Base = std::recursive_mutex;

	friend shared_lock<recursive_mutex>;
	friend lock_guard<recursive_mutex>;
	friend unique_lock<recursive_mutex>;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;
	template <typename...>
	friend class scoped_lock;

public:
	const recursive_mutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;
};

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

using std::recursive_mutex;

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

}  // namespace reindexer
