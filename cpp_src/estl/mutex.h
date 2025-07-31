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

class [[nodiscard]] RX_CAPABILITY("mutex") mutex : private std::mutex {
	using Base = std::mutex;

	friend shared_lock<mutex>;
	friend lock_guard<mutex>;
	friend unique_lock<mutex>;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;
	template <typename...>
	friend class scoped_lock;

public:
	const mutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;
};

#else  // RX_THREAD_SAFETY_ANALYSIS_ENABLE

using std::mutex;

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

}  // namespace reindexer
