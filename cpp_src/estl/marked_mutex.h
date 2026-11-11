#pragma once

#include <string_view>
#include "thread_annotation_attributes.h"
#include "tools/errors.h"

namespace reindexer {

enum class [[nodiscard]] MutexMark : unsigned {
	DbManager = 0u,
	IndexText,
	Namespace,
	Reindexer,
	ReindexerStats,
	CloneNs,
	AsyncStorage,
	StorageDirOps
};
inline std::string_view DescribeMutexMark(MutexMark mark) {
	using namespace std::string_view_literals;
	switch (mark) {
		case MutexMark::DbManager:
			return "Database Manager"sv;
		case MutexMark::IndexText:
			return "Fulltext Index"sv;
		case MutexMark::Namespace:
			return "Namespace"sv;
		case MutexMark::Reindexer:
			return "Database"sv;
		case MutexMark::ReindexerStats:
			return "Reindexer Stats"sv;
		case MutexMark::CloneNs:
			return "Clone namespace"sv;
		case MutexMark::AsyncStorage:
			return "Async storage copy"sv;
		case MutexMark::StorageDirOps:
			return "Storage directories modification"sv;
	}
	throw Error(errLogic, "Unknown mutex type");
}

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <typename>
class unique_lock;
template <typename>
class lock_guard;
template <typename>
class shared_lock;
template <typename, typename>
class contexted_unique_lock;
template <typename, typename>
class contexted_shared_lock;
template <typename>
class smart_lock;

namespace details {
template <typename>
struct BaseMutexImpl;
}  // namespace details
//
#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

template <typename Mutex, MutexMark m>
class [[nodiscard]] RX_CAPABILITY("mutex") MarkedMutex : RX_MUTEX_ACCESS Mutex {
	using Base = Mutex;

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE
	friend unique_lock<MarkedMutex>;
	friend lock_guard<MarkedMutex>;
	friend smart_lock<MarkedMutex>;
	friend shared_lock<MarkedMutex>;
	friend details::BaseMutexImpl<MarkedMutex>;
	template <typename, typename>
	friend class contexted_unique_lock;
	template <typename, typename>
	friend class contexted_shared_lock;
#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE

public:
	constexpr static MutexMark mark = m;
	const MarkedMutex& operator!() const& noexcept { return *this; }
	auto operator!() const&& = delete;
};

}  // namespace reindexer
