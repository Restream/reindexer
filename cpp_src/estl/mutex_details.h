#pragma once

#include "thread_annotation_attributes.h"

#ifdef RX_THREAD_SAFETY_ANALYSIS_ENABLE

#include "estl/concepts.h"
#include "estl/marked_mutex.h"

namespace reindexer {

class mutex;
class timed_mutex;
class shared_mutex;
class shared_timed_mutex;
class recursive_mutex;
template <typename Mtx, auto tag>
class TaggedMutex;

namespace concepts {

namespace details {

template <typename T>
struct [[nodiscard]] IsMarkedMutexImpl : public std::false_type {};

template <typename Mtx, MutexMark m>
struct [[nodiscard]] IsMarkedMutexImpl<MarkedMutex<Mtx, m>> : public std::true_type {};

template <typename Mtx>
static constexpr bool IsMarkedMutex = IsMarkedMutexImpl<Mtx>::value;

template <typename T>
struct [[nodiscard]] IsTaggedMutexImpl : public std::false_type {};

template <typename Mtx, auto tag>
struct [[nodiscard]] IsTaggedMutexImpl<TaggedMutex<Mtx, tag>> : public std::true_type {};

template <typename Mtx>
static constexpr bool IsTaggedMutex = IsTaggedMutexImpl<Mtx>::value;

}  // namespace details

template <typename Mtx>
concept EstlMutex = OneOf<Mtx, mutex, timed_mutex, shared_mutex, shared_timed_mutex, recursive_mutex> || details::IsMarkedMutex<Mtx> ||
					details::IsTaggedMutex<Mtx>;

}  // namespace concepts

namespace details {

template <typename Mtx>
struct [[nodiscard]] BaseMutexImpl {
	using Type = Mtx;
};
template <concepts::EstlMutex Mtx>
struct [[nodiscard]] BaseMutexImpl<Mtx> {
	using Type = typename Mtx::Base;
};

template <typename Mtx>
using BaseMutex = typename BaseMutexImpl<Mtx>::Type;

}  // namespace details

}  // namespace reindexer

#endif	// RX_THREAD_SAFETY_ANALYSIS_ENABLE
