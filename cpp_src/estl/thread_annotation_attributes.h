#pragma once

#include "defines.h"
// Enable thread safety attributes only with clang.
// The attributes can be safely erased when compiling with other compilers.
#if defined(__clang__) && (!defined(SWIG))
#define RX_THREAD_ANNOTATION_ATTRIBUTE__(x) __attribute__((x))
#define RX_THREAD_SAFETY_ANALYSIS_ENABLE
#define RX_MUTEX_ACCESS private
#else										 // defined(__clang__) && (!defined(SWIG))
#define RX_THREAD_ANNOTATION_ATTRIBUTE__(x)	 // no-op
#define RX_MUTEX_ACCESS public
#endif	// defined(__clang__) && (!defined(SWIG))

#define RX_CAPABILITY(x) RX_THREAD_ANNOTATION_ATTRIBUTE__(capability(x))

#define RX_SCOPED_CAPABILITY RX_THREAD_ANNOTATION_ATTRIBUTE__(scoped_lockable)

#define RX_GUARDED_BY(x) RX_THREAD_ANNOTATION_ATTRIBUTE__(guarded_by(x))

#define RX_PT_GUARDED_BY(x) RX_THREAD_ANNOTATION_ATTRIBUTE__(pt_guarded_by(x))

#define RX_ACQUIRED_BEFORE(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(acquired_before(__VA_ARGS__))

#define RX_ACQUIRED_AFTER(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(acquired_after(__VA_ARGS__))

#define RX_REQUIRES(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(requires_capability(__VA_ARGS__))

#define RX_REQUIRES_SHARED(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(requires_shared_capability(__VA_ARGS__))

#define RX_ACQUIRE(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(acquire_capability(__VA_ARGS__))

#define RX_ACQUIRE_PACK(...) RX_ACQUIRE(__VA_ARGS__)

#define RX_ACQUIRE_SHARED(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(acquire_shared_capability(__VA_ARGS__))

#define RX_RELEASE(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(release_capability(__VA_ARGS__))

#define RX_RELEASE_SHARED(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(release_shared_capability(__VA_ARGS__))

#define RX_RELEASE_GENERIC(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(release_generic_capability(__VA_ARGS__))

#define RX_TRY_ACQUIRE(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_capability(__VA_ARGS__))

#define RX_TRY_ACQUIRE_SHARED(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(try_acquire_shared_capability(__VA_ARGS__))

#define RX_EXCLUDES(...) RX_THREAD_ANNOTATION_ATTRIBUTE__(locks_excluded(__VA_ARGS__))

#define RX_ASSERT_CAPABILITY(x) RX_THREAD_ANNOTATION_ATTRIBUTE__(assert_capability(x))

#define RX_ASSERT_SHARED_CAPABILITY(x) RX_THREAD_ANNOTATION_ATTRIBUTE__(assert_shared_capability(x))

#define RX_RETURN_CAPABILITY(x) RX_THREAD_ANNOTATION_ATTRIBUTE__(lock_returned(x))

#define RX_NO_THREAD_SAFETY_ANALYSIS RX_THREAD_ANNOTATION_ATTRIBUTE__(no_thread_safety_analysis)

#define RX_GET_WITHOUT_MUTEX_ANALYSIS [&]() RX_NO_THREAD_SAFETY_ANALYSIS RX_POST_LMBD_ALWAYS_INLINE -> decltype(auto)
