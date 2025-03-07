#pragma once

/// likely()
#if !defined(likely)
#if defined(__GNUC__) || defined(__clang__)
#define rx_likely(x) (__builtin_expect(!!(x), 1))
#else  // defined(__GNUC__) || defined(__clang__)
#define rx_likely(x) (x)
#endif	// defined(__GNUC__) || defined(__clang__)
#else	// !defined(likely)
#define rx_likely(x) likely(x)
#endif	// !defined(likely)

/// unlikely()
#if !defined(unlikely)
#if defined(__GNUC__) || defined(__clang__)
#define rx_unlikely(x) (__builtin_expect(!!(x), 0))
#else  // defined(__GNUC__) || defined(__clang__)
#define rx_unlikely(x) (x)
#endif	// defined(__GNUC__) || defined(__clang__)
#else	// !defined(unlikely)
#define rx_unlikely(x) unlikely(x)
#endif	// !defined(unlikely)

/// inline/noinline
#if defined(__GNUC__) || defined(__clang__)
#define RX_ATTR_ALWAYS_INLINE __attribute__((always_inline))
#define RX_ALWAYS_INLINE inline RX_ATTR_ALWAYS_INLINE
#if defined(__clang__)
#define RX_PRE_LMBD_ALWAYS_INLINE
#define RX_POST_LMBD_ALWAYS_INLINE RX_ATTR_ALWAYS_INLINE
#else
#define RX_POST_LMBD_ALWAYS_INLINE
#if defined(__MINGW64__) && __GNUC__ < 14
#define RX_PRE_LMBD_ALWAYS_INLINE
#else
#define RX_PRE_LMBD_ALWAYS_INLINE RX_ATTR_ALWAYS_INLINE
#endif
#endif
#define RX_NO_INLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define RX_ALWAYS_INLINE __forceinline
#define RX_PRE_LMBD_ALWAYS_INLINE
#define RX_POST_LMBD_ALWAYS_INLINE
#define RX_NO_INLINE __declspec(noinline)
#else
#define RX_ALWAYS_INLINE inline
#define RX_NO_INLINE
#define RX_PRE_LMBD_ALWAYS_INLINE
#define RX_POST_LMBD_ALWAYS_INLINE
#endif

// Targets
#if defined(_MSC_VER) || !defined(REINDEXER_WITH_SSE)
#define RX_AVX_TARGET_ATTR
#define RX_AVX2_TARGET_ATTR
#define RX_AVX512_TARGET_ATTR
#else
#define RX_AVX_TARGET_ATTR __attribute__((target("sse,sse2,sse3,ssse3,sse4,avx")))
#define RX_AVX2_TARGET_ATTR __attribute__((target("sse,sse2,sse3,ssse3,sse4,avx,avx2,fma")))
#define RX_AVX512_TARGET_ATTR __attribute__((target("sse,sse2,sse3,ssse3,sse4,avx,avx2,avx512f")))
#endif

