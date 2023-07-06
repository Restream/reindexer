#pragma once

/// likely()
#if !defined(likely)
#if defined(__GNUC__) || defined(__clang__)
#define rx_likely(x) (__builtin_expect(!!(x), 1))
#else  // defined(__GNUC__) || defined(__clang__)
#define rx_likely(x) x
#endif	// defined(__GNUC__) || defined(__clang__)
#else	// !defined(likely)
#define rx_likely(x) likely(x)
#endif	// !defined(likely)

/// unlikely()
#if !defined(unlikely)
#if defined(__GNUC__) || defined(__clang__)
#define rx_unlikely(x) (__builtin_expect(!!(x), 0))
#else  // defined(__GNUC__) || defined(__clang__)
#define rx_unlikely(x) x
#endif	// defined(__GNUC__) || defined(__clang__)
#else	// !defined(unlikely)
#define rx_unlikely(x) unlikely(x)
#endif	// !defined(unlikely)

/// inline/noinline
#if defined(__GNUC__) || defined(__clang__)
#define RX_ALWAYS_INLINE inline __attribute__((always_inline))
#define RX_NO_INLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#define RX_ALWAYS_INLINE __forceinline
#define RX_NO_INLINE __declspec(noinline)
#else
#define RX_ALWAYS_INLINE inline
#define RX_NO_INLINE
#endif
