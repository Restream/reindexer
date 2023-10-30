#pragma once

#include "estl/defines.h"

namespace reindexer {

#ifdef NDEBUG
#define assertrx(e) ((void)0)
#define assertrx_throw(e) ((void)0)
#define assertrx_dbg(e) ((void)0)
#else  // !NDEBUG

// fail_assertrx can actually throw, but this exception can not be handled properly,
// so it was marked as 'noexcept' for the optimization purposes
[[noreturn]] void fail_assertrx(const char *assertion, const char *file, unsigned line, const char *function) noexcept;
[[noreturn]] void fail_throwrx(const char *assertion, const char *file, unsigned line, const char *function);

#ifdef __cplusplus
#define assertrx(expr) (rx_likely(static_cast<bool>(expr)) ? void(0) : reindexer::fail_assertrx(#expr, __FILE__, __LINE__, __FUNCTION__))
#define assertrx_throw(expr) \
    (rx_likely(static_cast<bool>(expr)) ? void(0) : reindexer::fail_throwrx(#expr, __FILE__, __LINE__, __FUNCTION__))
#endif	// __cplusplus

#ifndef RX_WITH_STDLIB_DEBUG
#define assertrx_dbg(e) ((void)0)
#else  // RX_WITH_STDLIB_DEBUG

#ifdef __cplusplus
// Macro for the extra debug. Works only when RX_WITH_STDLIB_DEBUG is defined and NDEBUG is not defined
#define assertrx_dbg(expr) assertrx(expr)
#endif	// __cplusplus

#endif	// !RX_WITH_STDLIB_DEBUG

#endif	// NDEBUG

}  // namespace reindexer
