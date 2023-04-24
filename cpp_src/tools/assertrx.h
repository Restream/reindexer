#pragma once

namespace reindexer {

#ifdef NDEBUG
#define assertrx(e) ((void)0)
#define assertrx_throw(e) ((void)0)
#else  // !NDEBUG

[[noreturn]] void fail_assertrx(const char *assertion, const char *file, unsigned line, const char *function);
[[noreturn]] void fail_throwrx(const char *assertion, const char *file, unsigned line, const char *function);

#ifdef __cplusplus
#define assertrx(expr) (static_cast<bool>(expr) ? void(0) : reindexer::fail_assertrx(#expr, __FILE__, __LINE__, __FUNCTION__))
#define assertrx_throw(expr) (static_cast<bool>(expr) ? void(0) : reindexer::fail_throwrx(#expr, __FILE__, __LINE__, __FUNCTION__))
#endif	// __cplusplus

#endif	// NDEBUG

}  // namespace reindexer
