#pragma once

#include <iostream>
#include <string>
#include <string_view>
#include "core/type_consts.h"
#include "estl/intrusive_ptr.h"

#ifdef REINDEX_CORE_BUILD
#include "spdlog/fmt/bundled/printf.h"
#include "spdlog/fmt/fmt.h"
#endif	// REINDEX_CORE_BUILD

namespace reindexer {

class Error {
public:
	Error() noexcept = default;
	Error(int code);
	Error(int code, std::string_view what);
#ifdef REINDEX_CORE_BUILD
	template <typename... Args>
	Error(int code, const char *fmt, const Args &... args) : Error(code, fmt::sprintf(fmt, args...)) {}
#endif	// REINDEX_CORE_BUILD

	const std::string &what() const noexcept;
	int code() const noexcept;
	bool ok() const noexcept { return !ptr_; }

	explicit operator bool() const noexcept { return !ok(); }

protected:
	struct payload {
		payload(int code, const std::string &what) : code_(code), what_(what) {}
		int code_;
		std::string what_;
	};
	intrusive_ptr<intrusive_atomic_rc_wrapper<payload>> ptr_;
};

#if defined(NDEBUG) || !defined(REINDEX_CORE_BUILD)
#define assertf(...) ((void)0)
#else
template <typename... Args>
void assertf_fmt(const char *fmt, const Args &... args) {
	fmt::fprintf(std::cerr, fmt, args...);
}

#define assertf(e, fmt, ...)                                                                     \
	if (!(e)) {                                                                                  \
		assertf_fmt("%s:%d: failed assertion '%s':\n" fmt, __FILE__, __LINE__, #e, __VA_ARGS__); \
		abort();                                                                                 \
	}
#endif

}  // namespace reindexer
