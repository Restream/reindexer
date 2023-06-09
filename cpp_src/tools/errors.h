#pragma once

#include <iostream>
#include <string>
#include <string_view>
#include "core/type_consts.h"
#include "estl/intrusive_ptr.h"

#ifdef REINDEX_CORE_BUILD
#include "debug/backtrace.h"
#include "spdlog/fmt/bundled/printf.h"
#include "spdlog/fmt/fmt.h"
#endif	// REINDEX_CORE_BUILD

namespace reindexer {

class Error {
	using WhatT = intrusive_atomic_rc_wrapper<std::string>;
	using WhatPtr = intrusive_ptr<WhatT>;
	const static WhatPtr defaultErrorText_;

public:
	Error() noexcept = default;
	Error(ErrorCode code) noexcept : code_{code} {}
	Error(ErrorCode code, std::string what) noexcept : code_{code} {
		if (code_ != errOK) {
			try {
				what_ = make_intrusive<WhatT>(std::move(what));
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
	Error(ErrorCode code, std::string_view what) noexcept : code_{code} {
		if (code_ != errOK) {
			try {
				what_ = make_intrusive<WhatT>(what);
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
	Error(ErrorCode code, const char *what) noexcept : code_{code} {
		if (code_ != errOK) {
			try {
				what_ = make_intrusive<WhatT>(what);
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
	Error(const std::exception &e) noexcept : code_{errSystem} {
		try {
			what_ = make_intrusive<WhatT>(e.what());
		} catch (...) {
			what_ = defaultErrorText_;
		}
	}
	Error(const Error &) noexcept = default;
	Error(Error &&) noexcept = default;
	Error &operator=(const Error &) noexcept = default;
	Error &operator=(Error &&) noexcept = default;

#ifdef REINDEX_CORE_BUILD
	template <typename... Args>
	Error(ErrorCode code, const char *fmt, const Args &...args) noexcept : code_{code} {
		if (code_ != errOK) {
			try {
				try {
					what_ = make_intrusive<WhatT>(fmt::sprintf(fmt, args...));
				} catch (const fmt::FormatError &) {
					what_ = make_intrusive<WhatT>(fmt);
				}
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
#endif	// REINDEX_CORE_BUILD

	[[nodiscard]] const std::string &what() const &noexcept {
		static const std::string noerr;
		return what_ ? *what_ : noerr;
	}
	[[nodiscard]] std::string what() &&noexcept {
		if (what_) {
			return std::move(*what_);
		} else {
			return {};
		}
	}
	[[nodiscard]] ErrorCode code() const noexcept { return code_; }
	[[nodiscard]] bool ok() const noexcept { return code_ == errOK; }

	explicit operator bool() const noexcept { return !ok(); }
	[[nodiscard]] bool operator==(const Error &other) const noexcept { return code() == other.code() && what() == other.what(); }
	[[nodiscard]] bool operator!=(const Error &other) const noexcept { return !(*this == other); }

private:
	WhatPtr what_;
	ErrorCode code_{errOK};
};

#if defined(REINDEX_CORE_BUILD)
#if defined(NDEBUG)
#define assertf(...) ((void)0)
#else
template <typename... Args>
void assertf_fmt(const char *fmt, const Args &...args) {
	fmt::fprintf(std::cerr, fmt, args...);
}

#define assertf(e, fmt, ...)                                                                     \
	if (!(e)) {                                                                                  \
		assertf_fmt("%s:%d: failed assertion '%s':\n" fmt, __FILE__, __LINE__, #e, __VA_ARGS__); \
		debug::print_crash_query(std::cerr);                                                     \
		abort();                                                                                 \
	}
#endif	// NDEBUG
#endif	// REINDEX_CORE_BUILD

}  // namespace reindexer
