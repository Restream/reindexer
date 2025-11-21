#pragma once

#include <iosfwd>
#include <string>
#include <string_view>
#include <version>
#include "core/type_consts.h"
#include "estl/defines.h"
#include "estl/intrusive_ptr.h"

#if defined(REINDEX_CORE_BUILD)
#include "fmt/format.h"
#elif defined(REINDEX_WITH_EXTERNAL_FMT)
#include <fmt/format.h>
#elif defined(__cpp_lib_format)
#include <format>
#endif	// REINDEX_CORE_BUILD

namespace reindexer {
namespace format_details {

#if defined(REINDEX_CORE_BUILD) || defined(REINDEX_WITH_EXTERNAL_FMT)

#if defined(_MSC_VER) && _MSC_VER <= 1929
template <typename... Args>
using RxFormatString = std::string_view;

template <typename... Args>
RX_ALWAYS_INLINE constexpr std::string_view FormatStringToStringView(RxFormatString<Args...> fmt) {
	return fmt;
}
#else	// !defined(_MSC_VER) || _MSC_VER > 1929
template <typename... Args>
using RxFormatString = fmt::format_string<Args...>;

template <typename... Args>
RX_ALWAYS_INLINE constexpr std::string_view FormatStringToStringView(RxFormatString<Args...> fmt) {
	return std::string_view(fmt.get().data(), fmt.get().size());
}
#endif	// defined(_MSC_VER) && _MSC_VER <= 1929

template <typename... Args>
RX_ALWAYS_INLINE std::string format(RxFormatString<Args...> fmt, Args&&... args) {
	return fmt::vformat(FormatStringToStringView<Args...>(fmt), fmt::make_format_args(args...));
}

#else  // !defined(REINDEX_CORE_BUILD) && !defined(REINDEX_WITH_EXTERNAL_FMT)
template <typename... Args>
using RxFormatString = std::string_view;

template <typename... Args>
RX_ALWAYS_INLINE constexpr std::string_view FormatStringToStringView(RxFormatString<Args...> fmt) {
	return fmt;
}

#if defined(__cpp_lib_format)
template <typename... Args>
RX_ALWAYS_INLINE std::string format(RxFormatString<Args...> fmt, Args&&... args) {
	return std::vformat(FormatStringToStringView<Args...>(fmt), std::make_format_args(args...));
}
#elif !defined(REINDEX_DEBUG_NO_FMT_FALLBACK)
template <typename... Args>
RX_ALWAYS_INLINE std::string format(RxFormatString<Args...> fmt, [[maybe_unused]] Args&&... args) {
	// You may define REINDEX_WITH_EXTERNAL_FMT macro to include your own external fmtlib
	std::string fmtString(fmt);
	fmtString.append("/ (unable to format error text; compiler does not support format library)");
	return fmtString;
}
#else	// defined(REINDEX_DEBUG_NO_FMT_FALLBACK)

template <typename... Args>
constexpr bool kFormatIsAvaliable = false;	// This value is always false; Required for correct static assertion

template <typename... Args>
std::string format([[maybe_unused]] RxFormatString<Args...> fmt, [[maybe_unused]] Args&&... args) {
	static_assert(
		kFormatIsAvaliable<Args...>,
		"Format library is required, but not available for this build (explicitly disabled via REINDEX_DEBUG_NO_FMT_FALLBACK macro)");
	return {};
}
#endif	// defined(__cpp_lib_format)

#endif	// REINDEX_CORE_BUILD

}  // namespace format_details

[[noreturn]] void print_backtrace_and_abort(const char* assertion, const char* file, unsigned line, std::string_view description) noexcept;
[[noreturn]] void print_backtrace_and_abort(const char* assertion, const char* file, unsigned line) noexcept;

#if defined(NDEBUG)
#define assertf(...) ((void)0)
#else
// Using (void)f here to force ';' usage after the macro
#define assertf(e, f, ...)                                                             \
	if (!(e)) [[unlikely]] {                                                           \
		try {                                                                          \
			auto description = reindexer::format_details::format(f, __VA_ARGS__);      \
			reindexer::print_backtrace_and_abort(#e, __FILE__, __LINE__, description); \
		} catch (...) {                                                                \
			reindexer::print_backtrace_and_abort(#e, __FILE__, __LINE__);              \
		}                                                                              \
	}                                                                                  \
	(void)f
#endif	// NDEBUG

#if defined(RX_WITH_STDLIB_DEBUG)
// Using (void)f here to force ';' usage after the macro
#define assertf_dbg(e, f, ...)                                                         \
	if (!(e)) [[unlikely]] {                                                           \
		try {                                                                          \
			auto description = reindexer::format_details::format(f, __VA_ARGS__);      \
			reindexer::print_backtrace_and_abort(#e, __FILE__, __LINE__, description); \
		} catch (...) {                                                                \
			reindexer::print_backtrace_and_abort(#e, __FILE__, __LINE__);              \
		}                                                                              \
	}                                                                                  \
	(void)f
#else
#define assertf_dbg(...) ((void)0)
#endif	// RX_WITH_STDLIB_DEBUG

class [[nodiscard]] Error final : public std::exception {
	using WhatT = intrusive_atomic_rc_wrapper<std::string>;
	using WhatPtr = intrusive_ptr<WhatT>;
	const static WhatPtr defaultErrorText_;

public:
	Error() noexcept = default;
	Error(ErrorCode code) noexcept : std::exception(), code_{code} {}
	Error(ErrorCode code, std::string&& what) noexcept : std::exception(), code_{code} {
		if (code_ != errOK) {
			try {
				what_ = make_intrusive<WhatT>(std::move(what));
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
	Error(ErrorCode code, std::string_view what) noexcept : std::exception(), code_{code} {
		if (code_ != errOK) {
			try {
				what_ = make_intrusive<WhatT>(what);
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
	Error(ErrorCode code, const char* what) noexcept : Error(code, std::string_view(what)) {}
	Error(const std::exception& e, ErrorCode backoffCode = errSystem) noexcept : std::exception(), code_{backoffCode} {
		try {
			if (auto err = dynamic_cast<const Error*>(&e); err) {
				code_ = err->code_;
				what_ = err->what_;
			} else {
				what_ = make_intrusive<WhatT>(e.what());
			}
		} catch (...) {
			what_ = defaultErrorText_;
		}
	}
	Error(std::exception&& e, ErrorCode backoffCode = errSystem) noexcept : std::exception(), code_{backoffCode} {
		try {
			if (auto err = dynamic_cast<Error*>(&e); err) {
				code_ = err->code_;
				what_ = std::move(err->what_);
			} else {
				what_ = make_intrusive<WhatT>(e.what());
			}
		} catch (...) {
			what_ = defaultErrorText_;
		}
	}
	Error(const Error&) noexcept = default;
	Error(Error&&) noexcept = default;
	Error& operator=(const Error&) noexcept = default;
	Error& operator=(Error&&) noexcept = default;

	template <typename... Args>
	Error(ErrorCode code, format_details::RxFormatString<Args...> fmt, Args&&... args) noexcept : std::exception(), code_{code} {
		if (code_ != errOK) {
			try {
				try {
					what_ = make_intrusive<WhatT>(format_details::format(fmt, std::forward<Args>(args)...));
				} catch (const std::exception&) {
					const std::string_view view = format_details::FormatStringToStringView<Args...>(fmt);
					assertf_dbg(false, "Incorrect error format: '{}'", view);
					what_ = make_intrusive<WhatT>(view);
				}
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}

	const char* what() const noexcept override { return whatStr().c_str(); }
	const std::string& whatStr() const& noexcept {
		static const std::string noerr("");
		return what_ ? *what_ : noerr;
	}
	std::string whatStr() && noexcept {
		if (what_) {
			return std::move(*what_);
		} else {
			return {};
		}
	}
	ErrorCode code() const noexcept { return code_; }
	bool ok() const noexcept { return code_ == errOK; }

	explicit operator bool() const noexcept { return !ok(); }
	bool operator==(const Error& other) const noexcept { return code() == other.code() && whatStr() == other.whatStr(); }
	bool operator!=(const Error& other) const noexcept { return !(*this == other); }

private:
	WhatPtr what_;
	ErrorCode code_{errOK};
};

std::ostream& operator<<(std::ostream& os, const Error& error);

}  // namespace reindexer
