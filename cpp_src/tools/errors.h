#pragma once

#include <iostream>
#include <string>
#include <string_view>
#include "core/type_consts.h"
#include "estl/intrusive_ptr.h"

#ifdef REINDEX_CORE_BUILD
#include "fmt/printf.h"
#endif	// REINDEX_CORE_BUILD

namespace reindexer {

[[noreturn]] void print_backtrace_and_abort(std::string&& msg);

#if defined(REINDEX_CORE_BUILD)
template <typename... Args>
std::string assertf_fmt(const char* fmt, const Args&... args) {
	return fmt::sprintf(fmt, args...);
}
#if defined(NDEBUG)
#define assertf(...) ((void)0)
#else
// Using (void)fmt here to force ';' usage after the macro
#define assertf(e, fmt, ...)                                                                                     \
	if rx_unlikely (!(e)) {                                                                                      \
		reindexer::print_backtrace_and_abort(                                                                    \
			reindexer::assertf_fmt("%s:%d: failed assertion '%s':\n" fmt, __FILE__, __LINE__, #e, __VA_ARGS__)); \
	}                                                                                                            \
	(void)fmt
#endif	// NDEBUG

#ifdef RX_WITH_STDLIB_DEBUG
#define assertf_dbg(e, fmt, ...)                                                                                                       \
	if rx_unlikely (!(e)) {                                                                                                            \
		print_backtrace_and_abort(reindexer::assertf_fmt("%s:%d: failed assertion '%s':\n" fmt, __FILE__, __LINE__, #e, __VA_ARGS__)); \
	}                                                                                                                                  \
	(void)fmt
#else  // RX_WITH_STDLIB_DEBUG
#define assertf_dbg(...) ((void)0)
#endif	// RX_WITH_STDLIB_DEBUG

#endif	// REINDEX_CORE_BUILD

class [[nodiscard]] Error final : public std::exception {
	using WhatT = intrusive_atomic_rc_wrapper<std::string>;
	using WhatPtr = intrusive_ptr<WhatT>;
	const static WhatPtr defaultErrorText_;

public:
	Error() noexcept = default;
	Error(ErrorCode code) noexcept : std::exception(), code_{code} {}
	Error(ErrorCode code, std::string what) noexcept : std::exception(), code_{code} {
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
	Error(ErrorCode code, const char* what) noexcept : std::exception(), code_{code} {
		if (code_ != errOK) {
			try {
				what_ = make_intrusive<WhatT>(what);
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
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

#ifdef REINDEX_CORE_BUILD
	template <typename... Args>
	Error(ErrorCode code, const char* fmt, const Args&... args) noexcept : std::exception(), code_{code} {
		if (code_ != errOK) {
			try {
				try {
					what_ = make_intrusive<WhatT>(fmt::sprintf(fmt, args...));
				} catch (const std::exception&) {
					assertf_dbg(false, "Incorrect error format: '%s'", fmt);
					what_ = make_intrusive<WhatT>(fmt);
				}
			} catch (...) {
				what_ = defaultErrorText_;
			}
		}
	}
#endif	// REINDEX_CORE_BUILD

	[[nodiscard]] const char* what() const noexcept override { return whatStr().c_str(); }
	[[nodiscard]] const std::string& whatStr() const& noexcept {
		static const std::string noerr("");
		return what_ ? *what_ : noerr;
	}
	[[nodiscard]] std::string whatStr() && noexcept {
		if (what_) {
			return std::move(*what_);
		} else {
			return {};
		}
	}
	[[nodiscard]] ErrorCode code() const noexcept { return code_; }
	[[nodiscard]] bool ok() const noexcept { return code_ == errOK; }

	explicit operator bool() const noexcept { return !ok(); }
	[[nodiscard]] bool operator==(const Error& other) const noexcept { return code() == other.code() && whatStr() == other.whatStr(); }
	[[nodiscard]] bool operator!=(const Error& other) const noexcept { return !(*this == other); }

private:
	WhatPtr what_;
	ErrorCode code_{errOK};
};

std::ostream& operator<<(std::ostream& os, const Error& error);

}  // namespace reindexer
