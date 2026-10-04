#pragma once
#ifdef WITH_OPENSSL

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/sha.h>

#include <openssl/ssl.h>

#ifdef _WIN32
// Thank's to windows.h include
#ifdef ERROR
#undef ERROR
#endif
#endif

#else
#include <fmt/printf.h>
#include "tools/assertrx.h"

struct [[nodiscard]] TLSDummy {
	template <typename... Args>
	const TLSDummy& operator()(Args&&...) const noexcept {
		assertrx(false);
		return *this;
	}

	template <typename T>
	operator T() const noexcept {
		assertrx(false);
		return {};
	}
};

// for fmt-lib
template <typename Cout>
Cout& operator<<(Cout& cout, TLSDummy) {
	return cout;
}

template <>
struct [[nodiscard]] fmt::formatter<TLSDummy> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const TLSDummy&, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "");
	}
};

#define OPENSSL_DUMMY_SYM(sym) inline TLSDummy sym;

#endif

namespace reindexer::openssl {
inline bool IsBuiltWithOpenSSL() {
#ifdef WITH_OPENSSL
	return true;
#else
	return false;
#endif
}
}  // namespace reindexer::openssl
