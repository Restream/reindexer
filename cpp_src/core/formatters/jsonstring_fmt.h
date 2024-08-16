#pragma once

#include "fmt/format.h"
#include "gason/gason.h"

template <>
struct fmt::printf_formatter<gason::JsonString> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const gason::JsonString& s, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "{}", std::string_view(s));
	}
};
