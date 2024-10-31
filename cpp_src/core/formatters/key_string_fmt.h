#pragma once

#include "core/keyvalue/key_string.h"
#include "fmt/format.h"

template <>
struct fmt::printf_formatter<reindexer::key_string> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::key_string& s, ContextT& ctx) const {
		return s ? fmt::format_to(ctx.out(), "{}", std::string_view(*s)) : fmt::format_to(ctx.out(), "<null key_string>");
	}
};

template <>
struct fmt::formatter<reindexer::key_string> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::key_string& s, ContextT& ctx) const {
		return s ? fmt::formatter<std::string_view>::format(std::string_view(*s), ctx) : fmt::format_to(ctx.out(), "<null key_string>");
	}
};
