#pragma once

#include "core/keyvalue/key_string.h"
#include "fmt/format.h"
#include "fmt/printf.h"

template <>
struct fmt::formatter<reindexer::key_string> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::key_string& s, ContextT& ctx) const {
		return s ? fmt::formatter<std::string_view>::format(std::string_view(s), ctx) : fmt::format_to(ctx.out(), "<null key_string>");
	}
};

namespace fmt {
template <>
inline auto formatter<reindexer::key_string>::format(const reindexer::key_string& s, fmt::basic_printf_context<char>& ctx) const {
	return s ? fmt::format_to(ctx.out(), "{}", std::string_view(s)) : fmt::format_to(ctx.out(), "<null key_string>");
}
}  // namespace fmt
