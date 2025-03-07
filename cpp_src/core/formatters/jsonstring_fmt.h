#pragma once

#include "fmt/format.h"
#include "fmt/printf.h"
#include "gason/gason.h"

template <>
struct fmt::formatter<gason::JsonString> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const gason::JsonString& s, ContextT& ctx) const {
		return fmt::formatter<std::string_view>::format(s, ctx);
	}
};

namespace fmt {
template <>
inline auto formatter<gason::JsonString>::format(const gason::JsonString& s, fmt::basic_printf_context<char>& ctx) const {
	return fmt::format_to(ctx.out(), "{}", std::string_view(s));
}
}  // namespace fmt
