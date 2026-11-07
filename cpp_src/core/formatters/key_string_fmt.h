#pragma once

#include "core/keyvalue/key_string.h"
#include "fmt/format.h"

template <>
struct [[nodiscard]] fmt::formatter<reindexer::key_string> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::key_string& s, ContextT& ctx) const {
		return s ? fmt::formatter<std::string_view>::format(std::string_view(s), ctx) : fmt::format_to(ctx.out(), "<null key_string>");
	}
};
