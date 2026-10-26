#pragma once

#include "estl/tokenizer_range.h"
#include "fmt/format.h"

template <>
struct [[nodiscard]] fmt::formatter<reindexer::TokenizerRange> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::TokenizerRange& range, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "line: {} column: {} {}", range.lineEnd + 1, range.columnStart, range.columnEnd);
	}
};
