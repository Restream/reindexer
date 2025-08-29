#pragma once

#include "fmt/format.h"
#include "tools/lsn.h"

template <>
struct [[nodiscard]] fmt::formatter<reindexer::lsn_t> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::lsn_t& lsn, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "{}:{}", lsn.Server(), lsn.Counter());
	}
};
