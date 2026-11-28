#pragma once

#include "core/id_type.h"
#include "fmt/format.h"

template <>
struct [[nodiscard]] fmt::formatter<reindexer::IdType> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::IdType& id, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "{}", id.ToNumber());
	}
};
