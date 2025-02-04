#pragma once

#include "core/namespace/namespacename.h"
#include "fmt/format.h"

template <>
struct fmt::printf_formatter<reindexer::NamespaceName> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::NamespaceName& name, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "{}", name.OriginalName());
	}
};

template <>
struct fmt::formatter<reindexer::NamespaceName> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::NamespaceName& name, ContextT& ctx) const {
		return fmt::formatter<std::string_view>::format(name.OriginalName(), ctx);
	}
};
