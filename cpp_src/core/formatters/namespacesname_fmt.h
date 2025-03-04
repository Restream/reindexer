#pragma once

#include "core/namespace/namespacename.h"
#include "fmt/format.h"
#include "fmt/printf.h"

template <>
struct fmt::formatter<reindexer::NamespaceName> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const reindexer::NamespaceName& name, ContextT& ctx) const {
		return fmt::formatter<std::string_view>::format(name.OriginalName(), ctx);
	}
};

namespace fmt {
template <>
inline auto formatter<reindexer::NamespaceName>::format(const reindexer::NamespaceName& name, fmt::basic_printf_context<char>& ctx) const {
	return fmt::format_to(ctx.out(), "{}", name.OriginalName());
}
}  // namespace fmt
