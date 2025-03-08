#pragma once

#include "core/keyvalue/uuid.h"
#include "fmt/format.h"
#include "fmt/printf.h"

template <>
struct fmt::formatter<reindexer::Uuid> : public fmt::formatter<std::string> {
	template <typename ContextT>
	auto format(const reindexer::Uuid& uuid, ContextT& ctx) const {
		return fmt::formatter<std::string>::format(std::string(uuid), ctx);
	}
};

namespace fmt {
template <>
inline auto formatter<reindexer::Uuid>::format(const reindexer::Uuid& uuid, fmt::basic_printf_context<char>& ctx) const {
	return fmt::format_to(ctx.out(), "'{}'", std::string(uuid));
}
}  // namespace fmt
