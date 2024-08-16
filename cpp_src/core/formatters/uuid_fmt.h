#pragma once

#include "fmt/format.h"
#include "core/keyvalue/uuid.h"

template <>
struct fmt::printf_formatter<reindexer::Uuid> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::Uuid& uuid, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "'{}'", std::string(uuid));
	}
};

template <>
struct fmt::formatter<reindexer::Uuid> : public fmt::formatter<std::string> {
	template <typename ContextT>
	auto format(const reindexer::Uuid& uuid, ContextT& ctx) const {
		return fmt::formatter<std::string>::format(std::string(uuid), ctx);
	}
};
