#pragma once

#include "core/namespace/namespacestat.h"
#include "fmt/format.h"

template <>
struct [[nodiscard]] fmt::formatter<reindexer::ReplicationDataHash> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::ReplicationDataHash& datahash, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "{}/{}", datahash.hashV1,
							  datahash.hashV2.has_value() ? std::to_string(*datahash.hashV2) : "<empty>");
	}
};

template <>
struct [[nodiscard]] fmt::formatter<reindexer::PayloadChecksum> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	constexpr auto parse(ContextT& ctx) {
		return ctx.begin();
	}
	template <typename ContextT>
	auto format(const reindexer::PayloadChecksum& datahash, ContextT& ctx) const {
		return fmt::format_to(ctx.out(), "{}/{}", datahash.hashV1, datahash.hashV2);
	}
};
