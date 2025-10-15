#pragma once

#include "core/keyvalue/uuid.h"
#include "fmt/format.h"

template <>
struct [[nodiscard]] fmt::formatter<reindexer::Uuid> : public fmt::formatter<std::string> {
	template <typename ContextT>
	auto format(const reindexer::Uuid& uuid, ContextT& ctx) const {
		return fmt::formatter<std::string>::format(std::string(uuid), ctx);
	}
};
