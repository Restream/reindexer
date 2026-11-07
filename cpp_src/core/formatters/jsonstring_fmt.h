#pragma once

#include "fmt/format.h"
#include "gason/gason.h"

template <>
struct [[nodiscard]] fmt::formatter<gason::JsonString> : public fmt::formatter<std::string_view> {
	template <typename ContextT>
	auto format(const gason::JsonString& s, ContextT& ctx) const {
		return fmt::formatter<std::string_view>::format(s, ctx);
	}
};
