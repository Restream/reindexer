#pragma once

#include <optional>
#include <string_view>

namespace reindexer {

/** @brief A borrowed system-record update; a missing value removes the key, an empty value stores an empty body. */
struct [[nodiscard]] SysRecordUpdate {
	std::string_view key;
	std::optional<std::string_view> value;
};

}  // namespace reindexer
