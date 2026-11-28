#pragma once

#include <string_view>

namespace reindexer {

/** @brief Virtual WHERE field for WAL queries. Not a real index: the condition is handled by WALSelecter. */
constexpr std::string_view kLsnIndexName = "#lsn";

}  // namespace reindexer
