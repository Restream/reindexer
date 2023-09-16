#pragma once

#include <cstdint>
#include <string_view>

namespace reindexer {

int64_t getTimeNow(std::string_view mode = std::string_view{"sec"});

}  // namespace reindexer
