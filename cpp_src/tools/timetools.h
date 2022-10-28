#pragma once

#include <string_view>

namespace reindexer {

int64_t getTimeNow(std::string_view mode = std::string_view{"sec"});

}  // namespace reindexer
