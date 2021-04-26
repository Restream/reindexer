#pragma once

#include <string_view>

using std::string;

namespace reindexer {

int64_t getTimeNow(std::string_view mode = std::string_view{"sec"});

}  // namespace reindexer
