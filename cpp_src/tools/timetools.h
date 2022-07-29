#pragma once

#include <ctime>
#include <string_view>

using std::string;

namespace reindexer {

int64_t getTimeNow(std::string_view mode = std::string_view{"sec"});
std::tm localtime(const std::time_t& time_tt);

}  // namespace reindexer
