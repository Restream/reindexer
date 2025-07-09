#pragma once

#include <cstdint>
#include <ctime>
#include <string_view>

namespace reindexer {

enum class [[nodiscard]] TimeUnit : uint8_t { sec, msec, usec, nsec };
TimeUnit ToTimeUnit(std::string_view unit);

[[nodiscard]] int64_t getTimeNow(TimeUnit = TimeUnit::sec);
[[nodiscard]] std::tm localtime(const std::time_t& time_tt);

}  // namespace reindexer
