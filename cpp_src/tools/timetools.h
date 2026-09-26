#pragma once

#include <array>
#include <cstdint>
#include <ctime>
#include <string_view>

namespace reindexer {

enum class [[nodiscard]] TimeUnit : uint8_t { sec, msec, usec, nsec };
inline constexpr size_t kTimeUnitsCount = static_cast<size_t>(TimeUnit::nsec) + 1;
using NowTimes = std::array<int64_t, kTimeUnitsCount>;

TimeUnit ToTimeUnit(std::string_view unit);
std::string_view TimeUnitToString(TimeUnit timeUnit);
int64_t ConvertTime(int64_t t, TimeUnit from, TimeUnit to);

int64_t getTimeNow(TimeUnit = TimeUnit::sec);
std::tm localtime(const std::time_t& time_tt);

}  // namespace reindexer
