#pragma once

#include <chrono>
#include <string_view>

namespace reindexer {
namespace cluster {

constexpr std::string_view kAsyncReplStatsType = "async";
constexpr std::string_view kClusterReplStatsType = "cluster";

constexpr auto kLeaderPingInterval = std::chrono::milliseconds(200);
constexpr auto kMinLeaderAwaitInterval = kLeaderPingInterval * 5;
constexpr auto kMinStateCheckInerval = kLeaderPingInterval * 5;
constexpr auto kMaxLeaderAwaitDiff = std::chrono::milliseconds(600);
constexpr auto kMaxStateCheckDiff = std::chrono::milliseconds(600);
constexpr auto kGranularSleepInterval = std::chrono::milliseconds(50);
constexpr auto kDesiredLeaderTimeout = std::chrono::seconds(10);

}  // namespace cluster
}  // namespace reindexer
