#pragma once

#include <string_view>

namespace reindexer {

constexpr std::string_view kPerfStatsNamespace = "#perfstats";
constexpr std::string_view kQueriesPerfStatsNamespace = "#queriesperfstats";
constexpr std::string_view kMemStatsNamespace = "#memstats";
constexpr std::string_view kNamespacesNamespace = "#namespaces";
constexpr std::string_view kConfigNamespace = "#config";
constexpr std::string_view kActivityStatsNamespace = "#activitystats";
constexpr std::string_view kClientsStatsNamespace = "#clientsstats";
constexpr std::string_view kClusterConfigNamespace = "#clusterconfig";
constexpr std::string_view kReplicationStatsNamespace = "#replicationstats";
constexpr std::string_view kEmbeddersPseudoNamespace = "!embedders";

}  // namespace reindexer
