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

constexpr std::string_view kConfigSourceMetaPrefix = "_CONFIG_SOURCE_";

// Meta of the system namespaces, which is written by the core only
constexpr bool isReservedSystemMetaKey(std::string_view nsName, std::string_view key) noexcept {
	return nsName.starts_with('#') && key.starts_with(kConfigSourceMetaPrefix);
}

}  // namespace reindexer
