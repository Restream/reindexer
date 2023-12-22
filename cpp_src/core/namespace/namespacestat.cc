#include "namespacestat.h"

#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/logger.h"

namespace reindexer {

using namespace std::string_view_literals;

void NamespaceMemStat::GetJSON(WrSerializer &ser) {
	JsonBuilder builder(ser);

	builder.Put("name", name);
	builder.Put("items_count", itemsCount);

	if (emptyItemsCount) builder.Put("empty_items_count", emptyItemsCount);

	builder.Put("strings_waiting_to_be_deleted_size", stringsWaitingToBeDeletedSize);
	builder.Put("storage_ok", storageOK);
	builder.Put("storage_status", storageStatus);
	builder.Put("storage_enabled", storageEnabled);
	builder.Put("storage_path", storagePath);

	builder.Put("storage_loaded", storageLoaded);
	builder.Put("optimization_completed", optimizationCompleted);

	builder.Object("total")
		.Put("data_size", Total.dataSize)
		.Put("indexes_size", Total.indexesSize)
		.Put("cache_size", Total.cacheSize)
		.Put("index_optimizer_memory", Total.indexOptimizerMemory);

	{
		auto obj = builder.Object("replication");
		replication.GetJSON(obj);
	}

	{
		auto obj = builder.Object("join_cache");
		joinCache.GetJSON(obj);
	}
	{
		auto obj = builder.Object("query_cache");
		queryCache.GetJSON(obj);
	}

	auto arr = builder.Array("indexes");
	for (auto &index : indexes) {
		auto obj = arr.Object();
		index.GetJSON(obj);
	}
}

void LRUCacheMemStat::GetJSON(JsonBuilder &builder) {
	builder.Put("total_size", totalSize);
	builder.Put("items_count", itemsCount);
	builder.Put("empty_count", emptyCount);
	builder.Put("hit_count_limit", hitCountLimit);
}

void IndexMemStat::GetJSON(JsonBuilder &builder) {
	if (uniqKeysCount) builder.Put("uniq_keys_count", uniqKeysCount);
	if (trackedUpdatesCount) builder.Put("tracked_updates_count", trackedUpdatesCount);
	if (trackedUpdatesBuckets) builder.Put("tracked_updates_buckets", trackedUpdatesBuckets);
	if (trackedUpdatesSize) builder.Put("tracked_updates_size", trackedUpdatesSize);
	if (trackedUpdatesOveflow) builder.Put("tracked_updates_overflow", trackedUpdatesOveflow);
	if (dataSize) builder.Put("data_size", dataSize);
	if (idsetBTreeSize) builder.Put("idset_btree_size", idsetBTreeSize);
	if (idsetPlainSize) builder.Put("idset_plain_size", idsetPlainSize);
	if (sortOrdersSize) builder.Put("sort_orders_size", sortOrdersSize);
	if (fulltextSize) builder.Put("fulltext_size", fulltextSize);
	if (columnSize) builder.Put("column_size", columnSize);

	if (idsetCache.totalSize || idsetCache.itemsCount || idsetCache.emptyCount || idsetCache.hitCountLimit) {
		auto obj = builder.Object("idset_cache");
		idsetCache.GetJSON(obj);
	}

	builder.Put("name", name);
}

void PerfStat::GetJSON(JsonBuilder &builder) {
	builder.Put("total_queries_count", totalHitCount);
	builder.Put("total_avg_latency_us", totalTimeUs);
	builder.Put("total_avg_lock_time_us", totalLockTimeUs);
	builder.Put("last_sec_qps", avgHitCount);
	builder.Put("last_sec_avg_lock_time_us", avgLockTimeUs);
	builder.Put("last_sec_avg_latency_us", avgTimeUs);
	builder.Put("latency_stddev", stddev);
	builder.Put("min_latency_us", minTimeUs);
	builder.Put("max_latency_us", maxTimeUs);
}

void NamespacePerfStat::GetJSON(WrSerializer &ser) {
	JsonBuilder builder(ser);

	builder.Put("name", name);
	{
		auto obj = builder.Object("updates");
		updates.GetJSON(obj);
	}
	{
		auto obj = builder.Object("selects");
		selects.GetJSON(obj);
	}
	{
		auto obj = builder.Object("transactions");
		transactions.GetJSON(obj);
	}

	auto arr = builder.Array("indexes");

	for (unsigned i = 0; i < indexes.size(); i++) {
		auto obj = arr.Object();
		indexes[i].GetJSON(obj);
	}
}

void IndexPerfStat::GetJSON(JsonBuilder &builder) {
	builder.Put("name", name);
	{
		auto obj = builder.Object("selects");
		selects.GetJSON(obj);
	}
	{
		auto obj = builder.Object("commits");
		commits.GetJSON(obj);
	}
}

static bool LoadLsn(lsn_t &to, const gason::JsonNode &node) {
	if (!node.empty()) {
		if (node.value.getTag() == gason::JSON_OBJECT) {
			to.FromJSON(node);
		} else {
			to = lsn_t(node.As<int64_t>());
		}
		return true;
	}
	return false;
}

void ReplicationState::GetJSON(JsonBuilder &builder) {
	builder.Put("last_lsn", int64_t(lastLsn));
	{
		auto lastLsnObj = builder.Object("last_lsn_v2");
		lastLsn.GetJSON(lastLsnObj);
	}

	builder.Put("temporary", temporary);
	builder.Put("incarnation_counter", incarnationCounter);
	builder.Put("data_hash", dataHash);
	builder.Put("data_count", dataCount);
	builder.Put("updated_unix_nano", int64_t(updatedUnixNano));
	{
		auto nsVersionObj = builder.Object("ns_version");
		nsVersion.GetJSON(nsVersionObj);
	}
	{
		auto clStatusObj = builder.Object("clusterization_status");
		clusterStatus.GetJSON(clStatusObj);
	}
}

void ReplicationState::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);

		if (!LoadLsn(lastLsn, root["last_lsn_v2"])) {
			lastLsn = lsn_t(root["last_lsn"].As<int64_t>());
		}

		temporary = root["temporary"].As<bool>();
		incarnationCounter = root["incarnation_counter"].As<int>();
		dataHash = root["data_hash"].As<uint64_t>();
		dataCount = root["data_count"].As<int>();
		updatedUnixNano = root["updated_unix_nano"].As<uint64_t>();
		LoadLsn(nsVersion, root["ns_version"]);
		auto clStatusNode = root["clusterization_status"];
		if (!clStatusNode.empty()) {
			clusterStatus.FromJSON(clStatusNode);
		}
		{
			// v3 legacy
			lsn_t lastUpstreamLSN;
			wasV3ReplicatedNS = LoadLsn(lastUpstreamLSN, root["last_upstream_lsn"]) && !lastUpstreamLSN.isEmpty();
			wasV3ReplicatedNS = wasV3ReplicatedNS || root["slave_mode"].As<bool>(false);
		}

	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "ReplicationState: %s", ex.what());
	}
}

void ReplicationStat::GetJSON(JsonBuilder &builder) {
	ReplicationState::GetJSON(builder);
	builder.Put("wal_count", walCount);
	builder.Put("wal_size", walSize);
	builder.Put("server_id", serverId);
}

void TxPerfStat::GetJSON(JsonBuilder &builder) {
	builder.Put("total_count", totalCount);
	builder.Put("total_copy_count", totalCopyCount);
	builder.Put("avg_steps_count", avgStepsCount);
	builder.Put("min_steps_count", minStepsCount);
	builder.Put("max_steps_count", maxStepsCount);
	builder.Put("avg_prepare_time_us", avgPrepareTimeUs);
	builder.Put("min_prepare_time_us", minPrepareTimeUs);
	builder.Put("max_prepare_time_us", maxPrepareTimeUs);
	builder.Put("avg_commit_time_us", avgCommitTimeUs);
	builder.Put("min_commit_time_us", minCommitTimeUs);
	builder.Put("max_commit_time_us", maxCommitTimeUs);
	builder.Put("avg_copy_time_us", avgCopyTimeUs);
	builder.Put("min_copy_time_us", minCopyTimeUs);
	builder.Put("max_copy_time_us", maxCopyTimeUs);
}

static constexpr std::string_view nsClusterizationRoleToStr(ClusterizationStatus::Role role) noexcept {
	switch (role) {
		case ClusterizationStatus::Role::ClusterReplica:
			return "cluster_replica"sv;
		case ClusterizationStatus::Role::SimpleReplica:
			return "simple_replica"sv;
		case ClusterizationStatus::Role::None:
		default:
			return "none"sv;
	}
}

static constexpr ClusterizationStatus::Role strToNsClusterizationRole(std::string_view role) noexcept {
	if (role == "cluster_replica"sv) {
		return ClusterizationStatus::Role::ClusterReplica;
	} else if (role == "simple_replica"sv) {
		return ClusterizationStatus::Role::SimpleReplica;
	}
	return ClusterizationStatus::Role::None;
}

void ClusterizationStatus::GetJSON(WrSerializer &ser) const {
	JsonBuilder builder(ser);
	GetJSON(builder);
}

void ClusterizationStatus::GetJSON(JsonBuilder &builder) const {
	builder.Put("leader_id", leaderId);
	builder.Put("role", nsClusterizationRoleToStr(role));
}

Error ClusterizationStatus::FromJSON(span<char> json) {
	try {
		FromJSON(gason::JsonParser().Parse(json));
	} catch (const gason::Exception &ex) {
		return Error(errParseJson, "ClusterizationStatus: %s", ex.what());
	} catch (const Error &err) {
		return err;
	}
	return errOK;
}

void ClusterizationStatus::FromJSON(const gason::JsonNode &root) {
	leaderId = root["leader_id"].As<int>();
	role = strToNsClusterizationRole(root["role"].As<std::string_view>());
}

void ReplicationStateV2::GetJSON(JsonBuilder &builder) {
	builder.Put("last_lsn", int64_t(lastLsn));
	builder.Put("data_hash", dataHash);
	builder.Put("ns_version", int64_t(nsVersion));
	auto clusterObj = builder.Object("cluster_status");
	clusterStatus.GetJSON(clusterObj);
}

void ReplicationStateV2::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		lastLsn = lsn_t(root["last_lsn"].As<int64_t>());
		dataHash = root["data_hash"].As<uint64_t>();
		nsVersion = lsn_t(root["ns_version"].As<int64_t>());
		clusterStatus.FromJSON(root["cluster_status"]);
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "ReplicationState: %s", ex.what());
	}
}

}  // namespace reindexer
