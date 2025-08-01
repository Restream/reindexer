#include "namespacestat.h"
#include "core/cjson/jsonbuilder.h"
#include "core/embedding/embedderscache.h"
#include "core/system_ns_names.h"
#include "gason/gason.h"

namespace reindexer {

using namespace std::string_view_literals;

void NamespaceMemStat::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);

	builder.Put("name", name);
	builder.Put("type", type);
	builder.Put("items_count", itemsCount);

	if (emptyItemsCount) {
		builder.Put("empty_items_count", emptyItemsCount);
	}

	builder.Put("strings_waiting_to_be_deleted_size", stringsWaitingToBeDeletedSize);
	builder.Put("storage_ok", storageOK);
	builder.Put("storage_status", storageStatus);
	builder.Put("storage_enabled", storageEnabled);
	builder.Put("storage_path", storagePath);

	builder.Put("optimization_completed", optimizationCompleted);

	builder.Object("total")
		.Put("data_size", Total.dataSize)
		.Put("indexes_size", Total.indexesSize)
		.Put("cache_size", Total.cacheSize)
		.Put("index_optimizer_memory", Total.indexOptimizerMemory)
		.Put("inmemory_storage_size", Total.inmemoryStorageSize);

	builder.Object("storage").Put("proxy_size", Storage.proxySize);

	if (type.empty() || type == kNamespaceStatType) {
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

		{
			auto arr = builder.Array("indexes");
			for (const auto& index : indexes) {
				auto obj = arr.Object();
				index.GetJSON(obj);
			}
		}
	} else {
		assertrx_throw(type == kEmbeddersStatType);

		auto arr = builder.Array("embedding_caches");
		for (const auto& embedder : embedders) {
			auto obj = arr.Object();
			embedder.GetJSON(obj);
		}
	}
}

void LRUCacheMemStat::GetJSON(JsonBuilder& builder) const {
	builder.Put("total_size", totalSize);
	builder.Put("items_count", itemsCount);
	builder.Put("empty_count", emptyCount);
	builder.Put("hit_count_limit", hitCountLimit);
}

void IndexMemStat::GetJSON(JsonBuilder& builder) const {
	if (uniqKeysCount) {
		builder.Put("uniq_keys_count", uniqKeysCount);
	}
	if (trackedUpdatesCount) {
		builder.Put("tracked_updates_count", trackedUpdatesCount);
	}
	if (trackedUpdatesBuckets) {
		builder.Put("tracked_updates_buckets", trackedUpdatesBuckets);
	}
	if (trackedUpdatesSize) {
		builder.Put("tracked_updates_size", trackedUpdatesSize);
	}
	if (trackedUpdatesOverflow) {
		builder.Put("tracked_updates_overflow", trackedUpdatesOverflow);
	}
	if (dataSize) {
		builder.Put("data_size", dataSize);
	}
	if (idsetBTreeSize) {
		builder.Put("idset_btree_size", idsetBTreeSize);
	}
	if (idsetPlainSize) {
		builder.Put("idset_plain_size", idsetPlainSize);
	}
	if (sortOrdersSize) {
		builder.Put("sort_orders_size", sortOrdersSize);
	}
	if (columnSize) {
		builder.Put("column_size", columnSize);
	}
	if (indexingStructSize) {
		builder.Put("indexing_struct_size", indexingStructSize);
	}
	if (isBuilt.has_value()) {
		builder.Put("is_built", isBuilt.value());
	}

	if (idsetCache.totalSize || idsetCache.itemsCount || idsetCache.emptyCount || idsetCache.hitCountLimit) {
		auto obj = builder.Object("idset_cache");
		idsetCache.GetJSON(obj);
	}

	builder.Put("name", name);
}

void EmbeddersCacheMemStat::GetJSON(JsonBuilder& builder) const {
	builder.Put("cache_tag", tag);
	builder.Put("capacity", capacity);
	{
		auto obj = builder.Object("cache");
		cache.GetJSON(obj);
	}
	builder.Put("storage_ok", storageOK);
	builder.Put("storage_status", storageStatus);
	builder.Put("storage_path", storagePath);
	builder.Put("storage_size", storageSize);
}

void PerfStat::GetJSON(JsonBuilder& builder) const {
	builder.Put("total_queries_count", totalHitCount);
	builder.Put("total_avg_latency_us", totalAvgTimeUs);
	builder.Put("total_avg_lock_time_us", totalAvgLockTimeUs);
	builder.Put("last_sec_qps", lastSecHitCount);
	builder.Put("last_sec_avg_lock_time_us", lastSecAvgLockTimeUs);
	builder.Put("last_sec_avg_latency_us", lastSecAvgTimeUs);
	builder.Put("latency_stddev", stddev);
	builder.Put("min_latency_us", minTimeUs);
	builder.Put("max_latency_us", maxTimeUs);
}

void NamespacePerfStat::GetJSON(WrSerializer& ser) const {
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
	if (queryCountCache.state != LRUCachePerfStat::State::DoesNotExist) {
		auto obj = builder.Object("query_count_cache");
		queryCountCache.GetJSON(obj);
	}
	if (joinCache.state != LRUCachePerfStat::State::DoesNotExist) {
		auto obj = builder.Object("join_cache");
		joinCache.GetJSON(obj);
	}

	auto arr = builder.Array("indexes");

	for (const auto& index : indexes) {
		auto obj = arr.Object();
		index.GetJSON(obj);
	}
}

void EmbedderCachePerfStat::GetJSON(JsonBuilder& builder) const {
	builder.Put("cache_tag", tag);
	LRUCachePerfStat::GetJSON(builder);
}

void IndexPerfStat::GetJSON(JsonBuilder& builder) const {
	builder.Put("name", name);
	{
		auto obj = builder.Object("selects");
		selects.GetJSON(obj);
	}
	{
		auto obj = builder.Object("commits");
		commits.GetJSON(obj);
	}
	if (cache.state != LRUCachePerfStat::State::DoesNotExist) {
		auto obj = builder.Object("cache");
		cache.GetJSON(obj);
	}

	if (!upsertEmbedderCache.tag.empty()) {
		auto obj = builder.Object("upsert_embedder_cache");
		upsertEmbedderCache.GetJSON(obj);
	}
	if (!queryEmbedderCache.tag.empty()) {
		auto obj = builder.Object("query_embedder_cache");
		queryEmbedderCache.GetJSON(obj);
	}
}

static bool LoadLsn(lsn_t& to, const gason::JsonNode& node) {
	if (!node.empty()) {
		if (node.value.getTag() == gason::JsonTag::OBJECT) {
			to.FromJSON(node);
		} else {
			to = lsn_t(node.As<int64_t>());
		}
		return true;
	}
	return false;
}

void ReplicationState::GetJSON(JsonBuilder& builder) const {
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
	builder.Put("admissible_token", token);
	{
		auto nsVersionObj = builder.Object("ns_version");
		nsVersion.GetJSON(nsVersionObj);
	}
	{
		auto clStatusObj = builder.Object("clusterization_status");
		clusterStatus.GetJSON(clStatusObj);
	}
}

void ReplicationState::FromJSON(std::span<char> json) {
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
		token = root["admissible_token"].As<std::string>();
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

	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "ReplicationState: {}", ex.what());
	}
}

void ReplicationStat::GetJSON(JsonBuilder& builder) const {
	ReplicationState::GetJSON(builder);
	builder.Put("wal_count", walCount);
	builder.Put("wal_size", walSize);
	builder.Put("server_id", serverId);
}

void TxPerfStat::GetJSON(JsonBuilder& builder) const {
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

void TxPerfStat::FromJSON(const gason::JsonNode& node) {
	totalCount = node["total_count"].As<size_t>(totalCount);
	totalCopyCount = node["total_copy_count"].As<size_t>(totalCopyCount);
	avgStepsCount = node["avg_steps_count"].As<size_t>(avgStepsCount);
	minStepsCount = node["min_steps_count"].As<size_t>(minStepsCount);
	maxStepsCount = node["max_steps_count"].As<size_t>(maxStepsCount);
	avgPrepareTimeUs = node["avg_prepare_time_us"].As<size_t>(avgPrepareTimeUs);
	minPrepareTimeUs = node["min_prepare_time_us"].As<size_t>(minPrepareTimeUs);
	maxPrepareTimeUs = node["max_prepare_time_us"].As<size_t>(maxPrepareTimeUs);
	avgCommitTimeUs = node["avg_commit_time_us"].As<size_t>(avgCommitTimeUs);
	minCommitTimeUs = node["min_commit_time_us"].As<size_t>(minCommitTimeUs);
	maxCommitTimeUs = node["max_commit_time_us"].As<size_t>(maxCommitTimeUs);
	avgCopyTimeUs = node["avg_copy_time_us"].As<size_t>(avgCopyTimeUs);
	minCopyTimeUs = node["min_copy_time_us"].As<size_t>(minCopyTimeUs);
	maxCopyTimeUs = node["max_copy_time_us"].As<size_t>(maxCopyTimeUs);
}

void LRUCachePerfStat::GetJSON(JsonBuilder& builder) const {
	switch (state) {
		case State::DoesNotExist:
			return;
		case State::Inactive:
			builder.Put("is_active", false);
			break;
		case State::Active:
			builder.Put("is_active", true);
			break;
	}

	builder.Put("total_queries", TotalQueries());
	builder.Put("cache_hit_rate", HitRate());
}

uint64_t LRUCachePerfStat::TotalQueries() const noexcept { return hits + misses; }

double LRUCachePerfStat::HitRate() const noexcept {
	const auto tq = TotalQueries();
	return tq ? (double(hits) / double(tq)) : 0.0;
}

static constexpr std::string_view nsClusterOperationRoleToStr(ClusterOperationStatus::Role role) noexcept {
	switch (role) {
		case ClusterOperationStatus::Role::ClusterReplica:
			return "cluster_replica"sv;
		case ClusterOperationStatus::Role::SimpleReplica:
			return "simple_replica"sv;
		case ClusterOperationStatus::Role::None:
		default:
			return "none"sv;
	}
}

static constexpr ClusterOperationStatus::Role strToNsClusterOperationRole(std::string_view role) noexcept {
	if (role == "cluster_replica"sv) {
		return ClusterOperationStatus::Role::ClusterReplica;
	} else if (role == "simple_replica"sv) {
		return ClusterOperationStatus::Role::SimpleReplica;
	}
	return ClusterOperationStatus::Role::None;
}

void ClusterOperationStatus::GetJSON(WrSerializer& ser) const {
	JsonBuilder builder(ser);
	GetJSON(builder);
}

void ClusterOperationStatus::GetJSON(JsonBuilder& builder) const {
	builder.Put("leader_id", leaderId);
	builder.Put("role", nsClusterOperationRoleToStr(role));
}

Error ClusterOperationStatus::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		FromJSON(parser.Parse(json));
	} catch (const gason::Exception& ex) {
		return Error(errParseJson, "ClusterOperationStatus: {}", ex.what());
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

void ClusterOperationStatus::FromJSON(const gason::JsonNode& root) {
	leaderId = root["leader_id"].As<int>();
	role = strToNsClusterOperationRole(root["role"].As<std::string_view>());
}

void ReplicationStateV2::GetJSON(JsonBuilder& builder) const {
	builder.Put("last_lsn", int64_t(lastLsn));
	builder.Put("data_hash", dataHash);
	if (HasDataCount()) {
		builder.Put("data_count", dataCount);
	}
	builder.Put("ns_version", int64_t(nsVersion));
	auto clusterObj = builder.Object("cluster_status");
	clusterStatus.GetJSON(clusterObj);
}

void ReplicationStateV2::FromJSON(std::span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		lastLsn = lsn_t(root["last_lsn"].As<int64_t>());
		dataHash = root["data_hash"].As<uint64_t>();
		dataCount = root["data_count"].As<int64_t>(kNoDataCount);
		nsVersion = lsn_t(root["ns_version"].As<int64_t>());
		clusterStatus.FromJSON(root["cluster_status"]);
	} catch (const gason::Exception& ex) {
		throw Error(errParseJson, "ReplicationState: {}", ex.what());
	}
}

}  // namespace reindexer
