
#include "namespacestat.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/jsontools.h"
#include "tools/logger.h"

namespace reindexer {

void NamespaceMemStat::GetJSON(WrSerializer &ser) {
	JsonBuilder builder(ser);

	builder.Put("name", name);
	builder.Put("items_count", itemsCount);

	if (emptyItemsCount) builder.Put("empty_items_count", emptyItemsCount);

	builder.Put("data_size", dataSize);
	builder.Put("storage_ok", storageOK);
	builder.Put("storage_path", storagePath);

	builder.Put("storage_loaded", storageLoaded);
	builder.Put("optimization_completed", optimizationCompleted);

	builder.Object("total").Put("data_size", Total.dataSize).Put("indexes_size", Total.indexesSize).Put("cache_size", Total.cacheSize);

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

void MasterState::GetJSON(JsonBuilder &builder) {
	{
		auto lastUpstreamLSNmObj = builder.Object("last_upstream_lsn");
		lastUpstreamLSNm.GetJSON(lastUpstreamLSNmObj);
	}
	builder.Put("data_hash", dataHash);
	builder.Put("data_count", dataCount);
	builder.Put("updated_unix_nano", int64_t(updatedUnixNano));
}

void MasterState::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		FromJSON(root);
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "MasterState: %s", ex.what());
	}
}

void LoadLsn(lsn_t &to, const gason::JsonNode &node) {
	if (!node.empty()) {
		if (node.value.getTag() == gason::JSON_OBJECT)
			to.FromJSON(node);
		else
			to = lsn_t(node.As<int64_t>());
	}
}

void MasterState::FromJSON(const gason::JsonNode &root) {
	try {
		LoadLsn(lastUpstreamLSNm, root["last_upstream_lsn"]);
		dataHash = root["data_hash"].As<uint64_t>();
		dataCount = root["data_count"].As<int>();
		updatedUnixNano = root["updated_unix_nano"].As<int64_t>();
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "MasterState: %s", ex.what());
	}
}

static string_view replicationStatusToStr(ReplicationState::Status status) {
	switch (status) {
		case ReplicationState::Status::Idle:
			return "idle"_sv;
		case ReplicationState::Status::Error:
			return "error"_sv;
		case ReplicationState::Status::Fatal:
			return "fatal"_sv;
		case ReplicationState::Status::Syncing:
			return "syncing"_sv;
		case ReplicationState::Status::None:
		default:
			return "none"_sv;
	}
}

static ReplicationState::Status strToReplicationStatus(string_view status) {
	if (status == "idle"_sv) {
		return ReplicationState::Status::Idle;
	} else if (status == "error"_sv) {
		return ReplicationState::Status::Error;
	} else if (status == "fatal"_sv) {
		return ReplicationState::Status::Fatal;
	} else if (status == "syncing"_sv) {
		return ReplicationState::Status::Syncing;
	}
	return ReplicationState::Status::None;
}

void ReplicationState::GetJSON(JsonBuilder &builder) {
	builder.Put("last_lsn", int64_t(lastLsn));
	{
		auto lastLsnObj = builder.Object("last_lsn_v2");
		lastLsn.GetJSON(lastLsnObj);
	}
	builder.Put("slave_mode", slaveMode);
	builder.Put("replicator_enabled", replicatorEnabled);
	builder.Put("temporary", temporary);
	builder.Put("incarnation_counter", incarnationCounter);
	builder.Put("data_hash", dataHash);
	builder.Put("data_count", dataCount);
	builder.Put("updated_unix_nano", int64_t(updatedUnixNano));
	builder.Put("status", replicationStatusToStr(status));
	{
		auto originLSNObj = builder.Object("origin_lsn");
		originLSN.GetJSON(originLSNObj);
	}
	{
		auto lastSelfLSNObj = builder.Object("last_self_lsn");
		lastSelfLSN.GetJSON(lastSelfLSNObj);
	}
	{
		auto lastUpstreamLSNObj = builder.Object("last_upstream_lsn");
		lastUpstreamLSN.GetJSON(lastUpstreamLSNObj);
	}
	if (replicatorEnabled) {
		builder.Put("error_code", replError.code());
		builder.Put("error_message", replError.what());
		auto masterObj = builder.Object("master_state");
		masterState.GetJSON(masterObj);
	}
}

void ReplicationState::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);

		lastLsn = lsn_t(root["last_lsn"].As<int64_t>());
		LoadLsn(lastLsn, root["last_lsn_v2"]);

		slaveMode = root["slave_mode"].As<bool>();
		replicatorEnabled = root["replicator_enabled"].As<bool>();
		temporary = root["temporary"].As<bool>();
		incarnationCounter = root["incarnation_counter"].As<int>();
		dataHash = root["data_hash"].As<uint64_t>();
		dataCount = root["data_count"].As<int>();
		updatedUnixNano = root["updated_unix_nano"].As<uint64_t>();
		status = strToReplicationStatus(root["status"].As<string_view>());
		LoadLsn(originLSN, root["origin_lsn"]);
		LoadLsn(lastSelfLSN, root["last_self_lsn"]);
		LoadLsn(lastUpstreamLSN, root["last_upstream_lsn"]);
		if (replicatorEnabled) {
			int errCode = root["error_code"].As<int>();
			replError = Error(errCode, root["error_message"].As<std::string>());
			try {
				masterState.FromJSON(root["master_state"]);
			} catch (const Error &e) {
				logPrintf(LogError, "[repl] Can't load master state error %s", e.what());
			} catch (const gason::Exception &e) {
				logPrintf(LogError, "[repl] Can't load master state gasson error %s", e.what());
			}
		}
	} catch (const gason::Exception &ex) {
		throw Error(errParseJson, "ReplicationState: %s", ex.what());
	}
}

void ReplicationStat::GetJSON(JsonBuilder &builder) {
	ReplicationState::GetJSON(builder);
	if (!slaveMode) {
		builder.Put("wal_count", walCount);
		builder.Put("wal_size", walSize);
	}
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

}  // namespace reindexer
