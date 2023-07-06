#pragma once

#include "namespacedef.h"

namespace reindexer {

constexpr char kPerfStatsNamespace[] = "#perfstats";
constexpr char kQueriesPerfStatsNamespace[] = "#queriesperfstats";
constexpr char kMemStatsNamespace[] = "#memstats";
constexpr char kNamespacesNamespace[] = "#namespaces";
constexpr char kConfigNamespace[] = "#config";
constexpr char kActivityStatsNamespace[] = "#activitystats";
constexpr char kClientsStatsNamespace[] = "#clientsstats";
constexpr char kNsNameField[] = "name";
const std::vector<std::string> kDefDBConfig = {
	R"json({
		"type":"profiling",
		"profiling":{
			"queriesperfstats":false,
			"queries_threshold_us":10,
			"perfstats":false,
			"memstats":true,
			"activitystats":false,
			"long_queries_logging":{
				"select":{
					"threshold_us": -1,
					"normalized": false
				},
				"update_delete":{
					"threshold_us": -1,
					"normalized": false
				},
				"transaction":{
					"threshold_us": -1,
					"avg_step_threshold_us": -1
				}
			}
		}
	})json",
	R"json({
		"type":"namespaces",
		"namespaces":[
			{
				"namespace":"*",
				"log_level":"none",
				"lazyload":false,
				"unload_idle_threshold":0,
				"join_cache_mode":"off",
				"start_copy_policy_tx_size":10000,
				"copy_policy_multiplier":5,
				"tx_size_to_always_copy":100000,
				"optimization_timeout_ms":800,
				"optimization_sort_workers":4,
				"wal_size":4000000,
				"min_preselect_size":1000,
				"max_preselect_size":1000,
				"max_preselect_part":0.1,
				"index_updates_counting_mode":false,
				"sync_storage_flush_limit":20000
			}
		]
	})json",
	R"json({
		"type":"replication",
		"replication":{
			"role":"none",
			"master_dsn":"cproto://127.0.0.1:6534/db",
			"cluster_id":2,
			"force_sync_on_logic_error": false,
			"force_sync_on_wrong_data_hash": false,
			"namespaces":[]
		}
	})json",
	R"json({
		"type":"action",
		"action":{
			"command":""
		}
	})json"};

const std::vector<NamespaceDef> kSystemNsDefs = {
	NamespaceDef(kConfigNamespace, StorageOpts().Enabled().CreateIfMissing().DropOnFileFormatError())
		.AddIndex("type", "hash", "string", IndexOpts().PK()),
	NamespaceDef(kPerfStatsNamespace, StorageOpts())
		.AddIndex(kNsNameField, "hash", "string", IndexOpts().PK())
		.AddIndex("updates.total_queries_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("updates.total_avg_latency_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("updates.last_sec_qps", "-", "int64", IndexOpts().Dense())
		.AddIndex("updates.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("selects.total_queries_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("selects.total_avg_latency_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("selects.last_sec_qps", "-", "int64", IndexOpts().Dense())
		.AddIndex("selects.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("transactions.total_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("transactions.total_copy_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("transactions.avg_steps_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("transactions.avg_prepare_time_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("transactions.avg_commit_time_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("transactions.avg_copy_time_us", "-", "int64", IndexOpts().Dense()),
	NamespaceDef(kActivityStatsNamespace, StorageOpts())
		.AddIndex("query_id", "hash", "int", IndexOpts().PK())
		.AddIndex("client", "-", "string", IndexOpts().Dense())
		.AddIndex("query", "-", "string", IndexOpts().Dense())
		.AddIndex("query_start", "-", "string", IndexOpts().Dense())
		.AddIndex("blocked", "-", "bool", IndexOpts().Dense())
		.AddIndex("description", "-", "string", IndexOpts().Sparse()),
	NamespaceDef(kQueriesPerfStatsNamespace, StorageOpts())
		.AddIndex("query", "hash", "string", IndexOpts().PK())
		.AddIndex("total_queries_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("total_avg_latency_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("total_avg_lock_time_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("last_sec_qps", "-", "int64", IndexOpts().Dense())
		.AddIndex("last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("last_sec_avg_lock_time_us", "-", "int64", IndexOpts().Dense())
		.AddIndex("latency_stddev", "-", "double", IndexOpts().Dense()),
	NamespaceDef(kNamespacesNamespace, StorageOpts()).AddIndex(kNsNameField, "hash", "string", IndexOpts().PK()),
	NamespaceDef(kPerfStatsNamespace, StorageOpts()).AddIndex(kNsNameField, "hash", "string", IndexOpts().PK()),
	NamespaceDef(kMemStatsNamespace, StorageOpts())
		.AddIndex(kNsNameField, "hash", "string", IndexOpts().PK())
		.AddIndex("items_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("total.data_size", "-", "int64", IndexOpts().Dense())
		.AddIndex("total.indexes_size", "-", "int64", IndexOpts().Dense())
		.AddIndex("total.cache_size", "-", "int64", IndexOpts().Dense())
		.AddIndex("strings_waiting_to_be_deleted_size", "-", "int64", IndexOpts().Dense())
		.AddIndex("storage_ok", "-", "bool", IndexOpts().Dense())
		.AddIndex("storage_enabled", "-", "bool", IndexOpts().Dense())
		.AddIndex("storage_status", "-", "string", IndexOpts().Dense())
		.AddIndex("storage_path", "-", "string", IndexOpts().Dense())
		.AddIndex("storage_loaded", "-", "bool", IndexOpts().Dense())
		.AddIndex("optimization_completed", "-", "bool", IndexOpts().Dense())
		.AddIndex("query_cache.total_size", "-", "int64", IndexOpts().Dense())
		.AddIndex("query_cache.items_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("query_cache.empty_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("query_cache.hit_count_limit", "-", "int64", IndexOpts().Dense())
		.AddIndex("join_cache.total_size", "-", "int64", IndexOpts().Dense())
		.AddIndex("join_cache.items_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("join_cache.empty_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("join_cache.hit_count_limit", "-", "int64", IndexOpts().Dense()),
	NamespaceDef(kClientsStatsNamespace, StorageOpts())
		.AddIndex("connection_id", "hash", "int", IndexOpts().PK())
		.AddIndex("ip", "-", "string", IndexOpts().Dense())
		.AddIndex("user_name", "-", "string", IndexOpts().Dense())
		.AddIndex("user_rights", "-", "string", IndexOpts().Dense())
		.AddIndex("db_name", "-", "string", IndexOpts().Dense())
		.AddIndex("current_activity", "-", "string", IndexOpts().Dense())
		.AddIndex("start_time", "-", "int64", IndexOpts().Dense())
		.AddIndex("sent_bytes", "-", "int64", IndexOpts().Dense())
		.AddIndex("recv_bytes", "-", "int64", IndexOpts().Dense())
		.AddIndex("send_buf_bytes", "-", "int64", IndexOpts().Dense())
		.AddIndex("pended_updates", "-", "int64", IndexOpts().Dense())
		.AddIndex("send_rate", "-", "int64", IndexOpts().Dense())
		.AddIndex("recv_rate", "-", "int64", IndexOpts().Dense())
		.AddIndex("last_send_ts", "-", "int64", IndexOpts().Dense())
		.AddIndex("last_recv_ts", "-", "int64", IndexOpts().Dense())
		.AddIndex("client_version", "-", "string", IndexOpts().Dense())
		.AddIndex("app_name", "-", "string", IndexOpts().Dense())
		.AddIndex("tx_count", "-", "int64", IndexOpts().Dense())
		.AddIndex("is_subscribed", "-", "bool", IndexOpts().Dense())
		.AddIndex("updates_lost", "-", "int64", IndexOpts().Dense())};

}  // namespace reindexer
