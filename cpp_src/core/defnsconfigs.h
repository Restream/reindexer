#pragma once

#include "namespacedef.h"
#include "system_ns_names.h"

namespace reindexer {

constexpr char kNsNameField[] = "name";

constexpr std::string_view kDefProfilingConfig = R"json({
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
	})json";

constexpr std::string_view kDefNamespacesConfig = R"json({
		"type":"namespaces",
		"namespaces":[
			{
				"namespace":"*",
				"log_level":"none",
				"strict_mode":"names",
				"join_cache_mode":"off",
				"start_copy_policy_tx_size":10000,
				"copy_policy_multiplier":5,
				"tx_size_to_always_copy":100000,
				"tx_vec_insertion_threads":4,
				"optimization_timeout_ms":800,
				"optimization_sort_workers":4,
				"wal_size":4000000,
				"min_preselect_size":1000,
				"max_preselect_size":1000,
				"max_preselect_part":0.1,
				"max_iterations_idset_preresult":20000,
				"index_updates_counting_mode":false,
				"sync_storage_flush_limit":20000,
				"ann_storage_cache_build_timeout_ms": 5000,
				"cache":{
					"index_idset_cache_size":134217728,
					"index_idset_hits_to_cache":2,
					"ft_index_cache_size":134217728,
					"ft_index_hits_to_cache":2,
					"joins_preselect_cache_size":268435456,
					"joins_preselect_hit_to_cache":2,
					"query_count_cache_size":134217728,
					"query_count_hit_to_cache":2
				}
			}
		]
	})json";

constexpr std::string_view kDefReplicationConfig = R"json({
		"type":"replication",
		"replication":{
			"cluster_id":1,
			"server_id":0
		}
	})json";

constexpr std::string_view kDefAsyncReplicationConfig = R"json({
		"type":"async_replication",
		"async_replication":{
			"role": "none",
			"replication_mode": "default",
			"app_name": "rx_repl_leader",
			"sync_threads":4,
			"syncs_per_thread":2,
			"sync_timeout_sec":60,
			"online_updates_timeout_sec":20,
			"enable_compression":true,
			"force_sync_on_logic_error": false,
			"force_sync_on_wrong_data_hash": false,
			"retry_sync_interval_msec":30000,
			"batching_routines_count":100,
			"max_wal_depth_on_force_sync":1000,
			"online_updates_delay_msec":100,
			"self_replication_token": "",
			"log_level":"none",
			"namespaces":[],
			"nodes": []
		}
	})json";

constexpr std::string_view kDefEmbeddersConfig = R"json({
		"type":"embedders",
		"caches":[
			{
				"cache_tag":"*",
				"max_cache_items":1000000,
				"hit_to_cache":1
			}
		]
	})json";

constexpr std::string_view kDefActionConfig = R"json({
		"type":"action",
		"action":{
			"command":""
		}
	})json";

constexpr std::string_view kDefDBConfig[] = {kDefProfilingConfig,		 kDefNamespacesConfig, kDefReplicationConfig,
											 kDefAsyncReplicationConfig, kDefEmbeddersConfig,  kDefActionConfig};

const NamespaceDef kSystemNsDefs[] = {
	NamespaceDef(kConfigNamespace, StorageOpts().Enabled().CreateIfMissing().DropOnFileFormatError())
		.AddIndex("type", "hash", "string", IndexOpts().PK()),
	NamespaceDef(kPerfStatsNamespace, StorageOpts())
		.AddIndex(kNsNameField, "hash", "string", IndexOpts().PK())
		.AddIndex("updates.total_queries_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("updates.total_avg_latency_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("updates.last_sec_qps", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("updates.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("selects.total_queries_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("selects.total_avg_latency_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("selects.last_sec_qps", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("selects.last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("transactions.total_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("transactions.total_copy_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("transactions.avg_steps_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("transactions.avg_prepare_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("transactions.avg_commit_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("transactions.avg_copy_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn()),
	NamespaceDef(kActivityStatsNamespace, StorageOpts())
		.AddIndex("query_id", "hash", "int", IndexOpts().PK())
		.AddIndex("client", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("query", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("query_start", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("blocked", "-", "bool", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("description", "-", "string", IndexOpts().Sparse()),
	NamespaceDef(kQueriesPerfStatsNamespace, StorageOpts())
		.AddIndex("query", "hash", "string", IndexOpts().PK())
		.AddIndex("total_queries_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("total_avg_latency_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("total_avg_lock_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("last_sec_qps", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("last_sec_avg_latency_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("last_sec_avg_lock_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("latency_stddev", "-", "double", IndexOpts().Dense().NoIndexColumn()),
	NamespaceDef(kNamespacesNamespace, StorageOpts()).AddIndex(kNsNameField, "hash", "string", IndexOpts().PK()),
	NamespaceDef(kMemStatsNamespace, StorageOpts())
		.AddIndex(kNsNameField, "hash", "string", IndexOpts().PK())
		.AddIndex("items_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("total.data_size", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("total.indexes_size", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("total.cache_size", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("strings_waiting_to_be_deleted_size", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("storage_ok", "-", "bool", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("storage_enabled", "-", "bool", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("storage_status", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("storage_path", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("optimization_completed", "-", "bool", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("query_cache.total_size", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("query_cache.items_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("query_cache.empty_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("query_cache.hit_count_limit", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("join_cache.total_size", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("join_cache.items_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("join_cache.empty_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("join_cache.hit_count_limit", "-", "int64", IndexOpts().Dense().NoIndexColumn()),
	NamespaceDef(kClientsStatsNamespace, StorageOpts())
		.AddIndex("connection_id", "hash", "int", IndexOpts().PK())
		.AddIndex("ip", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("user_name", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("user_rights", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("db_name", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("current_activity", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("start_time", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("sent_bytes", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("recv_bytes", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("send_buf_bytes", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("send_rate", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("recv_rate", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("last_send_ts", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("last_recv_ts", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("client_version", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("app_name", "-", "string", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("tx_count", "-", "int64", IndexOpts().Dense().NoIndexColumn()),
	NamespaceDef(kReplicationStatsNamespace, StorageOpts())
		.AddIndex("type", "hash", "string", IndexOpts().PK())
		.AddIndex("update_drops", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("pending_updates_count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("pending_updates_size", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("wal_sync.count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("wal_sync.avg_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("wal_sync.max_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("force_sync.count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("force_sync.avg_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("force_sync.max_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("initial_sync.total_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("initial_sync.wal_sync.count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("initial_sync.wal_sync.avg_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("initial_sync.wal_sync.max_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("initial_sync.force_sync.count", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("initial_sync.force_sync.avg_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())
		.AddIndex("initial_sync.force_sync.max_time_us", "-", "int64", IndexOpts().Dense().NoIndexColumn())};

}  // namespace reindexer
