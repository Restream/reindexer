#include <fstream>
#include <thread>

#include "core/cjson/jsonbuilder.h"
#include "core/dbconfig.h"
#include "core/keyvalue/variant.h"
#include "core/namespace/namespacestat.h"
#include "core/reindexer.h"
#include "core/system_ns_names.h"
#include "core/type_consts.h"
#include "tools/errors.h"
#include "tools/fsops.h"
#include "tools/serializer.h"
#include "vendor/gason/gason.h"

#include "gtest/gtest.h"

#define __FILENAME__ (strrchr("/" __FILE__, '/') + 1)
#define GTEST_TRACE_SCOPE(SCOPE_DESCRIPTION) testing::ScopedTrace trace(__FILE__, __LINE__, SCOPE_DESCRIPTION)
#define GTEST_TRACE_FUNCTION() GTEST_TRACE_SCOPE(__FUNCTION__)

using namespace reindexer;

class [[nodiscard]] ReplicationConfigTests : public ::testing::Test {
public:
	using ItemType = typename reindexer::Reindexer::ItemT;
	using QueryResultsType = typename reindexer::Reindexer::QueryResultsT;
	enum class [[nodiscard]] ConfigType { File, Namespace };

	const std::chrono::milliseconds kReplicationConfLoadDelay = std::chrono::milliseconds(1200);
	const std::string kSimpleReplConfigStoragePath = fs::JoinPath(fs::GetTempDir(), "reindex/simple_replicationConf_tests/");
	const std::string kStoragePath = kSimpleReplConfigStoragePath;
	const std::string kBuiltin = "builtin://" + kStoragePath;
	const std::string kReplicationConfigFilename = "replication.conf";
	const std::string kReplFilePath = reindexer::fs::JoinPath(kStoragePath, kReplicationConfigFilename);

	// defining test data
	const reindexer::ReplicationConfigData initialReplConf{0, 2, {}};
	const reindexer::ReplicationConfigData correctReplConf{10, 2, {}};
	const reindexer::ReplicationConfigData updatedReplConf{100, 3, {}};

	const reindexer::ReplicationConfigData invalidReplConf{-10, 2, {}};
	const reindexer::ReplicationConfigData invalidReplConf1000{1000, 2, {}};
	const reindexer::ReplicationConfigData fallbackReplConf{0, 2, {}};

	void SetUp() override { std::ignore = fs::RmDirAll(kStoragePath); }
	void TearDown() override {}

	void WriteConfigFile(const std::string& path, const std::string& configYaml) {
		std::ofstream file(path, std::ios_base::trunc);
		file << configYaml;
		file.flush();
	}

	template <typename ConfigT>
	void WriteConfigToFile(const ConfigT& newConf, const std::string& path) {
		WrSerializer ser;
		newConf.GetYAML(ser);

		WriteConfigFile(path, std::string(ser.Slice()));
	}

	bool CheckReplicationConfigData(const Error& errParse, const ReplicationConfigData& actualConfig,
									const ReplicationConfigData& expectedConfig) {
		GTEST_TRACE_FUNCTION();

		EXPECT_EQ(expectedConfig.Validate(), actualConfig.Validate()) << "parse result: " << errParse.what();
		EXPECT_EQ(expectedConfig, actualConfig);
		return expectedConfig == actualConfig;
	}

	bool CheckReplicationConfigFile(const std::string& storagePath, const ReplicationConfigData& expectedConf,
									bool expectErrorParseYAML = false) {
		GTEST_TRACE_FUNCTION();

		std::string replConfYaml;
		auto read = fs::ReadFile(fs::JoinPath(storagePath, kReplicationConfigFilename), replConfYaml);
		EXPECT_GT(read, 0) << "Repl config file read error";
		if (read < 0) {
			return false;
		}
		ReplicationConfigData replConf;
		auto errParse = replConf.FromYAML(replConfYaml);

		if (expectErrorParseYAML) {
			EXPECT_EQ(errParse.code(), errParseYAML) << errParse.what();
			return errParse.code() == errParseYAML;
		} else {
			return CheckReplicationConfigData(errParse, replConf, expectedConf);
		}
	}

	bool CheckReplicationConfigNS(reindexer::Reindexer& rx, const ReplicationConfigData& expectedConf, bool expectErrorParseJSON = false) {
		GTEST_TRACE_FUNCTION();
		QueryResultsType results;
		auto err = rx.Select(Query(kConfigNamespace).Where("type", CondEq, "replication"), results);
		EXPECT_TRUE(err.ok()) << err.what();

		ReplicationConfigData replConf;
		Error errParse{errNotFound, "Not found"};
		for (auto it : results) {
			WrSerializer ser;

			err = it.GetJSON(ser, false);
			EXPECT_TRUE(err.ok()) << err.what();
			try {
				gason::JsonParser parser;
				gason::JsonNode configJson = parser.Parse(ser.Slice());
				auto confType = configJson["type"].As<std::string_view>();
				if (confType == "replication") {
					auto& replConfigJson = configJson["replication"];
					errParse = replConf.FromJSON(replConfigJson);
					break;
				}
			} catch (const Error&) {
				assert(false);
			}
		}

		if (expectErrorParseJSON) {
			EXPECT_EQ(errParse.code(), errParseJson) << errParse.what();
			return errParse.code() == errParseJson;
		} else {
			return CheckReplicationConfigData(errParse, replConf, expectedConf);
		}
	}

	bool CheckNamespacesReplicationConfig(reindexer::Reindexer& rx, const ReplicationConfigData& expected) {
		GTEST_TRACE_FUNCTION();
		{
			GTEST_TRACE_SCOPE("Checking #memstats.server_id for non-system namespaces.");
			QueryResultsType results;
			auto query = "select name, replication.server_id from #memstats";
			auto err = rx.ExecSQL(query, results);
			EXPECT_TRUE(err.ok()) << err.what();
			for (auto it : results) {
				WrSerializer ser;
				err = it.GetJSON(ser, false);
				EXPECT_TRUE(err.ok()) << err.what();
				try {
					gason::JsonParser parser;
					gason::JsonNode memstatJson = parser.Parse(ser.Slice());
					auto nsName = memstatJson["name"].As<std::string_view>();
					auto& replJson = memstatJson["replication"];
					auto namespaceServerId = replJson["server_id"].As<int>(-1);
					EXPECT_EQ(namespaceServerId, expected.serverID) << "Failed for Namespace \"" << nsName << "\"";
				} catch (const Error& error) {
					EXPECT_TRUE(error.ok()) << error.what();
				}
			}
		}

		{
			GTEST_TRACE_SCOPE("Checking ReplState.nsVersion.server_id for system namespaces");
			std::vector<reindexer::NamespaceDef> nsDefs;
			auto err = rx.EnumNamespaces(nsDefs, reindexer::EnumNamespacesOpts().OnlyNames());
			EXPECT_TRUE(err.ok()) << err.what();
			for (auto& nsDef : nsDefs) {
				if (nsDef.name.empty() || (nsDef.name[0] != '#')) {
					// we will perform checks only for well-defined system namespaces
					continue;
				}
				reindexer::ReplicationStateV2 replState;
				auto error = rx.GetReplState(nsDef.name, replState);
				EXPECT_TRUE(error.ok()) << error.what();
				EXPECT_EQ(replState.nsVersion.Server(), expected.serverID) << "Check failed for NS \"" << nsDef.name << "\"";
			}
		}
		return true;
	}

	template <bool ExpectErrorOnUpsert = false>
	void SetReplicationConfigNS(reindexer::Reindexer& rx, const ReplicationConfigData& config) {
		GTEST_TRACE_FUNCTION();
		upsertConfigItemFromObject<decltype(config), ExpectErrorOnUpsert>(rx, "replication", config);
	}

	template <bool ExpectErrorOnUpsert = false>
	void SetJSONtoConfigNS(reindexer::Reindexer& rx, std::string_view stringJSON) {
		GTEST_TRACE_FUNCTION();
		upsertConfigItemFromJSON<ExpectErrorOnUpsert>(rx, stringJSON);
	}

protected:
	template <typename ValueT, bool ExpectErrorOnUpsert = false>
	void upsertConfigItemFromObject(reindexer::Reindexer& rx, std::string_view type, const ValueT& object) {
		GTEST_TRACE_FUNCTION();

		WrSerializer ser;
		{
			JsonBuilder jb(ser);
			jb.Put("type", type);
			{
				auto objBuilder = jb.Object(type);
				object.GetJSON(objBuilder);
			}
		}

		upsertConfigItemFromJSON<ExpectErrorOnUpsert>(rx, ser.Slice());
	}

	template <bool ExpectErrorOnUpsert = false>
	void upsertConfigItemFromJSON(reindexer::Reindexer& rx, const std::string_view stringJSON) {
		GTEST_TRACE_FUNCTION();

		auto item = rx.NewItem(kConfigNamespace);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON(stringJSON);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.Upsert(kConfigNamespace, item);
		if constexpr (ExpectErrorOnUpsert) {
			ASSERT_FALSE(err.ok()) << err.what();
		} else {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
};

TEST(DBConfigTests, ReadValidJsonConfiguration) {
	DBConfigProvider db;

	constexpr std::string_view jsonStr = R"json({
		"profiling":{
			"queriesperfstats": false,
			"queries_threshold_us": 10,
			"perfstats": false,
			"memstats": true,
			"activitystats": false,
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
		},
		"namespaces":[
			{
				"namespace":"*",
				"log_level":"none",
				"join_cache_mode":"off",
				"start_copy_policy_tx_size":10000,
				"copy_policy_multiplier":5,
				"tx_size_to_always_copy":100000,
				"optimization_timeout_ms":800,
				"optimization_sort_workers":4,
				"wal_size":4000000,
				"min_preselect_size": 1000,
				"max_preselect_size": 1000,
				"max_preselect_part":0.1,
				"index_updates_counting_mode":false,
				"sync_storage_flush_limit":25000
			}
		],
		"replication":{
			"server_id": 10,
			"cluster_id": 11,
		},
		"async_replication":{
			"role": "none",
			"log_level":"none",
			"sync_threads":4,
			"syncs_per_thread":2,
			"online_updates_timeout_sec":20,
			"sync_timeout_sec":60,
			"retry_sync_interval_msec":30000,
			"enable_compression":true,
			"batching_routines_count": 100,
			"force_sync_on_logic_error": false,
			"force_sync_on_wrong_data_hash": false,
			"max_wal_depth_on_force_sync": 1000,
			"namespaces":[]
			"nodes": []
		}
	})json";

	gason::JsonParser parser;
	gason::JsonNode configJson = parser.Parse(jsonStr);

	auto err = db.FromJSON(configJson);
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST(DBConfigTests, ReadInvalidJsonConfiguration) {
	DBConfigProvider db;

	constexpr std::string_view jsonStr = R"json({
		"profiling":{
			"queriesperfstats":"false_",
			"queries_threshold_us":"10_",
			"perfstats": "false_",
			"memstats": "true_",
			"activitystats": "false_",
			"long_queries_logging":{
				"select":{
					"threshold_us": "-1_",
					"normalized": "_false"
				},
				"update_delete":{
					"threshold_us": "-1_",
					"normalized": "+false"
				},
				"transaction":{
					"threshold_us": "-1_",
					"avg_step_threshold_us": "test -1"
				}
			}
		},
		"namespaces":[
			{
				"namespace":"*",
				"log_level":"none",
				"join_cache_mode":"off",
				"start_copy_policy_tx_size":10000,
				"copy_policy_multiplier":5,
				"tx_size_to_always_copy":100000,
				"optimization_timeout_ms":800,
				"optimization_sort_workers":"4_",
				"wal_size":4000000,
				"min_preselect_size":1000,
				"max_preselect_size":"1000_",
				"max_preselect_part":0.1,
				"index_updates_counting_mode":false,
				"sync_storage_flush_limit":25000
			}
		],
		"replication":{
			"server_id": "0test",
			"cluster_id": "1test",
		},
		"async_replication":{
			"role": "none",
			"sync_threads":4,
			"syncs_per_thread":2,
			"online_updates_timeout_sec":20,
			"sync_timeout_sec":60,
			"retry_sync_interval_msec":30000,
			"enable_compression":true,
			"batching_routines_count":"100test",
			"force_sync_on_logic_error": false,
			"force_sync_on_wrong_data_hash": false,
			"max_wal_depth_on_force_sync":1000,
			"namespaces":[]
			"nodes": []
		}
	})json";

	gason::JsonParser parser;
	gason::JsonNode configJson = parser.Parse(jsonStr);

	auto err = db.FromJSON(configJson);
	EXPECT_FALSE(err.ok()) << err.what();
}

/** @brief Tests replication.conf readings at db load or startup
 *	@details Test plan:
 *	Continuous tests set, after step 0 working with existing db folder
 *	0. Warm up: create and initialize new DB in kStoragePath, close db
 *	1. Write correct replication.conf, connect to DB, verify readings - shall success
 *	2. Write invalid replication.conf, connect to DB - shall fail with error.code() = errParams
 *	3. Write invalid1000 replication.conf, connect to DB - shall fail with error.code() = errParams
 *	4. Write invalid replication.conf with non-numeric values, connect to DB - shall fail with error error.code() = errParseYAML
 *	5. Write correct updated replication.conf, connect to DB, verify readings - shall success
 */
TEST_F(ReplicationConfigTests, ReadReplicationConfAtStartup) {
	// 0. Warm up:
	{
		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// 1. For existing DB: Start with correct replication.conf and connect. CHECK readings.
	{
		GTEST_TRACE_SCOPE("Starting with correct replication.conf server_id = " + std::to_string(correctReplConf.serverID));

		WriteConfigToFile(correctReplConf, kReplFilePath);
		CheckReplicationConfigFile(kStoragePath, correctReplConf);

		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();
		CheckReplicationConfigNS(rt, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
		CheckReplicationConfigFile(kStoragePath, correctReplConf);
	}

	// 2. Write invalid replication.conf, connect to DB - shall fail with error.code() = errParams
	{
		GTEST_TRACE_SCOPE("Starting with invalid replication.conf server_id = " + std::to_string(invalidReplConf.serverID));
		WriteConfigToFile(invalidReplConf, kReplFilePath);
		CheckReplicationConfigFile(kStoragePath, invalidReplConf);

		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_EQ(err.code(), errParams) << err.what();
	}

	// 3. Write invalid1000 replication.conf, connect to DB - shall fail with error.code() = errParams
	{
		GTEST_TRACE_SCOPE("Starting with invalid replication.conf server_id = " + std::to_string(invalidReplConf1000.serverID));
		WriteConfigToFile(invalidReplConf1000, kReplFilePath);

		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_EQ(err.code(), errParams) << err.what();
	}

	// 4. Write invalid replication.conf with non-numeric values, connect to DB - shall fail with error error.code() = errParseYAML
	{
		GTEST_TRACE_SCOPE("Starting with invalid replication.server_id = \"invalid\"");
		WriteConfigFile(kReplFilePath,
						"server_id: invalid\n"
						"cluster_id: invalid\n");

		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_EQ(err.code(), errParseYAML) << err.what();
	}

	// 5. Write correct updated replication.conf, connect to DB, verify readings - shall success
	{
		GTEST_TRACE_SCOPE("Starting with updated replication.conf server_id = " + std::to_string(updatedReplConf.serverID));
		WriteConfigToFile(updatedReplConf, kReplFilePath);
		CheckReplicationConfigFile(kStoragePath, updatedReplConf);

		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();
		CheckReplicationConfigNS(rt, updatedReplConf);
		CheckNamespacesReplicationConfig(rt, updatedReplConf);
		CheckReplicationConfigFile(kStoragePath, updatedReplConf);
	}
}

/** @brief Tests invalid server_id in #config.replication and replication.conf when replication.conf always present
 *  @details Test plan:
 *  Continuous tests set
 *	0. Warm up: create and initialize new DB in kStoragePath, close
 *	1. Write correct replication.conf from initialReplConf, CONNECT, verify - shall success
 *	All cases from 2 shall be done on existing db connection without restarts
 *	2. Write correct replication.conf from correctReplConf, CHECK readings - internal state and replication.conf shall be changed
 *	3. Write invalid replication.conf from invalidReplConf, CHECK readings - internal state and replication.conf shall NOT be changed
 *	4. Write invalid replication.conf from invalidReplConf1000, CHECK readings - internal state and replication.conf shall NOT be changed
 *	5. Write invalid replication.conf with non-numeric values, CHECK readings - internal state and replication.conf shall NOT be changed
 */
TEST_F(ReplicationConfigTests, ReplicationConfChangedAtRuntime) {
	// 0. Warm up: create and initialize new DB in kStoragePath
	{
		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	// 1. Write correct replication.conf from initialReplConf, CONNECT, verify - shall success
	reindexer::Reindexer rt;
	{
		GTEST_TRACE_SCOPE("Starting with replication.conf server_id = " + std::to_string(initialReplConf.serverID));
		WriteConfigToFile(initialReplConf, kReplFilePath);
		CheckReplicationConfigFile(kStoragePath, initialReplConf);
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();
		CheckReplicationConfigNS(rt, initialReplConf);
		CheckNamespacesReplicationConfig(rt, initialReplConf);
	}

	// All cases from 2 shall be done on existing db connection without restarts
	// 2. Write correct replication.conf from correctReplConf,
	{
		GTEST_TRACE_SCOPE("Writing correct replication.conf server_id = " + std::to_string(correctReplConf.serverID));
		WriteConfigToFile(correctReplConf, kReplFilePath);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, correctReplConf);
		CheckReplicationConfigNS(rt, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}

	// 3. Write invalid replication.conf from invalidReplConf,
	{
		GTEST_TRACE_SCOPE("Writing invalid replication.conf server_id = " + std::to_string(invalidReplConf.serverID));
		WriteConfigToFile(invalidReplConf, kReplFilePath);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, invalidReplConf);
		CheckReplicationConfigNS(rt, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}

	// 4. Write invalid replication.conf from invalidReplConf1000, CHECK readings - internal state and replication.conf shall NOT be
	// changed
	{
		GTEST_TRACE_SCOPE("Writing invalid replication.conf server_id = " + std::to_string(invalidReplConf1000.serverID));
		WriteConfigToFile(invalidReplConf1000, kReplFilePath);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, invalidReplConf1000);
		CheckReplicationConfigNS(rt, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}

	// 5. Write invalid replication.conf with non-numeric values, CHECK readings - internal state and replication.conf shall NOT be changed
	{
		GTEST_TRACE_SCOPE("Writing invalid replication.conf server_id = \"invalid\"");
		const bool kExpectErrorParseYAML = true;
		WriteConfigFile(kReplFilePath,
						"server_id: invalid\n"
						"cluster_id: invalid\n");
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, invalidReplConf, kExpectErrorParseYAML);
		CheckReplicationConfigNS(rt, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}
}

/** @brief Tests updates to server_id in #config.replication when replication.conf always present
 *	@details Test plan:
 *	Continuous tests set
 *	0. Warm up: create and initialize new DB in kStoragePath, close
 *	All cases from 1 shall be done on existing db connection without restarts
 *	1. Write correct replication.conf from initialReplConf, CHECK readings
 *		- internal state and replication.conf shall be changed
 *	2. Set correct replication data to #config from correctReplConf, CHECK readings
 *		- internal state and replication.conf shall be changed
 *	3. Set invalid replication data to #config from invalidReplConf, CHECK readings
 *		- internal state and replication.conf shall NOT be changed
 *	4. Set invalid replication data to #config from invalidReplConf1000, CHECK readings
 *		- internal state and replication.conf shall NOT be changed
 *	5. Set invalid replication data to #config with non-numeric values, CHECK readings
 *		- internal state and replication.conf shall NOT be changed
 */
TEST_F(ReplicationConfigTests, SetServerIdToConfigWithReplicationConf) {
	const bool kExpectErrorOnUpsert = true;
	reindexer::Reindexer rt;
	Error err = rt.Connect(kBuiltin);
	ASSERT_TRUE(err.ok()) << err.what();

	// 1. Write correct replication.conf from initialReplConf, CHECK readings
	//	- internal state and replication.conf shall be changed
	{
		GTEST_TRACE_SCOPE("Writing replication.conf server_id = " + std::to_string(initialReplConf.serverID));
		WriteConfigToFile(initialReplConf, kReplFilePath);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigNS(rt, initialReplConf);
		CheckReplicationConfigFile(kStoragePath, initialReplConf);
		CheckNamespacesReplicationConfig(rt, initialReplConf);
	}

	// 2. Set correct replication data to #config from correctReplConf, CHECK readings
	//	- internal state and replication.conf shall be changed
	{
		GTEST_TRACE_SCOPE("Setting replication.server_id = " + std::to_string(correctReplConf.serverID) + " to #config");
		SetReplicationConfigNS(rt, correctReplConf);
		CheckReplicationConfigNS(rt, correctReplConf);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}

	// 3. Set invalid replication data to #config from invalidReplConf, CHECK readings
	//	- internal state and replication.conf shall NOT be changed
	{
		GTEST_TRACE_SCOPE("Setting invalid replication.server_id = " + std::to_string(invalidReplConf.serverID) + " to #config");
		SetReplicationConfigNS<kExpectErrorOnUpsert>(rt, invalidReplConf);
		CheckReplicationConfigNS(rt, invalidReplConf);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}

	// 4. Set invalid replication data to #config from invalidReplConf1000, CHECK readings
	//	- internal state and replication.conf shall NOT be changed
	{
		GTEST_TRACE_SCOPE("Setting invalid replication.server_id = " + std::to_string(invalidReplConf1000.serverID) + " to #config");
		SetReplicationConfigNS<kExpectErrorOnUpsert>(rt, invalidReplConf1000);
		CheckReplicationConfigNS(rt, invalidReplConf1000);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}

	// 5. Set invalid replication data to #config with non-numeric values, CHECK readings
	//	- internal state and replication.conf shall NOT be changed
	{
		using namespace std::string_view_literals;
		GTEST_TRACE_SCOPE("Setting invalid replication.server_id = \"invalid\" to #config");
		SetJSONtoConfigNS<kExpectErrorOnUpsert>(rt,
												R"json({
													"type":"replication",
													"replication":{
														"server_id":"invalid",
														"cluster_id":"invalid",
													}
												})json"sv);
		const bool kExpectErrorParseJson = true;
		CheckReplicationConfigNS(rt, invalidReplConf1000, kExpectErrorParseJson);
		std::this_thread::sleep_for(kReplicationConfLoadDelay);
		CheckReplicationConfigFile(kStoragePath, correctReplConf);
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}
}

/** @brief Checks invalid server_id in #config when replication.conf does not exist
 *	@details Test plan:
 *	Continuous tests set
 *	1. Start with default db, set correct server_id, set invalid server_id, shutdown
 *	All cases from 2 shall be done on existing db
 *	2. Start with invalid server_id, check fallback server_id present, set another invalid server_id, shutdown
 *	3. Start with invalid server_id, check fallback server_id present, restore correct server_id, shutdown
 *	4. Start with correct sever_id, verify
 */
TEST_F(ReplicationConfigTests, SetServerIDToConfigRestartWithoutReplicationConf) {
	const bool kExpectErrorOnUpsert = true;

	// 1. Start with default db, set correct server_id, set invalid server_id, shutdown
	{
		reindexer::Reindexer rt;
		{
			GTEST_TRACE_SCOPE("Setting correct replication.server_id = " + std::to_string(correctReplConf.serverID) + " to #config");
			Error err = rt.Connect(kBuiltin);
			ASSERT_TRUE(err.ok()) << err.what();

			SetReplicationConfigNS(rt, correctReplConf);
			CheckReplicationConfigNS(rt, correctReplConf);
			std::this_thread::sleep_for(kReplicationConfLoadDelay);
			ASSERT_EQ(fs::Stat(kReplFilePath), fs::StatError) << "replication.conf shall not exist when present.";
			CheckNamespacesReplicationConfig(rt, correctReplConf);
		}
		{
			GTEST_TRACE_SCOPE("Setting invalid replication.server_id = " + std::to_string(invalidReplConf.serverID) + " to #config");

			SetReplicationConfigNS<kExpectErrorOnUpsert>(rt, invalidReplConf);
			CheckReplicationConfigNS(rt, invalidReplConf);
			std::this_thread::sleep_for(kReplicationConfLoadDelay);
			ASSERT_EQ(fs::Stat(kReplFilePath), fs::StatError) << "replication.conf shall not exist when present.";
			CheckNamespacesReplicationConfig(rt, correctReplConf);
		}
	}

	// 2. Start with invalid server_id, check fallback server_id present, set another invalid server_id, shutdown
	{
		reindexer::Reindexer rt;
		{
			GTEST_TRACE_SCOPE("Reloading with invalid replication.server_id = " + std::to_string(invalidReplConf.serverID) + " to #config");
			ASSERT_TRUE(fs::Stat(kReplFilePath) == fs::StatError) << "replication.conf shall not exist when present.";

			Error err = rt.Connect(kBuiltin);
			ASSERT_TRUE(err.ok()) << err.what();
			CheckReplicationConfigNS(rt, invalidReplConf);
			CheckNamespacesReplicationConfig(rt, fallbackReplConf);
		}

		{
			GTEST_TRACE_SCOPE("Setting invalid replication.server_id = " + std::to_string(invalidReplConf1000.serverID) + " to #config");
			SetReplicationConfigNS<kExpectErrorOnUpsert>(rt, invalidReplConf1000);
			std::this_thread::sleep_for(kReplicationConfLoadDelay);
			CheckReplicationConfigNS(rt, invalidReplConf1000);
			ASSERT_EQ(fs::Stat(kReplFilePath), fs::StatError) << "replication.conf shall not exist when present.";
			CheckNamespacesReplicationConfig(rt, fallbackReplConf);
		}
	}

	// 3. Start with invalid server_id, check fallback server_id present, restore correct server_id, shutdown
	{
		reindexer::Reindexer rt;
		{
			GTEST_TRACE_SCOPE("Reloading with invalid replication.server_id = " + std::to_string(invalidReplConf1000.serverID) +
							  " in #config");
			ASSERT_TRUE(fs::Stat(kReplFilePath) == fs::StatError) << "replication.conf shall not exist when present.";

			Error err = rt.Connect(kBuiltin);
			ASSERT_TRUE(err.ok()) << err.what();
			CheckReplicationConfigNS(rt, invalidReplConf1000);
			ASSERT_EQ(fs::Stat(kReplFilePath), fs::StatError) << "replication.conf shall not exist when present.";
			CheckNamespacesReplicationConfig(rt, fallbackReplConf);
		}

		{
			GTEST_TRACE_SCOPE("Setting correct replication.server_id = " + std::to_string(correctReplConf.serverID) + " to #config");
			SetReplicationConfigNS(rt, correctReplConf);
			CheckReplicationConfigNS(rt, correctReplConf);
			ASSERT_EQ(fs::Stat(kReplFilePath), fs::StatError) << "replication.conf shall not exist when present.";
			CheckNamespacesReplicationConfig(rt, correctReplConf);
		}
	}

	// 4. Start with correct sever_id, verify
	{
		GTEST_TRACE_SCOPE("Reloading with correct replication.server_id = " + std::to_string(correctReplConf.serverID) + " in #config");
		ASSERT_EQ(fs::Stat(kReplFilePath), fs::StatError) << "replication.conf shall not exist when present.";

		reindexer::Reindexer rt;
		Error err = rt.Connect(kBuiltin);
		ASSERT_TRUE(err.ok()) << err.what();
		CheckReplicationConfigNS(rt, correctReplConf);
		ASSERT_EQ(fs::Stat(kReplFilePath), fs::StatError) << "replication.conf shall not exist when present.";
		CheckNamespacesReplicationConfig(rt, correctReplConf);
	}
}
