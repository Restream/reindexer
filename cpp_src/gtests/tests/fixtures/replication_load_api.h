#pragma once

#include "cluster/stats/replicationstats.h"
#include "replication_api.h"
#include "tools/errors.h"
#include "vendor/hopscotch/hopscotch_map.h"

class ReplicationLoadApi : public ReplicationApi {
public:
	void InitNs() {
		counter_ = 0;
		auto opt = StorageOpts().Enabled(true).LazyLoad(true);
		opt.noQueryIdleThresholdSec = 10;

		// untill we use shared ptr it will be not destroyed
		auto srv = GetSrv(masterId_);
		auto &api = srv->api;

		Error err = api.reindexer->OpenNamespace("some", opt);
		ASSERT_TRUE(err.ok()) << err.what();
		err = api.reindexer->OpenNamespace("some1", opt);
		ASSERT_TRUE(err.ok()) << err.what();

		api.DefineNamespaceDataset("some", {
											   IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
											   IndexDeclaration{"int", "tree", "int", IndexOpts(), 0},
											   IndexDeclaration{"string", "hash", "string", IndexOpts(), 0},
										   });
		api.DefineNamespaceDataset("some1", {
												IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
												IndexDeclaration{"int", "tree", "int", IndexOpts(), 0},
												IndexDeclaration{"string", "hash", "string", IndexOpts(), 0},
											});
	}

	void FillData(size_t count) {
		// untill we use shared ptr it will be not destroyed
		auto srv = GetSrv(masterId_);
		auto &api = srv->api;

		shared_lock<shared_timed_mutex> lk(restartMutex_);

		for (size_t i = 0; i < count; ++i) {
			BaseApi::ItemType item = api.NewItem("some");
			// clang-format off

			Error err = item.FromJSON(
                        "{\n"
                        "\"id\":" + std::to_string(counter_)+",\n"
                        "\"int\":" + std::to_string(rand())+",\n"
                        "\"string\":\"" + api.RandString()+"\"\n"
                        "}");
			// clang-format on

			counter_++;

			api.Upsert("some", item);

			BaseApi::ItemType item1 = api.NewItem("some1");
			// clang-format off

						err = item1.FromJSON(
							"{\n"
							"\"id\":" +
							std::to_string(counter_) +
							",\n"
							"\"int\":" +
							std::to_string(rand()) +
							",\n"
							"\"string\":\"" +
							api.RandString() +
							"\"\n"
							"}");
			// clang-format on

			counter_++;

			api.Upsert("some1", item1);
		}
	}
	BaseApi::QueryResultsType SimpleSelect(size_t num) {
		Query qr = Query("some");
		auto srv = GetSrv(num);
		auto &api = srv->api;
		BaseApi::QueryResultsType res;
		auto err = api.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	BaseApi::QueryResultsType DeleteFromMaster() {
		auto srv = GetSrv(masterId_);
		auto &api = srv->api;
		BaseApi::QueryResultsType res;
		auto err = api.reindexer->Delete(Query("some"), res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	}
	cluster::ReplicationStats GetReplicationStats(size_t num, std::string_view type) { return GetSrv(num)->GetReplicationStats(type); }
	void RestartWithReplicationConfigFiles(size_t num, const string &asyncReplConfigYaml, const string &replConfigYaml) {
		GetSrv(num)->WriteAsyncReplicationConfig(asyncReplConfigYaml);
		GetSrv(num)->WriteReplicationConfig(replConfigYaml);
		StopServer(num);
		StartServer(num);
	}
	void SetServerConfig(size_t num, const AsyncReplicationConfigTest &config) {
		auto srv = GetSrv(num);
		srv->SetReplicationConfig(config);
	}
	void CheckReplicationConfigFile(size_t num, const AsyncReplicationConfigTest &config) {
		auto srv = GetSrv(num);
		auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::File);
		EXPECT_TRUE(config == curConfig);
	}
	void CheckReplicationConfigNamespace(size_t num, const AsyncReplicationConfigTest &config, std::chrono::seconds awaitTime) {
		auto srv = GetSrv(num);
		for (int i = 0; i < awaitTime.count(); ++i) {
			auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::Namespace);
			if (config == curConfig) {
				return;
			}
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
		EXPECT_TRUE(config == srv->GetServerConfig(ServerControl::ConfigType::Namespace));
	}
	void CheckReplicationConfigNamespace(size_t num, const AsyncReplicationConfigTest &config) {
		EXPECT_TRUE(config == GetSrv(num)->GetServerConfig(ServerControl::ConfigType::Namespace));
	}
	std::atomic_bool stop;

protected:
	size_t counter_;
};
