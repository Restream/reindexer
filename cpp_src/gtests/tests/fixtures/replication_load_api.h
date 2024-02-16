#pragma once

#include "cluster/stats/replicationstats.h"
#include "gtests/tools.h"
#include "replication_api.h"

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
											   IndexDeclaration{"uuid", "hash", "uuid", IndexOpts(), 0},
										   });
		api.DefineNamespaceDataset("some1", {
												IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
												IndexDeclaration{"int", "tree", "int", IndexOpts(), 0},
												IndexDeclaration{"string", "hash", "string", IndexOpts(), 0},
												IndexDeclaration{"uuid", "hash", "uuid", IndexOpts(), 0},
											});
	}

	void FillData(size_t count) {
		SCOPED_TRACE("Fill data " + std::to_string(count));
		// untill we use shared ptr it will be not destroyed
		auto srv = GetSrv(masterId_);
		auto &api = srv->api;

		shared_lock<shared_timed_mutex> lk(restartMutex_);

		for (size_t i = 0; i < count; ++i) {
			BaseApi::ItemType item = api.NewItem("some");
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			Error err = item.Unsafe().FromJSON(fmt::sprintf(R"json({"id":%d,"int":%d,"string":"%s","uuid":"%s"})json", counter_++, rand(),
															api.RandString(), randStrUuid()));
			ASSERT_TRUE(err.ok()) << err.what();
			api.Upsert("some", item);

			item = api.NewItem("some1");
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			err = item.Unsafe().FromJSON(fmt::sprintf(R"json({"id":%d,"int":%d,"string":"%s","uuid":"%s"})json", counter_++, rand(),
													  api.RandString(), randStrUuid()));
			ASSERT_TRUE(err.ok()) << err.what();
			api.Upsert("some1", item);
		}
	}
	BaseApi::QueryResultsType SimpleSelect(size_t num) {
		SCOPED_TRACE("Selecting some");
		Query qr = Query("some");
		auto srv = GetSrv(num);
		auto &api = srv->api;
		BaseApi::QueryResultsType res;
		auto err = api.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	BaseApi::QueryResultsType DeleteFromMaster() {
		SCOPED_TRACE("Deleting some from master");
		auto srv = GetSrv(masterId_);
		auto &api = srv->api;
		BaseApi::QueryResultsType res;
		auto err = api.reindexer->Delete(Query("some"), res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	}
	cluster::ReplicationStats GetReplicationStats(size_t num) { return GetSrv(num)->GetReplicationStats(cluster::kAsyncReplStatsType); }
	void SetReplicationLogLevel(size_t num, LogLevel level) {
		SCOPED_TRACE("SetReplicationLogLevel");
		GetSrv(num)->SetReplicationLogLevel(level, "async_replication");
	}
	void ForceSync() {
		SCOPED_TRACE("ForceSync");
		GetSrv(masterId_)->ForceSync();
	}
	void RestartWithReplicationConfigFiles(size_t num, const std::string &asyncReplConfigYaml, const std::string &replConfigYaml) {
		SCOPED_TRACE("RestartWithReplicationConfigFiles");
		GetSrv(num)->WriteAsyncReplicationConfig(asyncReplConfigYaml);
		GetSrv(num)->WriteReplicationConfig(replConfigYaml);
		ASSERT_TRUE(StopServer(num));
		ASSERT_TRUE(StartServer(num));
	}
	void SetServerConfig(size_t num, const AsyncReplicationConfigTest &config) {
		SCOPED_TRACE("SetServerConfig");
		auto srv = GetSrv(num);
		srv->SetReplicationConfig(config);
	}
	void CheckReplicationConfigFile(size_t num, const AsyncReplicationConfigTest &expConfig) {
		SCOPED_TRACE("Checking config from file");
		auto srv = GetSrv(num);
		auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::File);
		EXPECT_EQ(expConfig, curConfig) << "expConfig:\n" << expConfig.GetJSON() << "\ncurConfig:\n" << curConfig.GetJSON();
	}
	void CheckReplicationConfigNamespace(size_t num, const AsyncReplicationConfigTest &expConfig, std::chrono::seconds awaitTime) {
		SCOPED_TRACE("Checking config from namespace with timeout");
		auto srv = GetSrv(num);
		for (int i = 0; i < awaitTime.count(); ++i) {
			auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::Namespace);
			if (expConfig == curConfig) {
				return;
			}
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
		auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::Namespace);
		EXPECT_EQ(expConfig, curConfig) << "config:\n" << expConfig.GetJSON() << "\ncurConfig:\n" << curConfig.GetJSON();
	}
	void CheckReplicationConfigNamespace(size_t num, const AsyncReplicationConfigTest &expConfig) {
		SCOPED_TRACE("Checking config from namespace");
		EXPECT_EQ(expConfig, GetSrv(num)->GetServerConfig(ServerControl::ConfigType::Namespace));
	}
	std::atomic_bool stop;

protected:
	size_t counter_;
};
