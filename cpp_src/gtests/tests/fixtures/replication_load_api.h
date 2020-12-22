#pragma once

#include "replication_api.h"
#include "replicator/updatesobserver.h"
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
		std::atomic<size_t> completed(0);

		for (size_t i = 0; i < count; ++i) {
			auto item = new typename BaseApi::ItemType(api.NewItem("some"));
			// clang-format off

            Error err = item->FromJSON(
                        "{\n"
                        "\"id\":" + std::to_string(counter_)+",\n"
                        "\"int\":" + std::to_string(rand())+",\n"
                        "\"string\":\"" + api.RandString()+"\"\n"
                        "}");
			// clang-format on

			counter_++;

			api.Upsert("some", *item, [item, &completed](const reindexer::Error &) {
				completed++;
				delete item;
			});

			auto item1 = new typename BaseApi::ItemType(api.NewItem("some1"));
			// clang-format off

						err = item1->FromJSON(
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

			api.Upsert("some1", *item1, [item1, &completed](const reindexer::Error &) {
				completed++;
				delete item1;
			});
		}
		while (completed < count * 2) {	 // -V776
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
		api.Commit("some");
		api.Commit("some1");
	}
	reindexer::client::QueryResults SimpleSelect(size_t num) {
		Query qr = Query("some");
		reindexer::client::QueryResults res;
		auto srv = GetSrv(num);
		auto &api = srv->api;

		auto err = api.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	reindexer::client::QueryResults DeleteFromMaster() {
		reindexer::client::QueryResults res;
		auto srv = GetSrv(masterId_);

		auto &api = srv->api;

		auto err = api.reindexer->Delete(Query("some"), res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	}
	void RestartWithConfigFile(size_t num, const string &configYaml) {
		GetSrv(num)->WriteServerConfig(configYaml);
		StopServer(num);
		StartServer(num);
	}
	void SetServerConfig(size_t num, const ReplicationConfigTest &config) {
		auto srv = GetSrv(num);
		if (num) {
			srv->MakeSlave(0, config);
		} else {
			srv->MakeMaster(config);
		}
	}
	void CheckSlaveConfigFile(size_t num, const ReplicationConfigTest &config) {
		assert(num);
		auto srv = GetSrv(num);
		auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::File);
		EXPECT_TRUE(config == curConfig);
	}
	void CheckSlaveConfigNamespace(size_t num, const ReplicationConfigTest &config, std::chrono::seconds awaitTime) {
		assert(num);
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
	std::atomic_bool stop;

protected:
	size_t counter_;
};
