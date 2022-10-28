#pragma once

#include "replication_api.h"
#include "replicator/updatesobserver.h"
#include "vendor/hopscotch/hopscotch_map.h"

class ReplicationLoadApi : public ReplicationApi {
public:
	class UpdatesReciever : public IUpdatesObserver {
	public:
		void OnWALUpdate(LSNPair, std::string_view nsName, const WALRecord &) override final {
			std::lock_guard<std::mutex> lck(mtx_);
			auto found = updatesCounters_.find(nsName);
			if (found != updatesCounters_.end()) {
				++(found.value());
			} else {
				updatesCounters_.emplace(std::string(nsName), 1);
			}
		}
		void OnConnectionState(const Error &) override final {}
		void OnUpdatesLost(std::string_view) override final {}

		using map = tsl::hopscotch_map<std::string, size_t, nocase_hash_str, nocase_equal_str>;

		map Counters() const {
			std::lock_guard<std::mutex> lck(mtx_);
			return updatesCounters_;
		}
		void Reset() {
			std::lock_guard<std::mutex> lck(mtx_);
			updatesCounters_.clear();
		}
		void Dump() const {
			std::cerr << "Reciever dump: " << std::endl;
			auto counters = Counters();
			for (auto &it : counters) {
				std::cerr << it.first << ": " << it.second << std::endl;
			}
		}

	private:
		map updatesCounters_;
		mutable std::mutex mtx_;
	};

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
		BaseApi::QueryResultsType res(api.reindexer.get());
		auto err = api.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	BaseApi::QueryResultsType DeleteFromMaster() {
		auto srv = GetSrv(masterId_);
		auto &api = srv->api;
		BaseApi::QueryResultsType res(api.reindexer.get());
		auto err = api.reindexer->Delete(Query("some"), res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	}
	void RestartWithConfigFile(size_t num, const std::string &configYaml) {
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
