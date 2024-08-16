#pragma once

#include "core/cjson/tagsmatcher.h"
#include "gtests/tools.h"
#include "replication_api.h"
#include "replicator/updatesobserver.h"

class ReplicationLoadApi : public ReplicationApi {
public:
	class UpdatesReciever : public reindexer::IUpdatesObserver {
	public:
		void OnWALUpdate(reindexer::LSNPair, std::string_view nsName, const reindexer::WALRecord&) override final {
			std::lock_guard<std::mutex> lck(mtx_);
			auto found = updatesCounters_.find(nsName);
			if (found != updatesCounters_.end()) {
				++(found.value());
			} else {
				updatesCounters_.emplace(std::string(nsName), 1);
			}
		}
		void OnConnectionState(const Error&) override final {}
		void OnUpdatesLost(std::string_view) override final {}

		using map = tsl::hopscotch_map<std::string, size_t, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;

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
			for (auto& it : counters) {
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
		auto& api = srv->api;

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
		// untill we use shared ptr it will be not destroyed
		auto srv = GetSrv(masterId_);
		auto& api = srv->api;

		reindexer::shared_lock<reindexer::shared_timed_mutex> lk(restartMutex_);

		for (size_t i = 0; i < count; ++i) {
			BaseApi::ItemType item = api.NewItem("some");
			auto json = fmt::sprintf(R"json({"id":"%d", "int":"%d", "string":"%s", "uuid":"%s"})json", counter_, rand(), api.RandString(),
									 randStrUuid());
			Error err = item.Unsafe(true).FromJSON(json);
			++counter_;
			api.Upsert("some", item);

			BaseApi::ItemType item1 = api.NewItem("some1");
			json = fmt::sprintf(R"json({"id":"%d", "int":"%d", "string":"%s", "uuid":"%s"})json", counter_, rand(), api.RandString(),
								randStrUuid());
			err = item1.Unsafe(true).FromJSON(json);
			++counter_;
			api.Upsert("some1", item1);
		}
	}
	BaseApi::QueryResultsType SimpleSelect(size_t num) {
		reindexer::Query qr("some");
		auto srv = GetSrv(num);
		auto& api = srv->api;
		BaseApi::QueryResultsType res(api.reindexer.get());
		auto err = api.reindexer->Select(qr, res);
		EXPECT_TRUE(err.ok()) << err.what();

		return res;
	}
	BaseApi::QueryResultsType DeleteFromMaster() {
		auto srv = GetSrv(masterId_);
		auto& api = srv->api;
		BaseApi::QueryResultsType res(api.reindexer.get());
		auto err = api.reindexer->Delete(reindexer::Query("some"), res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	}
	void RestartWithConfigFile(size_t num, const std::string& configYaml) {
		GetSrv(num)->WriteServerConfig(configYaml);
		StopServer(num);
		StartServer(num);
	}
	void SetServerConfig(size_t num, const ReplicationConfigTest& config) {
		auto srv = GetSrv(num);
		if (num) {
			srv->MakeSlave(config);
		} else {
			srv->MakeMaster(config);
		}
	}
	void CheckSlaveConfigFile(size_t num, const ReplicationConfigTest& config) {
		assertrx(num);
		auto srv = GetSrv(num);
		auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::File);
		EXPECT_TRUE(config == curConfig) << "config:\n" << config.GetJSON() << "\ncurConfig:\n" << curConfig.GetJSON();
	}
	void CheckSlaveConfigNamespace(size_t num, const ReplicationConfigTest& config, std::chrono::seconds awaitTime) {
		assertrx(num);
		auto srv = GetSrv(num);
		for (int i = 0; i < awaitTime.count(); ++i) {
			auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::Namespace);
			if (config == curConfig) {
				return;
			}
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
		auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::Namespace);
		EXPECT_TRUE(config == curConfig) << "config:\n" << config.GetJSON() << "\ncurConfig:\n" << curConfig.GetJSON();
	}
	int32_t ValidateTagsmatchersVersions(std::string_view ns, std::optional<int32_t> minVersion = std::optional<int32_t>()) {
		std::vector<int32_t> versions;
		versions.reserve(GetServersCount());
		for (size_t i = 0; i < GetServersCount(); i++) {
			auto srv = GetSrv(i);
			auto& api = srv->api;
			BaseApi::QueryResultsType res(api.reindexer.get());
			auto err = api.reindexer->Select(reindexer::Query(ns), res);
			EXPECT_TRUE(err.ok()) << err.what();
			versions.emplace_back(res.getTagsMatcher(0).version());
		}
		for (size_t i = 1; i < versions.size(); ++i) {
			if (versions[i] != versions[i - 1]) {
				TestCout() << fmt::sprintf("TagsMatcher versions are different for the '%s':\n", ns);
				for (size_t j = 0; j < versions.size(); ++j) {
					TestCout() << fmt::sprintf("%d: %d\n", j, versions[j]);
				}
				TestCout() << std::endl;
				EXPECT_TRUE(false);
			}
		}
		assertrx(versions.size());
		if (minVersion.has_value()) {
			EXPECT_GT(versions[0], minVersion.value());
		}
		return versions[0];
	}
	void SetSchema(size_t num, std::string_view ns, std::string_view schema) {
		auto srv = GetSrv(num);
		auto err = srv->api.reindexer->SetSchema(ns, schema);
		EXPECT_TRUE(err.ok()) << err.what();
	}
	void ValidateSchemas(std::string_view ns, std::string_view expected) {
		for (size_t i = 0; i < GetServersCount(); i++) {
			auto srv = GetSrv(i);
			std::vector<reindexer::NamespaceDef> nsDefs;
			auto err = srv->api.reindexer->EnumNamespaces(nsDefs, reindexer::EnumNamespacesOpts().WithFilter(ns));
			EXPECT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(nsDefs.size(), 1) << "Namespace does not exist: " << ns;
			EXPECT_EQ(nsDefs[0].name, ns);
			EXPECT_EQ(nsDefs[0].schemaJson, expected);
		}
	}
	std::atomic_bool stop;

protected:
	size_t counter_;
};
