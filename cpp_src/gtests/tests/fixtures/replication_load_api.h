#pragma once

#include "cluster/consts.h"
#include "core/cjson/tagsmatcher.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "replication_api.h"

class [[nodiscard]] ReplicationLoadApi : public ReplicationApi {
public:
	void InitNs() {
		counter_ = 0;
		auto opt = StorageOpts().Enabled(true);
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
		SCOPED_TRACE("Fill data " + std::to_string(count));
		// untill we use shared ptr it will be not destroyed
		auto srv = GetSrv(masterId_);
		auto& api = srv->api;

		reindexer::shared_lock<reindexer::shared_timed_mutex> lk(restartMutex_);

		for (size_t i = 0; i < count; ++i) {
			api.UpsertJSON("some", fmt::format(R"json({{"id":{},"int":{},"string":"{}","uuid":"{}"}})json", counter_++, rand(),
											   api.RandString(), randStrUuid()));
			api.UpsertJSON("some1", fmt::format(R"json({{"id":{},"int":{},"string":"{}","uuid":"{}"}})json", counter_++, rand(),
												api.RandString(), randStrUuid()));
		}
	}
	BaseApi::QueryResultsType SimpleSelect(size_t num) {
		SCOPED_TRACE("Selecting some");
		return GetSrv(num)->api.Select(reindexer::Query("some"));
	}
	BaseApi::QueryResultsType DeleteFromMaster() {
		SCOPED_TRACE("Deleting some from master");
		auto srv = GetSrv(masterId_);
		BaseApi::QueryResultsType res;
		srv->api.Delete(reindexer::Query("some"), res);
		return res;
	}
	auto GetReplicationStats(size_t num) { return GetSrv(num)->GetReplicationStats(reindexer::cluster::kAsyncReplStatsType); }
	void SetReplicationLogLevel(size_t num, LogLevel level) {
		SCOPED_TRACE("SetReplicationLogLevel");
		GetSrv(num)->SetReplicationLogLevel(level, "async_replication");
	}
	void ForceSync() {
		SCOPED_TRACE("ForceSync");
		GetSrv(masterId_)->ForceSync();
	}
	void RestartWithReplicationConfigFiles(size_t num, const std::string& asyncReplConfigYaml, const std::string& replConfigYaml) {
		SCOPED_TRACE("RestartWithReplicationConfigFiles");
		GetSrv(num)->WriteAsyncReplicationConfig(asyncReplConfigYaml);
		GetSrv(num)->WriteReplicationConfig(replConfigYaml);
		ASSERT_TRUE(StopServer(num));
		ASSERT_TRUE(StartServer(num));
	}
	void SetServerConfig(size_t num, const AsyncReplicationConfigTest& config) {
		SCOPED_TRACE("SetServerConfig");
		auto srv = GetSrv(num);
		srv->SetReplicationConfig(config);
	}
	void CheckReplicationConfigFile(size_t num, const AsyncReplicationConfigTest& expConfig) {
		SCOPED_TRACE("Checking config from file");
		auto srv = GetSrv(num);
		auto curConfig = srv->GetServerConfig(ServerControl::ConfigType::File);
		EXPECT_EQ(expConfig, curConfig) << "expConfig:\n" << expConfig.GetJSON() << "\ncurConfig:\n" << curConfig.GetJSON();
	}
	void CheckReplicationConfigNamespace(size_t num, const AsyncReplicationConfigTest& expConfig, std::chrono::seconds awaitTime) {
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
	void CheckReplicationConfigNamespace(size_t num, const AsyncReplicationConfigTest& expConfig) {
		SCOPED_TRACE("Checking config from namespace");
		EXPECT_EQ(expConfig, GetSrv(num)->GetServerConfig(ServerControl::ConfigType::Namespace));
	}
	int32_t ValidateTagsmatchersVersions(std::string_view ns, std::optional<int32_t> minVersion = std::optional<int32_t>()) {
		std::vector<int32_t> versions;
		versions.reserve(GetServersCount());
		for (size_t i = 0; i < GetServersCount(); i++) {
			auto srv = GetSrv(i);
			auto& api = srv->api;
			BaseApi::QueryResultsType res;
			auto err = api.reindexer->Select(reindexer::Query(ns), res);
			EXPECT_TRUE(err.ok()) << err.what();
			versions.emplace_back(res.GetTagsMatcher(0).version());
		}
		for (size_t i = 1; i < versions.size(); ++i) {
			if (versions[i] != versions[i - 1]) {
				TestCout() << fmt::format("TagsMatcher versions are different for the '{}':\n", ns);
				for (size_t j = 0; j < versions.size(); ++j) {
					TestCout() << fmt::format("{}: {}\n", j, versions[j]);
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
