#pragma once

#include "replication_api.h"

class ReplicationLoadApi : public ReplicationApi {
public:
	void InitNs() {
		counter_ = 0;
		auto opt = StorageOpts().Enabled(true).LazyLoad(true);
		opt.noQueryIdleThresholdSec = 10;

		// untill we use shared ptr it will be not destroyed
		auto srv = GetSrv(0);
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
		auto srv = GetSrv(0);
		auto &api = srv->api;

		for (size_t i = 0; i < count; ++i) {
			auto item = api.NewItem("some");
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

			auto item1 = api.NewItem("some1");
			// clang-format off

             err = item1.FromJSON(
                         "{\n"
                         "\"id\":" + std::to_string(counter_)+",\n"
                         "\"int\":" + std::to_string(rand())+",\n"
                         "\"string\":\"" + api.RandString()+"\"\n"
                         "}");
			// clang-format on

			counter_++;

			api.Upsert("some1", item1);
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
		auto srv = GetSrv(0);

		auto &api = srv->api;

		auto err = api.reindexer->Delete(Query("some"), res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	}
	atomic_bool stop;

protected:
	size_t counter_;
};
