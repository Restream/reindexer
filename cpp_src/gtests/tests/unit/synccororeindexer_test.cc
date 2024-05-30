#include "client/synccororeindexer.h"
#include "client/cororeindexer.h"
#include "coroutine/waitgroup.h"
#include "gtest/gtest.h"
#include "gtests/tests/fixtures/servercontrol.h"
#include "net/ev/ev.h"
#include "tools/fsops.h"

const int kmaxIndex = 1000;
using namespace reindexer;

TEST(SyncCoroRx, BaseTest) {
	// Base test for SyncCoroReindexer client
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRx");
	reindexer::fs::RmDirAll(kTestDbPath);
	// server creation and configuration
	ServerControl server;
	const std::string_view nsName = "ns";
	server.InitServer(0, 8999, 9888, kTestDbPath, "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	// client creation
	reindexer::client::SyncCoroReindexer client;
	Error err = client.Connect("cproto://127.0.0.1:8999/db");
	ASSERT_TRUE(err.ok()) << err.what();
	// create namespace and indexes
	err = client.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex(nsName, indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client.AddIndex(nsName, indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	// add rows
	const int insRows = 200;
	const std::string strValue = "aaaaaaaaaaaaaaa";
	for (unsigned i = 0; i < insRows; i++) {
		reindexer::client::Item item = client.NewItem(nsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"" + strValue + "\"" + R"#(})#";
		err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		err = client.Upsert(nsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	// select all rows
	reindexer::client::SyncCoroQueryResults qResults(&client, 3);
	err = client.Select(std::string("select * from ") + std::string(nsName) + " order by id", qResults);
	// comparison of inserted data and received from select
	int indx = 0;
	for (auto it = qResults.begin(); it != qResults.end(); ++it, indx++) {
		reindexer::WrSerializer wrser;
		reindexer::Error err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		try {
			gason::JsonParser parser;
			gason::JsonNode json = parser.Parse(wrser.Slice());
			if (json["id"].As<int>(-1) != indx || json["val"].As<std::string_view>() != strValue) {
				ASSERT_TRUE(false) << "item value not correct";
			}

		} catch (const Error&) {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
}

TEST(SyncCoroRx, TestSyncCoroRx) {
	// test for inserting data in one thread
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRx");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	server.InitServer(0, 8999, 9888, kTestDbPath, "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	reindexer::client::SyncCoroReindexer client;
	Error err = client.Connect("cproto://127.0.0.1:8999/db");
	ASSERT_TRUE(err.ok()) << err.what();
	err = client.OpenNamespace("ns_test");
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex("ns_test", indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client.AddIndex("ns_test", indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	for (unsigned i = 0; i < kmaxIndex; i++) {
		reindexer::client::Item item = client.NewItem("ns_test");
		if (item.Status().ok()) {
			std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
			err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();
			err = client.Upsert("ns_test", item);
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	reindexer::client::SyncCoroQueryResults qResults(&client, 3);
	err = client.Select("select * from ns_test", qResults);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto i = qResults.begin(); i != qResults.end(); ++i) {
		reindexer::WrSerializer wrser;
		reindexer::Error err = i.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

TEST(SyncCoroRx, TestSyncCoroRxNThread) {
	// test for inserting data in many thread
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRxNThread");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	server.InitServer(0, 8999, 9888, kTestDbPath, "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);
	reindexer::client::SyncCoroReindexer client;
	auto err = client.Connect("cproto://127.0.0.1:8999/db");
	ASSERT_TRUE(err.ok()) << err.what();
	err = client.OpenNamespace("ns_test");
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex("ns_test", indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client.AddIndex("ns_test", indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	std::atomic<int> counter(kmaxIndex);
	auto insertThreadFun = [&client, &counter]() {
		while (true) {
			int c = counter.fetch_add(1);
			if (c < kmaxIndex * 2) {
				reindexer::client::Item item = client.NewItem("ns_test");
				std::string json = R"#({"id":)#" + std::to_string(c) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				reindexer::Error err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				err = client.Upsert("ns_test", item);
				ASSERT_TRUE(err.ok()) << err.what();
			} else {
				break;
			}
		}
	};

	std::vector<std::thread> poolThread;
	poolThread.reserve(10);
	for (int i = 0; i < 10; i++) {
		poolThread.emplace_back(std::thread(insertThreadFun));
	}
	for (auto& th : poolThread) {
		th.join();
	}
}

TEST(SyncCoroRx, DISABLED_TestCoroRxNCoroutine) {
	// for comparing synchcororeindexer client and single-threaded coro client
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestCoroRxNCoroutine");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	server.InitServer(0, 8999, 9888, kTestDbPath, "db", true);
	ReplicationConfigTest config("master");
	server.Get()->MakeMaster(config);

	system_clock_w::time_point t1 = system_clock_w::now();

	reindexer::net::ev::dynamic_loop loop;
	auto insert = [&loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect("cproto://127.0.0.1:8999/db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.OpenNamespace("ns_c");
		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
		err = rx.AddIndex("ns_c", indDef);
		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
		err = rx.AddIndex("ns_c", indDef2);
		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::coroutine::wait_group wg;

		auto insblok = [&rx, &wg](int from, int count) {
			reindexer::coroutine::wait_group_guard wgg(wg);
			for (int i = from; i < from + count; i++) {
				reindexer::client::Item item = rx.NewItem("ns_c");
				std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				auto err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				err = rx.Upsert("ns_c", item);
				ASSERT_TRUE(err.ok()) << err.what();
			}
		};

		const unsigned int kcoroCount = 10;
		unsigned int n = kmaxIndex / kcoroCount;
		wg.add(kcoroCount);
		for (unsigned int k = 0; k < kcoroCount; k++) {
			loop.spawn(std::bind(insblok, n * k, n));
		}
		wg.wait();
	};

	loop.spawn(insert);
	loop.run();
	system_clock_w::time_point t2 = system_clock_w::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;
}
