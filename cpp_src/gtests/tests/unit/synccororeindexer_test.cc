#include "client/synccororeindexer.h"
#include "client/cororeindexer.h"
#include "coroutine/waitgroup.h"
#include "gtest/gtest.h"
#include "gtests/tests/fixtures/servercontrol.h"
#include "net/ev/ev.h"
#include "tools/fsops.h"

using namespace reindexer;

const int kSyncCoroRxTestMaxIndex = 1000;
static const size_t kSyncCoroRxTestDefaultRpcPort = 8999;
static const size_t kSyncCoroRxTestDefaultHttpPort = 9888;

TEST(SyncCoroRx, BaseTest) {
	// Base test for SyncCoroReindexer client
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRx");
	reindexer::fs::RmDirAll(kTestDbPath);
	// server creation and configuration
	ServerControl server;
	const std::string_view nsName = "ns";
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	// client creation
	reindexer::client::SyncCoroReindexer client;
	Error err = client.Connect("cproto://127.0.0.1:" + std::to_string(kSyncCoroRxTestDefaultRpcPort) + "/db");
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

	const int insRows = 200;
	const string strValue = "aaaaaaaaaaaaaaa";
	for (unsigned i = 0; i < insRows; i++) {
		reindexer::client::Item item = client.NewItem(nsName);
		if (item.Status().ok()) {
			std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"" + strValue + "\"" + R"#(})#";
			err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();
			err = client.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
	reindexer::client::SyncCoroQueryResults qResults(3);
	err = client.Select(std::string("select * from ") + std::string(nsName) + " order by id", qResults);

	unsigned int indx = 0;
	for (auto it = qResults.begin(); it != qResults.end(); ++it, indx++) {
		reindexer::WrSerializer wrser;
		reindexer::Error err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		try {
			gason::JsonParser parser;
			gason::JsonNode json = parser.Parse(wrser.Slice());
			if (json["id"].As<unsigned int>(-1) != indx || json["val"].As<std::string_view>() != strValue) {
				ASSERT_TRUE(false) << "item value not correct";
			}

		} catch (const Error&) {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}
}

TEST(SyncCoroRx, StopServerOnQuery) {
	// Check stop server on fetch results
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/StopServerOnQuery");
	const int kFetchAmount = 100;
	reindexer::fs::RmDirAll(kTestDbPath);
	// server creation and configuration
	ServerControl server;
	const std::string_view nsName = "ns";
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	// client creation
	reindexer::client::CoroReindexerConfig clientConf;
	clientConf.FetchAmount = kFetchAmount;
	reindexer::client::SyncCoroReindexer client(clientConf);
	Error err = client.Connect("cproto://127.0.0.1:" + std::to_string(kSyncCoroRxTestDefaultRpcPort) + "/db");
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

	const int insRows = kFetchAmount * 3;
	const string strValue = "aaaaaaaaaaaaaaa";
	for (unsigned i = 0; i < insRows; i++) {
		reindexer::client::Item item = client.NewItem(nsName);
		if (item.Status().ok()) {
			std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"" + strValue + "\"" + R"#(})#";
			err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();
			err = client.Upsert(nsName, item);
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			ASSERT_TRUE(err.ok()) << err.what();
		}
	}

	reindexer::client::SyncCoroQueryResults qResults(3);
	err = client.Select(std::string("select * from ") + std::string(nsName) + " order by id", qResults);

	unsigned int indx = 0;

	for (auto it = qResults.begin(); it != qResults.end(); ++it, indx++) {
		if (indx < kFetchAmount) {
			ASSERT_TRUE(qResults.Status().ok()) << qResults.Status().code() << " what= " << qResults.Status().what();

			reindexer::WrSerializer wrser;
			reindexer::Error err = it.GetJSON(wrser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			try {
				gason::JsonParser parser;
				gason::JsonNode json = parser.Parse(wrser.Slice());
				if (json["id"].As<unsigned int>(-1) != indx || json["val"].As<std::string_view>() != strValue) {
					ASSERT_TRUE(false) << "item value not correct id = " << json["id"].As<unsigned int>(-1)
									   << " strVal = " << json["val"].As<std::string_view>();
				}

			} catch (const Error&) {
				ASSERT_TRUE(err.ok()) << err.what();
			}
			if (indx == kFetchAmount - 1) {
				ASSERT_TRUE(server.DropAndWaitStop()) << "cannot stop server";
			}
		} else {
			ASSERT_TRUE(!qResults.Status().ok());
		}
	}
}

TEST(SyncCoroRx, TestSyncCoroRx) {
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRx");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	reindexer::client::SyncCoroReindexer client;
	Error err = client.Connect("cproto://127.0.0.1:" + std::to_string(kSyncCoroRxTestDefaultRpcPort) + "/db");
	ASSERT_TRUE(err.ok()) << err.what();
	err = client.OpenNamespace("ns_test");
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex("ns_test", indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client.AddIndex("ns_test", indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	for (unsigned i = 0; i < kSyncCoroRxTestMaxIndex; i++) {
		reindexer::client::Item item = client.NewItem("ns_test");
		if (item.Status().ok()) {
			std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
			err = item.FromJSON(json);
			ASSERT_TRUE(err.ok()) << err.what();
			err = client.Upsert("ns_test", item);
			ASSERT_TRUE(err.ok()) << err.what();
		} else {
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		}
	}
	reindexer::client::SyncCoroQueryResults qResults(3);
	client.Select("select * from ns_test", qResults);

	for (auto i = qResults.begin(); i != qResults.end(); ++i) {
		reindexer::WrSerializer wrser;
		reindexer::Error err = i.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
	}
}

TEST(SyncCoroRx, DISABLED_TestSyncCoroRxNThread) {
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRxNThread");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	reindexer::client::SyncCoroReindexer client;
	client.Connect("cproto://127.0.0.1:" + std::to_string(kSyncCoroRxTestDefaultRpcPort) + "/db");
	client.OpenNamespace("ns_test");
	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	client.AddIndex("ns_test", indDef);

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	client.AddIndex("ns_test", indDef2);

	std::atomic<int> counter(kSyncCoroRxTestMaxIndex);
	auto insertThreadFun = [&client, &counter]() {
		while (true) {
			int c = counter.fetch_add(1);
			if (c < kSyncCoroRxTestMaxIndex * 2) {
				reindexer::client::Item item = client.NewItem("ns_test");
				std::string json = R"#({"id":)#" + std::to_string(c) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				reindexer::Error err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				client.Upsert("ns_test", item);
			} else {
				break;
			}
		}
	};

	std::chrono::system_clock::time_point t1 = std::chrono::system_clock::now();
	std::vector<std::thread> pullThread;
	for (int i = 0; i < 10; i++) {
		pullThread.emplace_back(std::thread(insertThreadFun));
	}
	for (int i = 0; i < 10; i++) {
		pullThread[i].join();
	}
	std::chrono::system_clock::time_point t2 = std::chrono::system_clock::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;
}

TEST(SyncCoroRx, DISABLED_TestCoroRxNCoroutine) {
	// for comparing synchcororeindexer client and single-threaded coro client
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestCoroRxNCoroutine");
	reindexer::fs::RmDirAll(kTestDbPath);

	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	std::chrono::system_clock::time_point t1 = std::chrono::system_clock::now();

	reindexer::net::ev::dynamic_loop loop;
	auto insert = [&loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect("cproto://127.0.0.1:" + std::to_string(kSyncCoroRxTestDefaultRpcPort) + "/db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		rx.OpenNamespace("ns_c");
		reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
		rx.AddIndex("ns_c", indDef);
		reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
		rx.AddIndex("ns_c", indDef2);
		reindexer::coroutine::wait_group wg;

		auto insblok = [&rx, &wg](int from, int count) {
			reindexer::coroutine::wait_group_guard wgg(wg);
			for (int i = from; i < from + count; i++) {
				reindexer::client::Item item = rx.NewItem("ns_c");
				std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				auto err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				rx.Upsert("ns_c", item);
			}
		};

		const unsigned int kcoroCount = 10;
		unsigned int n = kSyncCoroRxTestMaxIndex / kcoroCount;
		wg.add(kcoroCount);
		for (unsigned int k = 0; k < kcoroCount; k++) {
			loop.spawn(std::bind(insblok, n * k, n));
		}
		wg.wait();
	};

	loop.spawn(insert);
	loop.run();
	std::chrono::system_clock::time_point t2 = std::chrono::system_clock::now();
	int dt_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count();
	std::cout << "dt_ms = " << dt_ms << std::endl;
}

TEST(SyncCoroRx, RxClientNThread) {
	reindexer::fs::RmDirAll("/tmp/RxClientNThread");
	const std::string kDbName = "db";
	const std::string kNsName = "ns_test";
	ServerControl server;
	server.InitServer(
		ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, "/tmp/RxClientNThread", kDbName));
	constexpr size_t kConns = 3;
	constexpr size_t kThreads = 10;
	client::CoroReindexerConfig cfg;
	cfg.AppName = "MultiConnRxClient";
	client::SyncCoroReindexer client(cfg, kConns);
	auto err = client.Connect(fmt::sprintf("cproto://127.0.0.1:%d/%s", kSyncCoroRxTestDefaultRpcPort, kDbName));
	ASSERT_TRUE(err.ok()) << err.what();
	err = client.OpenNamespace(kNsName);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex(kNsName, indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef2("index2", "hash", "int", IndexOpts());
	err = client.AddIndex(kNsName, indDef2);
	ASSERT_TRUE(err.ok()) << err.what();

	std::atomic<int> counter(kSyncCoroRxTestMaxIndex);
	auto insertThreadFun = [&client, &counter, &kNsName]() {
		while (true) {
			const int c = counter.fetch_add(1);
			if (c < kSyncCoroRxTestMaxIndex * 2) {
				reindexer::client::Item item = client.NewItem(kNsName);
				const std::string json = R"#({"id":)#" + std::to_string(c) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				reindexer::Error err = item.Unsafe(true).FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				client.Upsert(kNsName, item);
				std::this_thread::yield();
			} else {
				break;
			}
		}
	};

	std::vector<std::thread> threads;
	threads.reserve(kThreads);
	for (size_t i = 0; i < kThreads; i++) {
		threads.emplace_back(std::thread(insertThreadFun));
	}
	for (auto& th : threads) {
		th.join();
	}

	client::SyncCoroQueryResults qr;
	err = client.Select(Query("#clientsstats"), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::WrSerializer wrser;
	size_t resultConnsCount = 0;
	for (auto it = qr.begin(); it != qr.end(); ++it) {
		wrser.Reset();
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		std::string appName = clientsStats["app_name"].As<std::string>();
		if (appName != cfg.AppName) {
			continue;
		}
		++resultConnsCount;
		int64_t sentBytes = clientsStats["sent_bytes"].As<int64_t>();
		EXPECT_GT(sentBytes, 0);
		int64_t recvBytes = clientsStats["recv_bytes"].As<int64_t>();
		EXPECT_GT(recvBytes, 0);
	}
	ASSERT_EQ(resultConnsCount, kConns);
}

TEST(SyncCoroRx, StopWhileWriting) {
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/StopWhileWriting");
	reindexer::fs::RmDirAll(kTestDbPath);
	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	reindexer::client::SyncCoroReindexer client;
	const std::string kNsName = "ns_test";
	constexpr size_t kWritingThreadsCount = 5;
	constexpr auto kWritingThreadSleep = std::chrono::milliseconds(1);
	constexpr auto kSleepBetweenPhases = std::chrono::milliseconds(200);

	enum class State { Running, Stopping, Stopped, TestDone };
	std::atomic<State> state = State::Running;

	Error err = client.Connect("cproto://127.0.0.1:" + std::to_string(kSyncCoroRxTestDefaultRpcPort) + "/db");
	ASSERT_TRUE(err.ok()) << err.what();
	err = client.OpenNamespace(kNsName);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex(kNsName, indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<std::thread> writingThreads;
	for (size_t i = 0; i < kWritingThreadsCount; ++i) {
		writingThreads.emplace_back([&client, &state, &kNsName, kWritingThreadSleep]() noexcept {
			int i = 0;

			auto checkError = [](const Error& err, State beforeOp, State afterOp) noexcept {
				if (beforeOp == afterOp) {
					switch (beforeOp) {
						case State::Running:
							ASSERT_TRUE(err.ok()) << err.what();
							break;
						case State::TestDone:
						case State::Stopping:
							break;
						case State::Stopped:
							ASSERT_EQ(err.code(), errTerminated) << err.what();
							break;
					}
				}
			};

			while (state != State::TestDone) {
				std::this_thread::sleep_for(kWritingThreadSleep);

				auto stateBeforeOp = state.load();
				reindexer::client::Item item = client.NewItem(kNsName);
				auto stateAfterOp = state.load();
				checkError(item.Status(), stateBeforeOp, stateAfterOp);
				if (!item.Status().ok()) continue;

				const std::string json = R"#({"id":)#" + std::to_string(i++) + R"#(, "val":)#" + "\"aaaaaaaaaaaaaaa \"" + R"#(})#";
				stateBeforeOp = state.load();
				if (stateBeforeOp == State::Stopped) {
					continue;
				}
				auto err = item.FromJSON(json);
				stateAfterOp = state.load();
				checkError(err, stateBeforeOp, stateAfterOp);
				if (err.ok()) continue;

				stateBeforeOp = state.load();
				err = client.Upsert(kNsName, item);
				stateAfterOp = state.load();
				checkError(err, stateBeforeOp, stateAfterOp);
				if (err.ok()) continue;
			}
		});
	}

	std::this_thread::sleep_for(kSleepBetweenPhases);
	state = State::Stopping;
	client.Stop();
	state = State::Stopped;
	std::this_thread::sleep_for(kSleepBetweenPhases);
	state = State::TestDone;
	for (auto& th : writingThreads) {
		th.join();
	}
}
