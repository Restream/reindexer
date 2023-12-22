#include <condition_variable>
#include "client/cororeindexer.h"
#include "client/reindexer.h"
#include "coroutine/waitgroup.h"
#include "gtest/gtest.h"
#include "gtests/tests/fixtures/servercontrol.h"
#include "net/ev/ev.h"
#include "tools/fsops.h"

using namespace reindexer;

const int kSyncCoroRxTestMaxIndex = 1000;
static const size_t kSyncCoroRxTestDefaultRpcPort = 8999;
static const size_t kSyncCoroRxTestDefaultHttpPort = 9888;

struct SyncCoroRxHelpers {
	static const std::string kStrValue;

	template <typename RxT>
	static void FillData(RxT& client, std::string_view ns, unsigned rows, unsigned from = 0) {
		for (unsigned i = from; i < from + rows; i++) {
			reindexer::client::Item item = client.NewItem(ns);
			if (item.Status().ok()) {
				std::string json = R"#({"id":)#" + std::to_string(i) + R"#(, "val":)#" + "\"" + kStrValue + "\"" + R"#(})#";
				auto err = item.FromJSON(json);
				ASSERT_TRUE(err.ok()) << err.what();
				err = client.Upsert(ns, item);
				ASSERT_TRUE(err.ok()) << err.what();
			} else {
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			}
		}
	}
};

const std::string SyncCoroRxHelpers::kStrValue = "aaaaaaaaaaaaaaa";

TEST(SyncCoroRx, BaseTest) {
	// Base test for Reindexer client
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TestSyncCoroRx");
	reindexer::fs::RmDirAll(kTestDbPath);
	// server creation and configuration
	ServerControl server;
	const std::string_view nsName = "ns";
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	// client creation
	reindexer::client::Reindexer client;
	Error err = client.Connect("cproto://127.0.0.1:" + std::to_string(kSyncCoroRxTestDefaultRpcPort) + "/db");
	ASSERT_TRUE(err.ok()) << err.what();
	// create namespace and indexes
	err = client.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client.AddIndex(nsName, indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	const int insRows = 200;
	SyncCoroRxHelpers::FillData(client, nsName, insRows);
	reindexer::client::QueryResults qResults(3);
	err = client.Select(std::string("select * from ") + std::string(nsName) + " order by id", qResults);

	int indx = 0;
	for (auto it = qResults.begin(); it != qResults.end(); ++it, indx++) {
		reindexer::WrSerializer wrser;
		reindexer::Error err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		try {
			gason::JsonParser parser;
			gason::JsonNode json = parser.Parse(wrser.Slice());
			if (json["id"].As<int>(-1) != indx || json["val"].As<std::string_view>() != SyncCoroRxHelpers::kStrValue) {
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
	constexpr uint32_t kConnsCount = 2;
	constexpr uint32_t kThreadsCount = 2;
	reindexer::fs::RmDirAll(kTestDbPath);
	// server creation and configuration
	ServerControl server;
	const std::string_view nsName = "ns";
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	// client creation
	reindexer::client::ReindexerConfig clientConf;
	clientConf.FetchAmount = kFetchAmount;
	reindexer::client::Reindexer client(clientConf, kConnsCount, kThreadsCount);
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
	SyncCoroRxHelpers::FillData(client, nsName, insRows);

	reindexer::client::QueryResults qResults(3);
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
				if (json["id"].As<unsigned int>(-1) != indx || json["val"].As<std::string_view>() != SyncCoroRxHelpers::kStrValue) {
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
	reindexer::client::Reindexer client;
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

	SyncCoroRxHelpers::FillData(client, "ns_test", kSyncCoroRxTestMaxIndex);
	reindexer::client::QueryResults qResults(3);
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
	reindexer::client::Reindexer client;
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
	std::vector<std::thread> poolThread;
	poolThread.reserve(10);
	for (int i = 0; i < 10; i++) {
		poolThread.emplace_back(insertThreadFun);
	}
	for (auto& th : poolThread) {
		th.join();
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
			SyncCoroRxHelpers::FillData(rx, "ns_c", count, from);
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
	const auto kStoragePath = fs::JoinPath(fs::GetTempDir(), "reindex/RxClientNThread");
	reindexer::fs::RmDirAll(kStoragePath);
	const std::string kDbName = "db";
	const std::string kNsName = "ns_test";
	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kStoragePath, kDbName));
	constexpr size_t kConns = 3;
	constexpr size_t kThreads = 10;
	const size_t kSyncCoroRxThreads = 2;
	client::ReindexerConfig cfg;
	cfg.AppName = "MultiConnRxClient";
	client::Reindexer client(cfg, kConns, kSyncCoroRxThreads);
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

	client::QueryResults qr;
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
	const std::string kNsName = "ns_test";
	constexpr size_t kWritingThreadsCount = 5;
	constexpr uint32_t kConnsCount = 4;
	constexpr uint32_t kThreadsCount = 2;
	reindexer::client::Reindexer client(client::ReindexerConfig(), kConnsCount, kThreadsCount);
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

TEST(SyncCoroRx, AsyncCompletions) {
	// Check if async completions are actually asynchronous
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/AsyncCompletions");
	reindexer::fs::RmDirAll(kTestDbPath);
	const std::string kNsNames = "ns_test";
	std::condition_variable cv;
	std::mutex mtx;
	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	auto client = server.Get()->api.reindexer;
	Error err = client->OpenNamespace(kNsNames);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client->AddIndex(kNsNames, indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	size_t counter = 0;
	constexpr size_t kItemsCount = 10;
	std::atomic<bool> done = false;
	std::vector<client::Item> items(kItemsCount);
	for (auto& item : items) {
		item = client->NewItem(kNsNames);  // NewItem can not be created async
	}
	for (size_t i = 0; i < kItemsCount; ++i) {
		const std::string json = fmt::sprintf(R"#({"id": %d, "val": "aaaaaaaa"})#", i);
		auto err = items[i].FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		err = client
				  ->WithCompletion([&](const Error& e) {
					  ASSERT_TRUE(e.ok()) << e.what();
					  auto remainingTime = std::chrono::milliseconds(5000);
					  constexpr auto kStep = std::chrono::milliseconds(1);
					  while (remainingTime.count()) {
						  if (done) {
							  break;
						  }
						  remainingTime -= kStep;
						  std::this_thread::sleep_for(kStep);
					  }
					  assert(done.load());
					  std::unique_lock lck(mtx);
					  if (++counter == kItemsCount) {
						  lck.unlock();
						  cv.notify_one();
					  }
				  })
				  .Upsert(kNsNames, items[i]);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	done = true;
	std::unique_lock lck(mtx);
	auto res = cv.wait_for(lck, std::chrono::seconds(20), [&counter] { return counter == kItemsCount; });
	ASSERT_TRUE(res) << "counter = " << counter;
}

TEST(SyncCoroRx, AsyncCompletionsStop) {
	// Check if async completions are properlu handled during client's termination
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/AsyncCompletionsStop");
	reindexer::fs::RmDirAll(kTestDbPath);
	const std::string kNsNames = "ns_test";
	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	auto client = server.Get()->api.reindexer;
	Error err = client->OpenNamespace(kNsNames);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client->AddIndex(kNsNames, indDef);
	ASSERT_TRUE(err.ok()) << err.what();

	std::atomic<int> counter = 0;
	constexpr size_t kItemsCount = 10;
	std::vector<client::Item> items(kItemsCount);
	for (auto& item : items) {
		item = client->NewItem(kNsNames);  // NewItem can not be created async
	}
	for (size_t i = 0; i < kItemsCount; ++i) {
		const std::string json = fmt::sprintf(R"#({"id": %d, "val": "aaaaaaaa"})#", i);
		err = items[i].FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		err = client->WithCompletion([&](const Error&) { ++counter; }).Upsert(kNsNames, items[i]);
		ASSERT_TRUE(err.ok()) << err.what();
	}
	client->Stop();
	ASSERT_EQ(counter.load(), kItemsCount);
}

TEST(SyncCoroRx, TxInvalidation) {
	// Check if client transaction becomes invalid after reconnect
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/TxInvalidation");
	reindexer::fs::RmDirAll(kTestDbPath);
	const std::string kNsNames = "ns_test";
	const std::string kItemContent = R"json({"id": 1})json";
	const std::string kExpectedErrorText1 =
		"Connection was broken and all corresponding snapshots, queryresults and transaction were invalidated";
	const std::string kExpectedErrorText2 = "Request for invalid connection (probably this connection was broken and invalidated)";

	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));
	auto client = server.Get()->api.reindexer;
	Error err = client->OpenNamespace(kNsNames);
	ASSERT_TRUE(err.ok()) << err.what();

	auto originalTx = client->NewTransaction(kNsNames);
	ASSERT_TRUE(originalTx.Status().ok()) << originalTx.Status().what();

	auto movedTx = std::move(originalTx);
	{
		auto item = client->NewItem(kNsNames);
		item.FromJSON(kItemContent);
		// Special test case. Use after move is expected
		// NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
		err = originalTx.Insert(std::move(item));
		EXPECT_EQ(err.code(), errBadTransaction);
		client::QueryResults qr;
		err = client->CommitTransaction(originalTx, qr);
		EXPECT_EQ(err.code(), errBadTransaction);
	}
	{
		auto item = client->NewItem(kNsNames);
		item.FromJSON(kItemContent);
		err = movedTx.Insert(std::move(item));
		ASSERT_TRUE(err.ok()) << err.what();
	}

	client->Stop();
	err = client->Connect(server.Get()->kRPCDsn);
	ASSERT_TRUE(err.ok()) << err.what();
	err = client->Status(true);
	ASSERT_TRUE(err.ok()) << err.what();

	auto item = client->NewItem(kNsNames);
	item.FromJSON(kItemContent);
	err = movedTx.Insert(std::move(item));
	EXPECT_EQ(err.code(), errNetwork);
	if (err.what() != kExpectedErrorText1 && err.what() != kExpectedErrorText2) {
		ASSERT_TRUE(false) << err.what();
	}
	{
		Query q = Query().Set("id", {10});
		q.type_ = QueryType::QueryUpdate;
		err = movedTx.Modify(std::move(q));
		EXPECT_EQ(err.code(), errNetwork);
		if (err.what() != kExpectedErrorText1 && err.what() != kExpectedErrorText2) {
			ASSERT_TRUE(false) << err.what();
		}
	}
	{
		Query q;
		q.type_ = QueryType::QueryUpdate;
		err = movedTx.Modify(std::move(q));
		EXPECT_EQ(err.code(), errNetwork);
		if (err.what() != kExpectedErrorText1 && err.what() != kExpectedErrorText2) {
			ASSERT_TRUE(false) << err.what();
		}
	}
	client::QueryResults qr;
	err = client->CommitTransaction(movedTx, qr);
	EXPECT_EQ(err.code(), errNetwork);
	if (err.what() != kExpectedErrorText1 && err.what() != kExpectedErrorText2) {
		ASSERT_TRUE(false) << err.what();
	}
}

TEST(SyncCoroRx, QrInvalidation) {
	// Check if client QRs become invalid after reconnect
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/QrInvalidation");
	reindexer::fs::RmDirAll(kTestDbPath);
	const std::string kNsNames = "ns_test";
	const std::string kExpectedErrorText1 =
		"Connection was broken and all corresponding snapshots, queryresults and transaction were invalidated";
	const std::string kExpectedErrorText2 = "Request for invalid connection (probably this connection was broken and invalidated)";
	const unsigned kDataCount = 500;
	const unsigned kFetchCnt = 100;
	client::ReindexerConfig cfg(kFetchCnt);

	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));

	// Reconnect with correct options
	auto client = std::make_unique<client::Reindexer>(cfg);
	Error err = client->Connect(server.Get()->kRPCDsn);
	ASSERT_TRUE(err.ok()) << err.what();
	err = client->Status(true);
	ASSERT_TRUE(err.ok()) << err.what();

	err = client->OpenNamespace(kNsNames);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = client->AddIndex(kNsNames, indDef);
	ASSERT_TRUE(err.ok()) << err.what();
	SyncCoroRxHelpers::FillData(*client, kNsNames, kDataCount);

	client::QueryResults originalQr;
	err = client->Select(Query(kNsNames), originalQr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(originalQr.Count(), kDataCount);
	auto movedQr = std::move(originalQr);
	EXPECT_EQ(movedQr.Count(), kDataCount);

	// Check if original qr does not affect moved qr
	// Special test case. Use after move is expected
	// NOLINTNEXTLINE(bugprone-use-after-move,clang-analyzer-cplusplus.Move)
	for (auto& it : originalQr) {
		(void)it;
	}

	client->Stop();
	err = client->Connect(server.Get()->kRPCDsn);
	ASSERT_TRUE(err.ok()) << err.what();
	err = client->Status(true);
	ASSERT_TRUE(err.ok()) << err.what();

	unsigned idx = 0;
	auto mit = movedQr.begin();
	while (idx++ < kFetchCnt) {
		ASSERT_NE(mit, movedQr.end());
		ASSERT_TRUE(mit.Status().ok()) << mit.Status().what();
		++mit;
	}
	err = mit.Status();
	EXPECT_EQ(err.code(), errNetwork);
	if (err.what() != kExpectedErrorText1 && err.what() != kExpectedErrorText2) {
		ASSERT_TRUE(false) << err.what();
	}
}

TEST(SyncCoroRx, QRWithMultipleIterationLoops) {
	// Check if iterator has error status if user attempts to iterate over qrs, which were already fetched
	const std::string kTestDbPath = fs::JoinPath(fs::GetTempDir(), "SyncCoroRx/QRWithMultipleIterationLoops");
	reindexer::fs::RmDirAll(kTestDbPath);
	const std::string kNsName = "ns_test";
	constexpr unsigned kFetchCount = 50;
	constexpr unsigned kNsSize = kFetchCount * 3;
	client::ReindexerConfig cfg(kFetchCount);

	ServerControl server;
	server.InitServer(ServerControlConfig(0, kSyncCoroRxTestDefaultRpcPort, kSyncCoroRxTestDefaultHttpPort, kTestDbPath, "db"));

	// Reconnect with correct options
	client::Reindexer rx(cfg);
	Error err = rx.Connect(server.Get()->kRPCDsn);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rx.Status(true);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rx.OpenNamespace(kNsName);
	ASSERT_TRUE(err.ok()) << err.what();
	IndexDef indDef("id", "hash", "int", IndexOpts().PK());
	err = rx.AddIndex(kNsName, indDef);
	ASSERT_TRUE(err.ok()) << err.what();
	SyncCoroRxHelpers::FillData(rx, kNsName, kNsSize);

	client::QueryResults qr;
	err = rx.Select(Query(kNsName), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), kNsSize);
	WrSerializer ser;
	// First iteration loop (all of the items must be valid)
	for (auto& it : qr) {
		ser.Reset();
		ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		err = it.GetJSON(ser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		auto item = it.GetItem();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	}
	// Second iteration loop (unavailable items must be invalid)
	unsigned id = 0;
	for (auto& it : qr) {
		ser.Reset();
		if (id >= kNsSize - kFetchCount) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(fmt::sprintf(R"js({"id":%d,"val":"aaaaaaaaaaaaaaa"})js", id), ser.Slice());
		} else {
			EXPECT_FALSE(it.Status().ok()) << it.Status().what();
			err = it.GetJSON(ser, false);
			EXPECT_FALSE(err.ok()) << err.what();
			auto item = it.GetItem();
			EXPECT_FALSE(item.Status().ok()) << item.Status().what();
			err = it.GetCJSON(ser, false);
			EXPECT_FALSE(err.ok()) << err.what();
			err = it.GetMsgPack(ser, false);
			EXPECT_FALSE(err.ok()) << err.what();
		}
		++id;
	}
	rx.Stop();
}
