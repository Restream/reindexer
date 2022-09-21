#include <chrono>
#include <condition_variable>
#include "rpcclient_api.h"
#include "rpcserver_fake.h"
#include "tools/fsops.h"

#include "client/synccororeindexer.h"
#include "core/cjson/jsonbuilder.h"
#include "coroutine/waitgroup.h"
#include "net/ev/ev.h"
#include "reindexertestapi.h"

#include "core/namespace/snapshot/snapshot.h"

using std::chrono::seconds;

TEST_F(RPCClientTestApi, ConnectTimeout) {
	AddFakeServer();
	StartServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(1);
	config.RequestTimeout = seconds(5);
	reindexer::client::Reindexer rx(config);
	auto res = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	res = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
	EXPECT_EQ(res.code(), errTimeout);
	StopServer();
}

TEST_F(RPCClientTestApi, RequestTimeout) {
	AddFakeServer();
	StartServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(3);
	config.RequestTimeout = seconds(3);
	reindexer::client::Reindexer rx(config);
	auto res = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	const string kNamespaceName = "MyNamespace";
	res = rx.AddNamespace(reindexer::NamespaceDef(kNamespaceName));
	EXPECT_EQ(res.code(), errTimeout);
	res = rx.DropNamespace(kNamespaceName);
	EXPECT_TRUE(res.ok()) << res.what();
	StopServer();
}

TEST_F(RPCClientTestApi, RequestCancels) {
	AddFakeServer();
	StartServer();
	reindexer::client::Reindexer rx;
	auto res = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());

	{
		CancelRdxContext ctx;
		ctx.Cancel();
		res = rx.WithContext(&ctx).AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_EQ(res.code(), errCanceled);
	}

	{
		CancelRdxContext ctx;
		std::thread thr([&ctx, &rx] {
			auto res = rx.WithContext(&ctx).AddNamespace(reindexer::NamespaceDef("MyNamespace"));
			EXPECT_EQ(res.code(), errCanceled);
		});

		std::this_thread::sleep_for(std::chrono::seconds(1));
		ctx.Cancel();
		thr.join();
	}

	StopServer();
}

TEST_F(RPCClientTestApi, SuccessfullRequestWithTimeout) {
	AddFakeServer();
	StartServer();
	reindexer::client::ReindexerConfig config;
	config.ConnectTimeout = seconds(3);
	config.RequestTimeout = seconds(6);
	reindexer::client::Reindexer rx(config);
	auto res = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	EXPECT_TRUE(res.ok());
	res = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
	EXPECT_TRUE(res.ok());
	StopServer();
}

TEST_F(RPCClientTestApi, ErrorLoginResponse) {
	AddFakeServer();
	StartServer(kDefaultRPCServerAddr, errForbidden);
	reindexer::client::Reindexer rx;
	rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db");
	auto res = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
	EXPECT_EQ(res.code(), errForbidden);
	StopServer();
}

TEST_F(RPCClientTestApi, SeveralDsnReconnect) {
	const string cprotoIdentifier = "cproto://";
	const string dbName = "/test_db";
	const vector<string> uris = {"127.0.0.1:25673", "127.0.0.1:25674", "127.0.0.1:25675", "127.0.0.1:25676"};

	RPCServerConfig serverConfig;
	serverConfig.loginDelay = std::chrono::milliseconds(1);
	serverConfig.openNsDelay = std::chrono::milliseconds(1);
	serverConfig.selectDelay = std::chrono::milliseconds(1);
	for (const string& uri : uris) {
		AddFakeServer(uri, serverConfig);
		StartServer(uri);
	}

	reindexer::client::ReindexerConfig clientConfig;
	clientConfig.ConnectTimeout = seconds(10);
	clientConfig.RequestTimeout = seconds(10);
	clientConfig.ReconnectAttempts = 0;
	reindexer::client::Reindexer rx(clientConfig);
	std::vector<pair<string, reindexer::client::ConnectOpts>> connectData;
	for (const string& uri : uris) {
		connectData.emplace_back(string(cprotoIdentifier + uri + dbName), reindexer::client::ConnectOpts());
	}
	auto res = rx.Connect(connectData);
	EXPECT_TRUE(res.ok()) << res.what();

	for (size_t i = 0; i < 100; ++i) {
		if (CheckIfFakeServerConnected(uris[0])) break;
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	for (size_t i = 0; i < 100; ++i) {
		if (rx.Status().ok()) break;
		std::this_thread::sleep_for(std::chrono::milliseconds(10));
	}

	Query queryConfingNs = Query("#config");
	for (size_t i = 0; i < uris.size() - 1; ++i) {
		StopServer(uris[i]);
		for (size_t j = 0; j < 10; ++j) {
			client::QueryResults qr;
			res = rx.Select(queryConfingNs, qr);
			if (res.ok()) break;
			std::this_thread::sleep_for(std::chrono::milliseconds(100));
		}
		EXPECT_TRUE(res.ok()) << res.what();
	}
	StopAllServers();
}

TEST_F(RPCClientTestApi, SelectFromClosedNamespace) {
	// Should not be able to Select from closed namespace
	StartDefaultRealServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ReindexerConfig config;
		config.FetchAmount = 0;
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, opts);
		ASSERT_TRUE(err.ok());
		const string kNsName = "MyNamesapce";
		{
			reindexer::client::CoroQueryResults qr;
			err = rx.Select(reindexer::Query(kNsName), qr);
			ASSERT_FALSE(err.ok());
		}
		reindexer::NamespaceDef nsDef(kNsName);
		nsDef.AddIndex("id", "hash", "int", IndexOpts().PK());
		err = rx.AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();
		auto item = rx.NewItem(kNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		reindexer::WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put("id", 1);
		jsonBuilder.End();
		char* endp = nullptr;
		err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.Upsert(kNsName, item);
		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::client::CoroQueryResults qr;
		{
			err = rx.Select(reindexer::Query(kNsName), qr);
			ASSERT_TRUE(err.ok()) << err.what();
		}
		err = rx.CloseNamespace(kNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 1);
		for (auto& it : qr) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		}
		{
			reindexer::client::CoroQueryResults qr1;
			err = rx.Select(reindexer::Query(kNsName), qr1);
			ASSERT_TRUE(err.ok()) << err.what();  // TODO: Namespace is not actually closing now
		}
	});
	loop.run();
	StopServer();
}

TEST_F(RPCClientTestApi, RenameNamespace) {
	// Should not be able to Rename namespace
	StartDefaultRealServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ReindexerConfig config;
		config.FetchAmount = 0;
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();
		const string kInitialNsName = "InitialNamespace";
		const string kResultNsName = "ResultNamespace";
		reindexer::NamespaceDef nsDef(kInitialNsName);
		nsDef.AddIndex("id", "hash", "int", IndexOpts().PK());
		err = rx.AddNamespace(nsDef);
		ASSERT_TRUE(err.ok()) << err.what();
		auto item = rx.NewItem(kInitialNsName);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		reindexer::WrSerializer wrser;
		reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
		jsonBuilder.Put("id", 1);
		jsonBuilder.End();
		char* endp = nullptr;
		err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.Upsert(kInitialNsName, item);
		ASSERT_TRUE(err.ok()) << err.what();

		auto testInList = [&rx](const std::string& testNamespaceName, bool inList) {
			vector<reindexer::NamespaceDef> namespacesList;
			auto err = rx.EnumNamespaces(namespacesList, reindexer::EnumNamespacesOpts());
			ASSERT_TRUE(err.ok()) << err.what();
			auto r = std::find_if(namespacesList.begin(), namespacesList.end(),
								  [testNamespaceName](const reindexer::NamespaceDef& d) { return d.name == testNamespaceName; });
			if (inList) {
				ASSERT_FALSE(r == namespacesList.end()) << testNamespaceName << " not exist";
			} else {
				ASSERT_TRUE(r == namespacesList.end()) << testNamespaceName << " exist";
			}
		};

		auto getRowsInJSON = [&rx](const std::string& namespaceName, std::vector<std::string>& resStrings) {
			client::CoroQueryResults result;
			rx.Select(Query(namespaceName), result);
			resStrings.clear();
			for (auto it = result.begin(); it != result.end(); ++it) {
				reindexer::WrSerializer sr;
				it.GetJSON(sr, false);
				std::string_view sv = sr.Slice();
				resStrings.emplace_back(sv.data(), sv.size());
			}
		};

		std::vector<std::string> resStrings;
		std::vector<std::string> resStringsBeforeTest;
		testInList(kInitialNsName, true);
		testInList(kResultNsName, false);
		getRowsInJSON(kInitialNsName, resStringsBeforeTest);

		err = rx.RenameNamespace(kInitialNsName, kResultNsName);
		ASSERT_TRUE(err.ok()) << err.what();

		testInList(kInitialNsName, false);
		testInList(kResultNsName, true);
		getRowsInJSON(kResultNsName, resStrings);
		ASSERT_TRUE(resStrings == resStringsBeforeTest);
	});
	loop.run();
	StopServer();
}

TEST_F(RPCClientTestApi, CoroRequestTimeout) {
	// Should return error on request timeout
	RPCServerConfig conf;
	conf.loginDelay = std::chrono::seconds(0);
	conf.openNsDelay = std::chrono::seconds(4);
	AddFakeServer(kDefaultRPCServerAddr, conf);
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::CoroReindexerConfig config;
		config.NetTimeout = seconds(1);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		EXPECT_TRUE(err.ok()) << err.what();
		const string kNamespaceName = "MyNamespace";
		err = rx.AddNamespace(reindexer::NamespaceDef(kNamespaceName));
		EXPECT_EQ(err.code(), errTimeout);
		loop.sleep(std::chrono::seconds(4));
		err = rx.DropNamespace(kNamespaceName);
		EXPECT_TRUE(err.ok()) << err.what();
	});
	loop.run();
	StopServer();
}

TEST_F(RPCClientTestApi, CoroRequestCancels) {
	// Should return error on request cancel
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		EXPECT_TRUE(err.ok()) << err.what();

		{
			CancelRdxContext ctx;
			ctx.Cancel();
			err = rx.WithContext(&ctx).AddNamespace(reindexer::NamespaceDef("MyNamespace"));
			EXPECT_EQ(err.code(), errCanceled);
		}

		{
			CancelRdxContext ctx;
			coroutine::wait_group wg;
			loop.spawn(wg, [&ctx, &rx] {
				auto err = rx.WithContext(&ctx).AddNamespace(reindexer::NamespaceDef("MyNamespace"));
				EXPECT_EQ(err.code(), errCanceled);
			});

			loop.sleep(std::chrono::seconds(1));
			ctx.Cancel();
			wg.wait();
		}
	});
	loop.run();
	StopServer();
}

TEST_F(RPCClientTestApi, CoroSuccessfullRequestWithTimeout) {
	// Should be able to execute some basic requests with timeout
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::CoroReindexerConfig config;
		config.NetTimeout = seconds(6);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		EXPECT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_TRUE(err.ok()) << err.what();
	});
	loop.run();
	StopServer();
}

TEST_F(RPCClientTestApi, CoroErrorLoginResponse) {
	// Should return error on failed Login
	AddFakeServer();
	StartServer(kDefaultRPCServerAddr, errForbidden);
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_EQ(err.code(), errForbidden);
	});
	loop.run();
	StopServer();
}

TEST_F(RPCClientTestApi, CoroStatus) {
	// Should return correct Status, based on server's state
	std::string dbPath = std::string(kDbPrefix) + "/" + std::to_string(kDefaultRPCPort);
	reindexer::fs::RmDirAll(dbPath);
	AddRealServer(dbPath);
	ev::dynamic_loop loop;
	loop.spawn([this, &loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/db1", loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t i = 0; i < 5; ++i) {
			StartServer();
			err = rx.Status();
			ASSERT_TRUE(err.ok()) << err.what();
			StopServer();
			loop.sleep(std::chrono::milliseconds(20));	// Allow reading coroutine to handle disconnect
			err = rx.Status();
			ASSERT_EQ(err.code(), errNetwork);
		}
	});
	loop.run();
}

TEST_F(RPCClientTestApi, CoroUpserts) {
	// Should be able to execute some basic operations within multiple concurrent coroutines
	using namespace reindexer::client;
	using namespace reindexer::net::ev;
	using reindexer::coroutine::wait_group;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn([&loop]() noexcept {
		const std::string nsName = "ns1";
		const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();

		err = rx.OpenNamespace(nsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(nsName, {"id", {"id"}, "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		auto upsertFn = [&rx, &nsName](size_t begin, size_t cnt) {
			for (size_t i = begin; i < begin + cnt; ++i) {
				auto item = rx.NewItem(nsName);
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				reindexer::WrSerializer wrser;
				reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", i);
				jsonBuilder.End();
				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << err.what();
				err = rx.Upsert(nsName, item);
				ASSERT_TRUE(err.ok()) << err.what();
			}
		};

		auto txFunc = [&rx, &nsName](size_t begin, size_t cnt) {
			auto tx = rx.NewTransaction(nsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();
			for (size_t i = begin; i < begin + cnt; ++i) {
				auto item = tx.NewItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				reindexer::WrSerializer wrser;
				reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", i);
				jsonBuilder.End();
				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << err.what();
				err = tx.Upsert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			CoroQueryResults qrTx;
			auto err = rx.CommitTransaction(tx, qrTx);
			ASSERT_TRUE(err.ok()) << err.what();
		};

		auto selectFn = [&loop, &rx, &nsName](size_t cnt) {
			constexpr size_t kMultiplier = 9;
			for (size_t j = 0; j < kMultiplier * cnt; ++j) {
				if (j % kMultiplier == 0) {
					reindexer::client::CoroQueryResults qr;
					auto err = rx.Select(reindexer::Query(nsName), qr);
					ASSERT_TRUE(err.ok()) << err.what();
					for (auto& it : qr) {
						ASSERT_TRUE(it.Status().ok()) << it.Status().what();
					}
				} else {
					auto err = rx.Status();
					ASSERT_TRUE(err.ok()) << err.what();
				}
				loop.sleep(std::chrono::milliseconds(1));
			}
		};

		wait_group wg;
		constexpr size_t kCnt = 3000;
		loop.spawn(wg, std::bind(upsertFn, 0, kCnt));
		loop.spawn(wg, std::bind(upsertFn, kCnt, kCnt));
		loop.spawn(wg, std::bind(upsertFn, 2 * kCnt, kCnt));
		loop.spawn(wg, std::bind(upsertFn, 3 * kCnt, kCnt));
		loop.spawn(wg, std::bind(selectFn, 300));
		loop.spawn(wg, std::bind(txFunc, 4 * kCnt, 2 * kCnt));
		loop.spawn(wg, std::bind(txFunc, 6 * kCnt, 2 * kCnt));
		loop.spawn(wg, std::bind(txFunc, 8 * kCnt, 2 * kCnt));
		loop.spawn(wg, std::bind(selectFn, 300));

		wg.wait();

		reindexer::client::CoroQueryResults qr;
		err = rx.Select(reindexer::Query(nsName), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), 10 * kCnt);
		for (auto& it : qr) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		}
		err = rx.Stop();
		ASSERT_TRUE(err.ok()) << err.what();
	});

	loop.run();
}

template <typename RxT>
void ReconnectTest(RxT& rx, RPCClientTestApi& api, size_t dataCount, const std::string& nsName) {
	typename RxT::QueryResultsT qr;
	auto err = rx.Select(reindexer::Query(nsName), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), dataCount);

	api.StopServer();
	api.StartServer();
	qr = typename RxT::QueryResultsT();
	err = rx.Select(reindexer::Query(nsName), qr);
	if (err.ok()) {
		ASSERT_EQ(qr.Count(), dataCount);
	} else {
		ASSERT_EQ(err.code(), errNetwork) << err.what();
	}
	qr = typename RxT::QueryResultsT();
	err = rx.Select(reindexer::Query(nsName), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), dataCount);

	err = rx.Stop();
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, Reconnect) {
	// CoroReindexer should be able to handle reconnect properly
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn([this, &loop]() noexcept {
		constexpr auto kDataCount = 2;
		const std::string kNsName = "ns1";
		const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();
		CreateNamespace(rx, kNsName);
		FillData(rx, kNsName, 0, kDataCount);

		ReconnectTest(rx, *this, kDataCount, kNsName);
	});

	loop.run();
}

TEST_F(RPCClientTestApi, ReconnectSyncCoroRx) {
	// SyncCoroReindexer should be able to handle reconnect properly
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn([this, &loop]() noexcept {
		constexpr auto kDataCount = 2;
		const std::string kNsName = "ns1";
		const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		{
			reindexer::client::ConnectOpts opts;
			opts.CreateDBIfMissing();
			CoroReindexer crx;
			auto err = crx.Connect(dsn, loop, opts);
			ASSERT_TRUE(err.ok()) << err.what();
			CreateNamespace(crx, kNsName);
			FillData(crx, kNsName, 0, kDataCount);
		}

		SyncCoroReindexer rx;
		auto err = rx.Connect(dsn);
		ASSERT_TRUE(err.ok()) << err.what();

		ReconnectTest(rx, *this, kDataCount, kNsName);
	});

	loop.run();
}

TEST_F(RPCClientTestApi, ServerRestart) {
	// Client should handle error on server's restart
	using namespace reindexer::client;
	using namespace reindexer::net::ev;
	using reindexer::coroutine::wait_group;
	using reindexer::coroutine::wait_group_guard;

	std::atomic<bool> terminate = false;
	std::atomic<bool> ready = false;

	// Startup server
	StartDefaultRealServer();
	enum class Step { Init, ShutdownInProgress, ShutdownDone, RestartInProgress, RestartDone };
	std::atomic<Step> step = Step::Init;

	// Create thread, performing upserts
	std::thread upsertsTh([&terminate, &ready, &step] {
		dynamic_loop loop;

		loop.spawn([&loop, &terminate, &ready, &step]() noexcept {
			const std::string nsName = "ns1";
			const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
			reindexer::client::ConnectOpts opts;
			opts.CreateDBIfMissing();
			CoroReindexer rx;
			auto err = rx.Connect(dsn, loop, opts);
			ASSERT_TRUE(err.ok()) << err.what();

			err = rx.OpenNamespace(nsName);
			ASSERT_TRUE(err.ok()) << err.what();
			err = rx.AddIndex(nsName, {"id", {"id"}, "hash", "int", IndexOpts().PK()});
			ASSERT_TRUE(err.ok()) << err.what();

			auto upsertFn = [&loop, &rx, &nsName, &terminate, &step](wait_group& wg, size_t begin, size_t cnt) {
				wait_group_guard wgg(wg);
				while (!terminate) {
					for (size_t i = begin; i < begin + cnt; ++i) {
						auto item = rx.NewItem(nsName);
						ASSERT_TRUE(item.Status().ok()) << item.Status().what();

						reindexer::WrSerializer wrser;
						reindexer::JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
						jsonBuilder.Put("id", i);
						jsonBuilder.End();
						char* endp = nullptr;
						auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
						ASSERT_TRUE(err.ok()) << err.what();
						auto localStep = step.load();
						err = rx.Upsert(nsName, item);
						if (localStep == step.load()) {
							switch (localStep) {
								case Step::Init:
									// If server is running, updates have to return OK
									ASSERT_TRUE(err.ok()) << err.what();
									break;
								case Step::ShutdownDone:
									// If server was shutdown, updates have to return error
									ASSERT_TRUE(!err.ok());
									break;
								case Step::RestartDone:
									// If server was restarted, updates have to return OK
									ASSERT_TRUE(err.ok()) << err.what();
									break;
								default:;  // No additional checks in transition states
							}
						}
					}
					loop.sleep(std::chrono::milliseconds(50));
				}
			};

			wait_group wg;
			constexpr size_t kCnt = 100;
			wg.add(3);
			loop.spawn(std::bind(upsertFn, std::ref(wg), 0, kCnt));
			loop.spawn(std::bind(upsertFn, std::ref(wg), kCnt, kCnt));
			loop.spawn(std::bind(upsertFn, std::ref(wg), 2 * kCnt, kCnt));

			ready = true;
			wg.wait();

			err = rx.Stop();
			ASSERT_TRUE(err.ok()) << err.what();
		});

		loop.run();
	});
	while (!ready) {  // -V776
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	// Shutdown server
	step = Step::ShutdownInProgress;
	StopServer();
	step = Step::ShutdownDone;
	std::this_thread::sleep_for(std::chrono::milliseconds(300));

	step = Step::RestartInProgress;
	StartServer();
	step = Step::RestartDone;
	std::this_thread::sleep_for(std::chrono::milliseconds(300));

	terminate = true;
	upsertsTh.join();
}

TEST_F(RPCClientTestApi, TemporaryNamespaceAutoremove) {
	// Temporary namespace must be automaticly removed after disconnect
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn([&loop]() noexcept {
		const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();

		std::string tmpNsName;
		err = rx.CreateTemporaryNamespace("ns1", tmpNsName);
		ASSERT_TRUE(err.ok()) << err.what();

		// Check if temporary ns was created
		std::vector<NamespaceDef> nsList;
		err = rx.EnumNamespaces(nsList, EnumNamespacesOpts().OnlyNames().HideSystem().HideTemporary());
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(nsList.size(), 0);
		err = rx.EnumNamespaces(nsList, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(nsList.size(), 1);
		ASSERT_EQ(nsList[0].name, tmpNsName);

		// Reconnect
		err = rx.Stop();
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.Connect(dsn, loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();

		// Allow server to handle disconnect
		std::this_thread::sleep_for(std::chrono::seconds(2));

		// Check if namespce was removed
		nsList.clear();
		err = rx.EnumNamespaces(nsList, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << err.what();
		if (nsList.size() > 0) {
			for (auto& ns : nsList) std::cerr << ns.name << std::endl;
			ASSERT_TRUE(false);
		}

		err = rx.Stop();
		ASSERT_TRUE(err.ok()) << err.what();
	});

	loop.run();
}

TEST_F(RPCClientTestApi, ItemJSONWithDouble) {
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		auto item = rx.NewItem("ns");
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		{
			const std::string kJSON = R"_({"id":1234,"double":0.0})_";
			err = item.FromJSON(kJSON);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(item.GetJSON(), kJSON);
		}

		{
			const std::string kJSON = R"_({"id":1234,"double":0.1})_";
			err = item.FromJSON(kJSON);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(item.GetJSON(), kJSON);
		}
	});
	loop.run();
}

TEST_F(RPCClientTestApi, UnknowResultsFlag) {
	// Check if server will not resturn unknown result flag
	StartDefaultRealServer();
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([&loop, &finished] {
		reindexer::client::CoroReindexer rx;
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/db1", loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();
		const int kResultsUnknownFlag = 0x40000000;	 // Max available int flag
		client::CoroQueryResults qr(kResultsCJson | kResultsWithItemID | kResultsUnknownFlag);
		err = rx.Select(Query("#config").Where("type", CondEq, {"namespaces"}), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		// Check, that kResultsUnknownFlag was not sent back
		ASSERT_EQ(qr.GetFlags(), kResultsCJson | kResultsWithItemID);
		ASSERT_EQ(qr.Count(), 1);
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}
