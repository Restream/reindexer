#include <chrono>
#include <condition_variable>
#include "rpcclient_api.h"
#include "rpcserver_fake.h"
#include "tools/fsops.h"

#include "core/cjson/jsonbuilder.h"
#include "coroutine/waitgroup.h"
#include "net/ev/ev.h"

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
	bool finished = false;
	loop.spawn([&loop, &finished] {
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

		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
	StopServer();
}

TEST_F(RPCClientTestApi, RenameNamespace) {
	// Should not be able to Rename namespace
	StartDefaultRealServer();
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([&loop, &finished] {
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
				reindexer::string_view sv = sr.Slice();
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

		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
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
	bool finished = false;
	loop.spawn([&loop, &finished] {
		reindexer::client::ReindexerConfig config;
		config.RequestTimeout = seconds(1);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		EXPECT_TRUE(err.ok()) << err.what();
		const string kNamespaceName = "MyNamespace";
		err = rx.AddNamespace(reindexer::NamespaceDef(kNamespaceName));
		EXPECT_EQ(err.code(), errTimeout);
		loop.sleep(std::chrono::seconds(4));
		err = rx.DropNamespace(kNamespaceName);
		EXPECT_TRUE(err.ok()) << err.what();
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
	StopServer();
}

TEST_F(RPCClientTestApi, CoroRequestCancels) {
	// Should return error on request cancel
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([&loop, &finished] {
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
			wg.add(1);
			loop.spawn([&ctx, &rx, &wg] {
				coroutine::wait_group_guard wgg(wg);
				auto err = rx.WithContext(&ctx).AddNamespace(reindexer::NamespaceDef("MyNamespace"));
				EXPECT_EQ(err.code(), errCanceled);
			});

			loop.sleep(std::chrono::seconds(1));
			ctx.Cancel();
			wg.wait();
		}
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
	StopServer();
}

TEST_F(RPCClientTestApi, CoroSuccessfullRequestWithTimeout) {
	// Should be able to execute some basic requests with timeout
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([&loop, &finished] {
		reindexer::client::ReindexerConfig config;
		config.ConnectTimeout = seconds(3);
		config.RequestTimeout = seconds(6);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		EXPECT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_TRUE(err.ok()) << err.what();
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
	StopServer();
}

TEST_F(RPCClientTestApi, CoroErrorLoginResponse) {
	// Should return error on failed Login
	AddFakeServer();
	StartServer(kDefaultRPCServerAddr, errForbidden);
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([&loop, &finished] {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_EQ(err.code(), errForbidden);
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
	StopServer();
}

TEST_F(RPCClientTestApi, CoroStatus) {
	// Should return correct Status, based on server's state
	std::string dbPath = string(kDbPrefix) + "/" + kDefaultRPCPort;
	reindexer::fs::RmDirAll(dbPath);
	AddRealServer(dbPath);
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([this, &loop, &finished] {
		reindexer::client::CoroReindexer rx;
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		rx.Connect(string("cproto://") + kDefaultRPCServerAddr + "/db1", loop, opts);
		for (size_t i = 0; i < 5; ++i) {
			StartServer();
			auto err = rx.Status();
			ASSERT_TRUE(err.ok()) << err.what();
			StopServer();
			loop.sleep(std::chrono::milliseconds(20));	// Allow reading coroutine to handle disconnect
			err = rx.Status();
			ASSERT_EQ(err.code(), errNetwork);
		}
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(RPCClientTestApi, CoroUpserts) {
	// Should be able to execute some basic operations within multiple concurrent coroutines
	using namespace reindexer::client;
	using namespace reindexer::net::ev;
	using reindexer::coroutine::wait_group;
	using reindexer::coroutine::wait_group_guard;

	StartDefaultRealServer();
	dynamic_loop loop;
	bool finished = false;

	loop.spawn([&loop, &finished] {
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

		auto upsertFn = [&rx, &nsName](wait_group& wg, size_t begin, size_t cnt) {
			wait_group_guard wgg(wg);
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

		auto txFunc = [&rx, &nsName](wait_group& wg, size_t begin, size_t cnt) {
			wait_group_guard wgg(wg);
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
			auto err = rx.CommitTransaction(tx);
			ASSERT_TRUE(err.ok()) << err.what();
		};

		auto selectFn = [&loop, &rx, &nsName](wait_group& wg, size_t cnt) {
			wait_group_guard wgg(wg);
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
		wg.add(9);
		loop.spawn(std::bind(upsertFn, std::ref(wg), 0, kCnt));
		loop.spawn(std::bind(upsertFn, std::ref(wg), kCnt, kCnt));
		loop.spawn(std::bind(upsertFn, std::ref(wg), 2 * kCnt, kCnt));
		loop.spawn(std::bind(upsertFn, std::ref(wg), 3 * kCnt, kCnt));
		loop.spawn(std::bind(selectFn, std::ref(wg), 300));
		loop.spawn(std::bind(txFunc, std::ref(wg), 4 * kCnt, 2 * kCnt));
		loop.spawn(std::bind(txFunc, std::ref(wg), 6 * kCnt, 2 * kCnt));
		loop.spawn(std::bind(txFunc, std::ref(wg), 8 * kCnt, 2 * kCnt));
		loop.spawn(std::bind(selectFn, std::ref(wg), 300));

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
		finished = true;
	});

	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(RPCClientTestApi, CoroUpdatesFilteringByNs) {
	// Should be able to work with updates subscription and filtering
	StartDefaultRealServer();
	ev::dynamic_loop loop;
	bool finished = false;

	auto mainRoutine = [this, &loop, &finished] {
		reindexer::client::CoroReindexer rx;
		const string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		constexpr auto kNs1Name = "ns1"_sv;
		constexpr auto kNs2Name = "ns2"_sv;
		constexpr auto kNs3Name = "ns3"_sv;
		constexpr auto kNs4Name = "ns4"_sv;
		constexpr auto kNs5Name = "ns5"_sv;

		CreateNamespace(rx, kNs4Name);
		CreateNamespace(rx, kNs5Name);

		UpdatesReciever reciever1(loop);  // Recieves updates for ns 'n5' and 'ns1'
		{
			UpdatesFilters filters;
			filters.AddFilter(kNs5Name, UpdatesFilters::Filter());
			err = rx.SubscribeUpdates(&reciever1, filters);
			ASSERT_TRUE(err.ok()) << err.what();
			UpdatesFilters filters1;
			filters1.AddFilter(kNs1Name, UpdatesFilters::Filter());
			err = rx.SubscribeUpdates(&reciever1, filters1, SubscriptionOpts().IncrementSubscription());
			ASSERT_TRUE(err.ok()) << err.what();
		}

		{
			const size_t count = 50;
			FillData(rx, kNs4Name, 0, count);
			FillData(rx, kNs5Name, 0, count);
			ASSERT_TRUE(reciever1.AwaitNamespaces(1));
			ASSERT_TRUE(reciever1.AwaitItems(kNs5Name, count));
			reciever1.Reset();
		}

		UpdatesReciever reciever2(loop);  // Recieves all the updates
		{
			UpdatesFilters filters;
			err = rx.SubscribeUpdates(&reciever2, filters);
			ASSERT_TRUE(err.ok()) << err.what();
			UpdatesFilters filters1;
			err = rx.SubscribeUpdates(&reciever2, filters1, SubscriptionOpts().IncrementSubscription());
			ASSERT_TRUE(err.ok()) << err.what();
		}

		UpdatesReciever reciever3(loop);  // Recieves updates for ns 'ns4'
		{
			UpdatesFilters filters;
			filters.AddFilter(kNs4Name, UpdatesFilters::Filter());
			err = rx.SubscribeUpdates(&reciever3, filters);
			ASSERT_TRUE(err.ok()) << err.what();
		}

		{
			const size_t count = 100;
			FillData(rx, kNs4Name, 0, count);
			FillData(rx, kNs5Name, 0, count);
			ASSERT_TRUE(reciever1.AwaitNamespaces(1));
			ASSERT_TRUE(reciever1.AwaitItems(kNs5Name, count));
			reciever1.Reset();

			ASSERT_TRUE(reciever2.AwaitNamespaces(2));
			ASSERT_TRUE(reciever2.AwaitItems(kNs5Name, count));
			ASSERT_TRUE(reciever2.AwaitItems(kNs4Name, count));
			reciever2.Reset();

			ASSERT_TRUE(reciever3.AwaitNamespaces(1));
			ASSERT_TRUE(reciever3.AwaitItems(kNs4Name, count));
			reciever3.Reset();
		}

		err = rx.OpenNamespace(kNs1Name);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.OpenNamespace(kNs2Name);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.RenameNamespace(kNs1Name, string(kNs3Name));
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.DropNamespace(kNs2Name);
		ASSERT_TRUE(err.ok()) << err.what();
		{
			ASSERT_TRUE(reciever1.AwaitNamespaces(2));
			ASSERT_TRUE(reciever1.AwaitItems(kNs1Name, 2));
			ASSERT_TRUE(reciever1.AwaitItems(kNs2Name, 2));
			reciever1.Reset();

			ASSERT_TRUE(reciever2.AwaitNamespaces(2));
			ASSERT_TRUE(reciever2.AwaitItems(kNs1Name, 2));
			ASSERT_TRUE(reciever2.AwaitItems(kNs2Name, 2));
			reciever2.Reset();

			ASSERT_TRUE(reciever3.AwaitNamespaces(2));
			ASSERT_TRUE(reciever3.AwaitItems(kNs1Name, 2));
			ASSERT_TRUE(reciever3.AwaitItems(kNs2Name, 2));
			reciever3.Reset();
		}

		err = rx.UnsubscribeUpdates(&reciever1);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.UnsubscribeUpdates(&reciever2);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.UnsubscribeUpdates(&reciever3);
		ASSERT_TRUE(err.ok()) << err.what();

		{
			const size_t count = 50;
			FillData(rx, kNs4Name, 0, count);
			FillData(rx, kNs5Name, 0, count);
			loop.sleep(std::chrono::seconds(2));
			ASSERT_TRUE(reciever1.AwaitNamespaces(0));
			ASSERT_TRUE(reciever2.AwaitNamespaces(0));
			ASSERT_TRUE(reciever3.AwaitNamespaces(0));
		}
		err = rx.Stop();
		ASSERT_TRUE(err.ok()) << err.what();

		err = rx.Status();
		ASSERT_FALSE(err.ok());
		finished = true;
	};

	loop.spawn(mainRoutine);

	loop.run();
	ASSERT_TRUE(finished);
}
