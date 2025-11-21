#include <chrono>
#include <thread>
#include "query_aggregate_strict_mode_test.h"
#include "rpcclient_api.h"
#include "rpcserver_fake.h"
#include "tools/fsops.h"

#include "client/reindexer.h"
#include "client/snapshot.h"
#include "core/cjson/jsonbuilder.h"
#include "core/system_ns_names.h"
#include "coroutine/waitgroup.h"
#include "gtests/tests/gtest_cout.h"
#include "gtests/tools.h"
#include "net/ev/ev.h"

using std::chrono::seconds;

TEST_F(RPCClientTestApi, CoroRequestTimeout) {
	// Should return error on request timeout
	RPCServerConfig conf;
	conf.loginDelay = std::chrono::seconds(0);
	conf.openNsDelay = std::chrono::seconds(4);
	AddFakeServer(kDefaultRPCServerAddr, conf);
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([&loop] {
		reindexer::client::ReindexerConfig config;
		config.NetTimeout = seconds(1);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		const std::string kNamespaceName = "MyNamespace";
		err = rx.AddNamespace(reindexer::NamespaceDef(kNamespaceName));
		EXPECT_EQ(err.code(), errTimeout);
		loop.sleep(std::chrono::seconds(4));
		err = rx.DropNamespace(kNamespaceName);
		ASSERT_TRUE(err.ok()) << err.what();
	}));
	loop.run();
	Error err = StopServer();
	ASSERT_TRUE(err.ok()) << err.what();
}

static std::chrono::seconds GetMaxTimeForCoroSelectTimeout(unsigned requests, std::chrono::seconds delay) {
	const auto cpus = std::thread::hardware_concurrency();
	const auto kBase = std::max(requests * delay.count() / 16, delay.count());
	const std::chrono::seconds kDefaultMaxTime(kBase + 10);
	if (cpus == 0) {
		TestCout() << fmt::format("Unable to get CPUs count. Using test max time {} seconds Test may flack in this case",
								  4 * kDefaultMaxTime.count())
				   << std::endl;
		return 4 * kDefaultMaxTime;
	}
	auto resultMaxTime = kDefaultMaxTime;
	if (cpus == 1) {
		resultMaxTime = 16 * kDefaultMaxTime;
	} else if (cpus > 1 && cpus < 4) {
		resultMaxTime = 8 * kDefaultMaxTime;
	} else if (cpus >= 4 && cpus < 8) {
		resultMaxTime = 4 * kDefaultMaxTime;
	} else if (cpus >= 8 && cpus < 16) {
		resultMaxTime = 2 * kDefaultMaxTime;
	}
	TestCout() << fmt::format("Test max time: {} seconds for {} total requests on {} CPUs with {} seconds of delay for each request",
							  resultMaxTime.count(), requests, cpus, delay.count())
			   << std::endl;
	return resultMaxTime;
}

TEST_F(RPCClientTestApi, CoroSelectTimeout) {
	const std::string kNamespaceName = "MyNamespace";
	constexpr size_t kCorCount = 16;
	constexpr size_t kQueriesCount = 3;
	constexpr std::chrono::seconds kSelectDelay(4);
	RPCServerConfig conf;
	conf.loginDelay = std::chrono::seconds(0);
	conf.selectDelay = kSelectDelay;
	conf.openNsDelay = std::chrono::seconds{0};
	auto& server = AddFakeServer(kDefaultRPCServerAddr, conf);
	StartServer();
	ev::dynamic_loop loop;
	std::vector<bool> finished(kCorCount, false);
	ev::timer testTimer;
	testTimer.set([&](ev::timer&, int) {
		// Just to print output on CI
		ASSERT_TRUE(false) << fmt::format("Test deadline exceeded. Closed count: {}. Expected: {}. {}|", server.CloseQRRequestsCount(),
										  kCorCount * kQueriesCount, reindexer::steady_clock_w::now().time_since_epoch().count());
	});
	testTimer.set(loop);
	const auto kMaxTime = GetMaxTimeForCoroSelectTimeout(kCorCount * kQueriesCount, kSelectDelay);
	testTimer.start(double(kMaxTime.count()));
	for (size_t i = 0; i < kCorCount; ++i) {
		loop.spawn([&, index = i] {
			reindexer::client::ReindexerConfig config;
			config.NetTimeout = seconds(1);
			reindexer::client::CoroReindexer rx(config);
			auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
			ASSERT_TRUE(err.ok()) << err.what();
			coroutine::wait_group wg;
			wg.add(kQueriesCount);
			for (size_t j = 0; j < kQueriesCount; ++j) {
				loop.spawn([&] {
					coroutine::wait_group_guard wgg(wg);
					reindexer::client::CoroQueryResults qr;
					err = rx.Select(reindexer::Query(kNamespaceName), qr);
					EXPECT_EQ(err.code(), errTimeout);
				});
			}
			wg.wait();
			loop.granular_sleep(kSelectDelay * kQueriesCount * kCorCount, std::chrono::milliseconds{300},
								[&] { return server.CloseQRRequestsCount() >= kCorCount * kQueriesCount; });
			EXPECT_EQ(server.CloseQRRequestsCount(), kCorCount * kQueriesCount);
			err = rx.AddNamespace(reindexer::NamespaceDef(kNamespaceName + std::to_string(index)));
			ASSERT_TRUE(err.ok()) << err.what();
			finished[index] = true;
		});
	}
	loop.run();
	for (size_t i = 0; i < kCorCount; ++i) {
		ASSERT_TRUE(finished[i]);
	}
	const Error err = StopServer();
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, CoroRequestCancels) {
	// Should return error on request cancel
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([&loop] {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();

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
	}));
	loop.run();
	Error err = StopServer();
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, CoroSuccessfulRequestWithTimeout) {
	// Should be able to execute some basic requests with timeout
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([&loop] {
		reindexer::client::ReindexerConfig config;
		config.NetTimeout = seconds(6);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		ASSERT_TRUE(err.ok()) << err.what();
	}));
	loop.run();
	Error err = StopServer();
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, CoroErrorLoginResponse) {
	// Should return error on failed Login
	AddFakeServer();
	StartServer(kDefaultRPCServerAddr, errForbidden);
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([&loop] {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_EQ(err.code(), errForbidden);
	}));
	loop.run();
	Error err = StopServer();
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, CoroStatus) {
	// Should return correct Status, based on server's state
	std::string dbPath = std::string(kDbPrefix) + "/" + std::to_string(kDefaultRPCPort);
	std::ignore = reindexer::fs::RmDirAll(dbPath);
	AddRealServer(dbPath);
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([this, &loop] {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/db1", loop,
							  reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t i = 0; i < 5; ++i) {
			StartServer();
			err = rx.Status();
			ASSERT_TRUE(err.ok()) << err.what();
			err = StopServer();
			ASSERT_TRUE(err.ok()) << err.what();
			loop.sleep(std::chrono::milliseconds(20));	// Allow reading coroutine to handle disconnect
			err = rx.Status();
			ASSERT_EQ(err.code(), errNetwork) << err.what();
		}
	}));
	loop.run();
}

TEST_F(RPCClientTestApi, CoroUpserts) {
	// Should be able to execute some basic operations within multiple concurrent coroutines
	using namespace reindexer::client;
	using namespace reindexer::net::ev;
	using reindexer::coroutine::wait_group;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop] {
		const std::string nsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
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
		rx.Stop();
	}));

	loop.run();
	Error err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
}

template <typename RxT>
void ReconnectTest(RxT& rx, RPCClientTestApi& api, size_t dataCount, const std::string& nsName) {
	typename RxT::QueryResultsT qr;
	auto err = rx.Select(reindexer::Query(nsName), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), dataCount);

	err = api.StopServer();
	ASSERT_TRUE(err.ok()) << err.what();
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

	rx.Stop();
}

TEST_F(RPCClientTestApi, Reconnect) {
	// CoroReindexer should be able to handle reconnect properly
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn(exceptionWrapper([this, &loop] {
		constexpr auto kDataCount = 2;
		const std::string kNsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		CreateNamespace(rx, kNsName);
		FillData(rx, kNsName, 0, kDataCount);

		ReconnectTest(rx, *this, kDataCount, kNsName);
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, ReconnectSyncCoroRx) {
	// Reindexer should be able to handle reconnect properly
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn(exceptionWrapper([this, &loop] {
		constexpr auto kDataCount = 2;
		const std::string kNsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		{
			CoroReindexer crx;
			auto err = crx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
			ASSERT_TRUE(err.ok()) << err.what();
			CreateNamespace(crx, kNsName);
			FillData(crx, kNsName, 0, kDataCount);
		}

		client::Reindexer rx;
		auto err = rx.Connect(dsn);
		ASSERT_TRUE(err.ok()) << err.what();

		ReconnectTest(rx, *this, kDataCount, kNsName);
	}));

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

		loop.spawn(exceptionWrapper([&loop, &terminate, &ready, &step] {
			const std::string nsName = "ns1";
			const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
			CoroReindexer rx;
			auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
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
								case Step::ShutdownInProgress:
								case Step::RestartInProgress:;	// No additional checks in transition states
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

			rx.Stop();
		}));

		loop.run();
	});
	while (!ready) {  // -V776
		std::this_thread::sleep_for(std::chrono::milliseconds(100));
	}

	// Shutdown server
	step = Step::ShutdownInProgress;
	Error err = StopServer();
	ASSERT_TRUE(err.ok()) << err.what();
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
	// Temporary namespace must be automatically removed after disconnect
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop] {
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
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
		rx.Stop();
		err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		// Allow server to handle disconnect
		std::this_thread::sleep_for(std::chrono::seconds(2));

		// Check if namespace was removed
		nsList.clear();
		err = rx.EnumNamespaces(nsList, EnumNamespacesOpts().OnlyNames().HideSystem());
		ASSERT_TRUE(err.ok()) << err.what();
		if (nsList.size() > 0) {
			for (auto& ns : nsList) {
				std::cerr << ns.name << std::endl;
			}
			ASSERT_TRUE(false);
		}

		rx.Stop();
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, ItemJSONWithDouble) {
	ev::dynamic_loop loop;
	loop.spawn(exceptionWrapper([&loop] {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
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
	}));
	loop.run();
}

TEST_F(RPCClientTestApi, UnknownResultsFlag) {
	// Check if server will not return unknown result flag
	StartDefaultRealServer();
	ev::dynamic_loop loop;
	bool finished = false;
	loop.spawn([&loop, &finished] {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/db1", loop,
							  reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		const int kResultsUnknownFlag = 0x40000000;	 // Max available int flag
		client::CoroQueryResults qr(kResultsCJson | kResultsWithItemID | kResultsUnknownFlag);
		err = rx.Select(Query(reindexer::kConfigNamespace).Where("type", CondEq, "namespaces"), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		// Check, that kResultsUnknownFlag was not sent back
		ASSERT_EQ(qr.GetFlags(), kResultsCJson | kResultsWithItemID);
		ASSERT_EQ(qr.Count(), 1);
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(RPCClientTestApi, FirstSelectWithFetch) {
	StartDefaultRealServer();
	ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([this, &loop] {
		constexpr auto kDataCount = 15000;
		const std::string kNsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		{
			client::CoroReindexer crx;
			auto err = crx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
			ASSERT_TRUE(err.ok()) << err.what();
			CreateNamespace(crx, kNsName);
			FillData(crx, kNsName, 0, kDataCount);
		}
		{
			reindexer::client::ConnectOpts opts;
			client::CoroReindexer rxs;
			auto err = rxs.Connect(dsn, loop, opts);
			ASSERT_TRUE(err.ok()) << err.what();
			client::CoroQueryResults res;
			err = rxs.ExecSQL("Select * from " + kNsName + " order by id", res);
			ASSERT_TRUE(err.ok()) << err.what();
			size_t idCounter = 0;
			for (auto i : res) {
				ASSERT_TRUE(i.Status().ok());
				WrSerializer ser;
				err = i.GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				ASSERT_EQ(ser.Slice(), "{\"id\":" + std::to_string(idCounter) + "}");
				idCounter++;
			}
		}
		{
			client::ConnectOpts opts;
			client::CoroReindexer rxs;
			auto err = rxs.Connect(dsn, loop, opts);
			ASSERT_TRUE(err.ok()) << err.what();
			client::Snapshot snapshot;
			err = rxs.GetSnapshot(kNsName, SnapshotOpts(), snapshot);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_GT(snapshot.Size(), 0);
			for (auto s : snapshot) {
				const SnapshotChunk& chunk = s.Chunk();
				const std::vector<SnapshotRecord>& rec = chunk.Records();
				ASSERT_GT(rec.size(), 0);
			}
		}
		{
			client::ConnectOpts opts;
			client::CoroReindexer rxs;
			auto err = rxs.Connect(dsn, loop, opts);
			ASSERT_TRUE(err.ok()) << err.what();
			client::CoroTransaction tr = rxs.NewTransaction(kNsName);
			const int kTrItemCount = 10;
			for (int ti = 0; ti < kTrItemCount; ti++) {
				auto item = tr.NewItem();
				reindexer::WrSerializer wrser;
				reindexer::JsonBuilder jb(wrser);
				jb.Put("id", ti + 100000);
				jb.End();
				err = item.FromJSON(wrser.Slice());
				ASSERT_TRUE(err.ok()) << err.what();
				err = tr.Insert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			client::CoroQueryResults res;
			err = rxs.CommitTransaction(tr, res);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(res.Count(), kTrItemCount);
		}
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, FetchingWithJoin) {
	// Check that particular results fetching does not break tagsmatchers
	using namespace reindexer::client;
	using namespace reindexer::net::ev;
	using reindexer::coroutine::wait_group;
	using reindexer::coroutine::wait_group_guard;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop] {
		const std::string kLeftNsName = "left_ns";
		const std::string kRightNsName = "right_ns";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ReindexerConfig cfg;
		constexpr auto kFetchCount = 50;
		constexpr auto kNsSize = kFetchCount * 3;
		cfg.FetchAmount = kFetchCount;
		CoroReindexer rx(cfg);
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		err = rx.OpenNamespace(kLeftNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(kLeftNsName, {"id", {"id"}, "tree", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		err = rx.OpenNamespace(kRightNsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddIndex(kRightNsName, {"id", {"id"}, "hash", "int", IndexOpts().PK()});
		ASSERT_TRUE(err.ok()) << err.what();

		auto upsertFn = [&rx](const std::string& nsName, bool withValue) {
			for (size_t i = 0; i < kNsSize; ++i) {
				auto item = rx.NewItem(nsName);
				ASSERT_TRUE(item.Status().ok()) << nsName << " " << item.Status().what();

				WrSerializer wrser;
				JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", i);
				if (withValue) {
					jsonBuilder.Put("value", "value_" + std::to_string(i));
				}
				jsonBuilder.End();
				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << nsName << " " << err.what();
				err = rx.Upsert(nsName, item);
				ASSERT_TRUE(err.ok()) << nsName << " " << err.what();
			}
		};

		upsertFn(kLeftNsName, false);
		upsertFn(kRightNsName, true);

		client::CoroQueryResults qr;
		err = rx.Select(Query(kLeftNsName).Join(InnerJoin, Query(kRightNsName)).On("id", CondEq, "id").Sort("id", false), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), kNsSize);
		WrSerializer ser;
		unsigned i = 0;
		for (auto& it : qr) {
			ser.Reset();
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			const auto expected = fmt::format(R"json({{"id":{},"joined_{}":[{{"id":{},"value":"value_{}"}}]}})json", i, kRightNsName, i, i);
			EXPECT_EQ(ser.Slice(), expected);
			i++;
		}
		rx.Stop();
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, QRWithMultipleIterationLoops) {
	// Check if iterator has error status if user attempts to iterate over qrs, which were already fetched
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, this] {
		const std::string kNsName = "QRWithMultipleIterationLoops";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		client::ReindexerConfig cfg;
		constexpr auto kFetchCount = 50;
		constexpr auto kNsSize = kFetchCount * 3;
		cfg.FetchAmount = kFetchCount;
		CoroReindexer rx(cfg);
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		CreateNamespace(rx, kNsName);
		FillData(rx, kNsName, 0, kNsSize);

		client::CoroQueryResults qr;
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
				EXPECT_EQ(fmt::format("{{\"id\":{}}}", id), ser.Slice());
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
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, AggregationsFetching) {
	// Validate, that distinct results will remain valid after query results fetching.
	// Actual aggregation values will only be sent for initial 'select', but must be available at any point in iterator's lifetime.
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;
	constexpr unsigned kItemsCount = 100;
	constexpr unsigned kFetchLimit = kItemsCount / 5;

	loop.spawn(exceptionWrapper([&loop, this, kItemsCount] {
		const std::string nsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		client::ReindexerConfig cfg;
		cfg.FetchAmount = kFetchLimit;
		CoroReindexer rx(cfg);
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		CreateNamespace(rx, nsName);
		FillData(rx, nsName, 0, kItemsCount);

		{
			reindexer::client::CoroQueryResults qr;
			const auto q = Query(nsName).Distinct("id").ReqTotal().Explain();
			err = rx.Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kItemsCount);
			const auto initialAggs = qr.GetAggregationResults();
			ASSERT_EQ(initialAggs.size(), 2);
			ASSERT_EQ(initialAggs[0].GetType(), AggDistinct);
			ASSERT_EQ(initialAggs[1].GetType(), AggCount);
			const std::string explain = qr.GetExplainResults();
			ASSERT_GT(explain.size(), 0);
			WrSerializer wser;
			initialAggs[0].GetJSON(wser);
			initialAggs[1].GetJSON(wser);
			const std::string initialAggJSON(wser.Slice());
			for (auto& it : qr) {
				ASSERT_TRUE(it.Status().ok()) << it.Status().what();
				auto& aggs = qr.GetAggregationResults();
				ASSERT_EQ(aggs.size(), 2);
				wser.Reset();
				aggs[0].GetJSON(wser);
				aggs[1].GetJSON(wser);
				EXPECT_EQ(initialAggJSON, wser.Slice()) << q.GetSQL();
				EXPECT_EQ(qr.TotalCount(), kItemsCount);
				EXPECT_EQ(explain, qr.GetExplainResults());
			}
		}

		rx.Stop();
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, AggregationsFetchingWithLazyMode) {
	// Validate, that distinct results will remain valid after query results fetching in lazy mode
	// Actual aggregation values will be sent for initial 'select' only, but must be available at any point of iterator's lifetime.
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;
	constexpr unsigned kItemsCount = 100;
	constexpr unsigned kFetchLimit = kItemsCount / 5;

	loop.spawn(exceptionWrapper([&loop, this, kItemsCount] {
		const std::string nsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		client::ReindexerConfig cfg;
		cfg.FetchAmount = kFetchLimit;
		CoroReindexer rx(cfg);
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		CreateNamespace(rx, nsName);
		FillData(rx, nsName, 0, kItemsCount);

		{
			// Aggregation and explain will be available, if first access was performed before fetching
			CoroQueryResults qr(0, 0, client::LazyQueryResultsMode{});
			const auto q = Query(nsName).Distinct("id").ReqTotal().Explain();
			err = rx.Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kItemsCount);
			const auto initialAggs = qr.GetAggregationResults();
			ASSERT_EQ(initialAggs.size(), 2);
			ASSERT_EQ(initialAggs[0].GetType(), AggDistinct);
			ASSERT_EQ(initialAggs[1].GetType(), AggCount);
			const std::string explain = qr.GetExplainResults();
			ASSERT_GT(explain.size(), 0);
			WrSerializer wser;
			initialAggs[0].GetJSON(wser);
			initialAggs[1].GetJSON(wser);
			const std::string initialAggJSON(wser.Slice());
			for (auto& it : qr) {
				ASSERT_TRUE(it.Status().ok()) << it.Status().what();
				auto& aggs = qr.GetAggregationResults();
				ASSERT_EQ(aggs.size(), 2);
				wser.Reset();
				aggs[0].GetJSON(wser);
				aggs[1].GetJSON(wser);
				EXPECT_EQ(initialAggJSON, wser.Slice()) << q.GetSQL();
				EXPECT_EQ(qr.TotalCount(), kItemsCount);
				EXPECT_EQ(explain, qr.GetExplainResults());
			}
		}
		{
			// Aggregation and explain will throw exception, if first access was performed after fetching
			CoroQueryResults qr(0, 0, client::LazyQueryResultsMode{});
			const auto q = Query(nsName).Distinct("id").ReqTotal().Explain();
			err = rx.Select(q, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kItemsCount);
			unsigned i = 0;
			for (auto& it : qr) {
				ASSERT_TRUE(it.Status().ok()) << it.Status().what();
				if (i++ > kFetchLimit) {
					break;
				}
			}
			// NOLINTNEXTLINE (bugprone-unused-return-value)
			EXPECT_THROW(qr.GetAggregationResults(), Error);
			// NOLINTNEXTLINE (bugprone-unused-return-value)
			EXPECT_THROW(qr.GetExplainResults(), Error);
			EXPECT_EQ(qr.TotalCount(), kItemsCount);  // Total count is still available
		}

		rx.Stop();
	}));

	loop.run();
}
TEST_F(RPCClientTestApi, AggregationsWithStrictModeTest) {
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop] {
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ReindexerConfig cfg;
		auto rx = std::make_unique<CoroReindexer>(cfg);
		auto err = rx->Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		QueryAggStrictModeTest(rx);
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, SubQuery) {
	StartDefaultRealServer();
	reindexer::net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, this] {
		const std::string kLeftNsName = "left_ns";
		const std::string kRightNsName = "right_ns";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::ReindexerConfig cfg;
		constexpr auto kFetchCount = 50;
		constexpr auto kNsSize = kFetchCount * 3;
		cfg.FetchAmount = kFetchCount;
		reindexer::client::CoroReindexer rx(cfg);
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		CreateNamespace(rx, kLeftNsName);
		CreateNamespace(rx, kRightNsName);

		auto upsertFn = [&rx](const std::string& nsName) {
			for (size_t i = 0; i < kNsSize; ++i) {
				auto item = rx.NewItem(nsName);
				ASSERT_TRUE(item.Status().ok()) << nsName << " " << item.Status().what();

				WrSerializer wrser;
				JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", i);
				jsonBuilder.Put("value", "value_" + std::to_string(i));
				jsonBuilder.End();
				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << nsName << " " << err.what();
				err = rx.Upsert(nsName, item);
				ASSERT_TRUE(err.ok()) << nsName << " " << err.what();
			}
		};

		upsertFn(kLeftNsName);
		upsertFn(kRightNsName);

		const auto kHalfSize = kNsSize / 2;
		{
			client::CoroQueryResults qr;
			err = rx.Select(Query(kLeftNsName).Where("id", CondSet, Query(kRightNsName).Select({"id"}).Where("id", CondLt, kHalfSize)), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kHalfSize);
		}
		{
			const int limit = 10;
			client::CoroQueryResults qr;
			err = rx.Select(
				Query(kLeftNsName).Where(Query(kRightNsName).Where("id", CondLt, kHalfSize).ReqTotal(), CondEq, {kHalfSize}).Limit(limit),
				qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), limit);
		}
		rx.Stop();
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, CoroTransactionInsertWithPrecepts) {
	StartDefaultRealServer();
	reindexer::net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, this] {
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		const std::string kNsName = "TestCoroInsertWithPrecepts";
		CreateNamespace(rx, kNsName);

		constexpr int kNsSize = 5;

		auto insertFn = [&rx](const std::string& nsName, int count) {
			std::vector<std::string> precepts = {"id=SERIAL()"};

			auto tx = rx.NewTransaction(nsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();

			for (int i = 0; i < count; ++i) {
				auto item = tx.NewItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				WrSerializer wrser;
				JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", 100);
				jsonBuilder.End();

				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << err.what();

				item.SetPrecepts(precepts);

				err = tx.Insert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			client::CoroQueryResults qr;
			auto err = rx.CommitTransaction(tx, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), count);
		};

		insertFn(kNsName, kNsSize);

		{
			client::CoroQueryResults qr;
			err = rx.Select(Query(kNsName), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kNsSize);
			for (auto& it : qr) {
				ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			}
		}

		rx.Stop();
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, QuerySelectDWithin) {
	StartDefaultRealServer();
	reindexer::net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, this] {
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		const std::string kNsName = "TestQuerySelectDWithin";
		CreateNamespace(rx, kNsName);
		err = rx.AddIndex(kNsName, {"point", "rtree", "point", IndexOpts().RTreeType(IndexOpts::RStar)});
		ASSERT_TRUE(err.ok()) << err.what();

		constexpr int kNsSize = 5;

		auto insertFn = [&rx](const std::string& nsName, int count) {
			auto tx = rx.NewTransaction(nsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();

			for (int i = 0; i < count; ++i) {
				auto item = tx.NewItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				WrSerializer wrser;
				JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", i);
				jsonBuilder.Put("point", Variant{VariantArray::Create({i, i})});
				jsonBuilder.End();

				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << err.what();

				err = tx.Insert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			reindexer::client::CoroQueryResults qr;
			auto err = rx.CommitTransaction(tx, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), count);
		};

		insertFn(kNsName, kNsSize);

		{
			client::CoroQueryResults qr;
			err = rx.Select(Query(kNsName).DWithin("point", Point(3, 2.5), 2.2), qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), 3);

			WrSerializer ser;
			unsigned i = 2;
			for (auto& it : qr) {
				ser.Reset();
				ASSERT_TRUE(it.Status().ok()) << it.Status().what();
				err = it.GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				const auto expected = fmt::format(R"json({{"id":{},"point":[{:.1f},{:.1f}]}})json", i, float(i), float(i));
				EXPECT_EQ(ser.Slice(), expected);
				++i;
			}
		}

		rx.Stop();
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, QuerySelectFunctions) {
	StartDefaultRealServer();
	reindexer::net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, this] {
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		const std::string kNsName = "TestQuerySelectFunctions";
		CreateNamespace(rx, kNsName);
		err = rx.AddIndex(kNsName, reindexer::IndexDef{"ft", {"ft"}, "text", "string", IndexOpts{}});
		ASSERT_TRUE(err.ok()) << err.what();

		const std::array<std::string_view, 3> content = {"one word", "sword two", "three work 333"};

		auto insertFn = [&rx, &content](const std::string& nsName) {
			auto tx = rx.NewTransaction(nsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();

			unsigned i = 0;
			for (auto text : content) {
				auto item = tx.NewItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				WrSerializer wrser;
				JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", i);
				jsonBuilder.Put("ft", text);
				jsonBuilder.End();

				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << err.what();

				err = tx.Insert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
				++i;
			}
			reindexer::client::CoroQueryResults qr;
			auto err = rx.CommitTransaction(tx, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), content.size());
		};

		insertFn(kNsName);

		{
			client::CoroQueryResults qr;
			auto query = Query(kNsName).Where("ft", CondEq, "word~");
			query.AddFunction(R"(ft=highlight(<,>))");
			query.AddFunction(R"(ft=highlight(!!,!))");
			err = rx.Select(query, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), content.size());

			const std::array<std::string, 3> expected_content = {"one <word>", "<sword> two", "three <work> 333"};

			WrSerializer ser;
			std::string expected;
			unsigned i = 0;
			for (auto& it : qr) {
				ser.Reset();
				ASSERT_TRUE(it.Status().ok()) << it.Status().what();
				err = it.GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				expected = R"({"id":)" + std::to_string(i) + R"(,"ft":")" + expected_content[i] + R"("})";
				EXPECT_EQ(ser.Slice(), expected);
				++i;
			}
		}

		rx.Stop();
	}));

	loop.run();
}

TEST_F(RPCClientTestApi, QuerySetObjectUpdate) {
	StartDefaultRealServer();
	reindexer::net::ev::dynamic_loop loop;

	loop.spawn(exceptionWrapper([&loop, this] {
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(dsn, loop, reindexer::client::ConnectOpts().CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		constexpr std::string_view kNsName = "TestQuerySetObjectUpdate";
		CreateNamespace(rx, kNsName);
		err = rx.AddIndex(kNsName, reindexer::IndexDef{"idx", {"nested.field"}, "hash", "int", IndexOpts{}});
		ASSERT_TRUE(err.ok()) << err.what();

		constexpr unsigned kNsSize = 3;

		auto insertFn = [&rx](std::string_view nsName, unsigned count) {
			auto tx = rx.NewTransaction(nsName);
			ASSERT_TRUE(tx.Status().ok()) << tx.Status().what();

			for (unsigned i = 0; i < count; ++i) {
				auto item = tx.NewItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				WrSerializer wrser;
				JsonBuilder jsonBuilder(wrser, ObjType::TypeObject);
				jsonBuilder.Put("id", i);
				jsonBuilder.Put("nested", R"({"field": 1891})");
				jsonBuilder.End();

				char* endp = nullptr;
				auto err = item.Unsafe().FromJSON(wrser.Slice(), &endp);
				ASSERT_TRUE(err.ok()) << err.what();

				err = tx.Insert(std::move(item));
				ASSERT_TRUE(err.ok()) << err.what();
			}
			reindexer::client::CoroQueryResults qr;
			auto err = rx.CommitTransaction(tx, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), count);
		};

		insertFn(kNsName, kNsSize);

		client::CoroQueryResults qr;
		{
			err = rx.Update(Query(kNsName).Where("id", CondGe, "0").SetObject("nested", Variant(std::string(R"([{"field": 1240}])"))), qr);
			ASSERT_FALSE(err.ok());
			EXPECT_STREQ(err.what(), "Error modifying field value: 'Unsupported JSON format. Unnamed field detected'");
		}

		{
			err = rx.Update(Query(kNsName).Where("id", CondGe, "0").SetObject("nested", Variant(std::string(R"({{"field": 1240}})"))), qr);
			ASSERT_FALSE(err.ok());
			EXPECT_STREQ(err.what(), "Error modifying field value: 'JSONDecoder: Error parsing json: unquoted key, pos 15'");
		}

		{
			// R"(UPDATE TestQuerySetObjectUpdate SET nested = {"field": 1240} where id >= 0)"
			auto query = Query(kNsName).Where("id", CondGe, "0").SetObject("nested", Variant(std::string(R"({"field": 1240})")));
			err = rx.Update(query, qr);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(qr.Count(), kNsSize);

			WrSerializer ser;
			std::string expected;
			unsigned i = 0;
			for (auto& it : qr) {
				ser.Reset();
				ASSERT_TRUE(it.Status().ok()) << it.Status().what();
				err = it.GetJSON(ser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				expected = R"({"id":)" + std::to_string(i) + R"(,"nested":{"field":1240}})";
				EXPECT_EQ(ser.Slice(), expected);
				++i;
			}
		}

		rx.Stop();
	}));

	loop.run();
}
