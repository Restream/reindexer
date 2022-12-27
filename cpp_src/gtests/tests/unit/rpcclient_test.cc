#include <chrono>
#include <condition_variable>
#include <thread>
#include "rpcclient_api.h"
#include "rpcserver_fake.h"
#include "tools/fsops.h"

#include "client/reindexer.h"
#include "client/snapshot.h"
#include "core/cjson/jsonbuilder.h"
#include "coroutine/waitgroup.h"
#include "gtests/tests/gtest_cout.h"
#include "net/ev/ev.h"
#include "reindexertestapi.h"

using std::chrono::seconds;

TEST_F(RPCClientTestApi, CoroRequestTimeout) {
	// Should return error on request timeout
	RPCServerConfig conf;
	conf.loginDelay = std::chrono::seconds(0);
	conf.openNsDelay = std::chrono::seconds(4);
	AddFakeServer(kDefaultRPCServerAddr, conf);
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::ReindexerConfig config;
		config.NetTimeout = seconds(1);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		EXPECT_TRUE(err.ok()) << err.what();
		const std::string kNamespaceName = "MyNamespace";
		err = rx.AddNamespace(reindexer::NamespaceDef(kNamespaceName));
		EXPECT_EQ(err.code(), errTimeout);
		loop.sleep(std::chrono::seconds(4));
		err = rx.DropNamespace(kNamespaceName);
		EXPECT_TRUE(err.ok()) << err.what();
	});
	loop.run();
	Error err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
}

static std::chrono::seconds GetMaxTimeForCoroSelectTimeout(unsigned requests, std::chrono::seconds delay) {
	const auto cpus = std::thread::hardware_concurrency();
	const auto kBase = std::max(requests * delay.count() / 16, delay.count());
	const std::chrono::seconds kDefaultMaxTime(kBase + 10);
	if (cpus == 0) {
		TestCout() << fmt::sprintf("Unable to get CPUs count. Using test max time %d seconds Test may flack in this case",
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
	TestCout() << fmt::sprintf("Test max time: %d seconds for %d total requests on %d CPUs with %d seconds of delay for each request",
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
		ASSERT_TRUE(false) << fmt::sprintf("Test deadline exceeded. Closed count: %d. Expected: %d. %d|", server.CloseQRRequestsCount(),
										   kCorCount * kQueriesCount, std::chrono::steady_clock::now().time_since_epoch().count());
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
			EXPECT_TRUE(err.ok()) << err.what();
			finished[index] = true;
		});
	}
	loop.run();
	for (size_t i = 0; i < kCorCount; ++i) {
		EXPECT_TRUE(finished[i]);
	}
	Error const err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, CoroRequestCancels) {
	// Should return error on request cancel
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
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
	Error err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, CoroSuccessfullRequestWithTimeout) {
	// Should be able to execute some basic requests with timeout
	AddFakeServer();
	StartServer();
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::ReindexerConfig config;
		config.NetTimeout = seconds(6);
		reindexer::client::CoroReindexer rx(config);
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		EXPECT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_TRUE(err.ok()) << err.what();
	});
	loop.run();
	Error err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
}

TEST_F(RPCClientTestApi, CoroErrorLoginResponse) {
	// Should return error on failed Login
	AddFakeServer();
	StartServer(kDefaultRPCServerAddr, errForbidden);
	ev::dynamic_loop loop;
	loop.spawn([&loop]() noexcept {
		reindexer::client::CoroReindexer rx;
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/test_db", loop);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rx.AddNamespace(reindexer::NamespaceDef("MyNamespace"));
		EXPECT_EQ(err.code(), errForbidden);
	});
	loop.run();
	Error err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
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
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/db1", loop, opts);
		ASSERT_TRUE(err.ok()) << err.what();
		for (size_t i = 0; i < 5; ++i) {
			StartServer();
			err = rx.Status();
			ASSERT_TRUE(err.ok()) << err.what();
			err = StopServer();
			EXPECT_TRUE(err.ok()) << err.what();
			loop.sleep(std::chrono::milliseconds(20));	// Allow reading coroutine to handle disconnect
			err = rx.Status();
			ASSERT_EQ(err.code(), errNetwork) << err.what();
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
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
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
	Error err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
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
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
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
	// Reindexer should be able to handle reconnect properly
	using namespace reindexer::client;
	using namespace reindexer::net::ev;

	StartDefaultRealServer();
	dynamic_loop loop;

	loop.spawn([this, &loop]() noexcept {
		constexpr auto kDataCount = 2;
		const std::string kNsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		{
			reindexer::client::ConnectOpts opts;
			opts.CreateDBIfMissing();
			CoroReindexer crx;
			auto err = crx.Connect(dsn, loop, opts);
			ASSERT_TRUE(err.ok()) << err.what();
			CreateNamespace(crx, kNsName);
			FillData(crx, kNsName, 0, kDataCount);
		}

		client::Reindexer rx;
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
			const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
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
	Error err = StopServer();
	EXPECT_TRUE(err.ok()) << err.what();
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
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
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
		auto err = rx.Connect(std::string("cproto://") + kDefaultRPCServerAddr + "/db1", loop, opts);
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

TEST_F(RPCClientTestApi, FirstSelectWithFetch) {
	StartDefaultRealServer();
	ev::dynamic_loop loop;

	loop.spawn([this, &loop]() noexcept {
		constexpr auto kDataCount = 15000;
		const std::string kNsName = "ns1";
		const std::string dsn = "cproto://" + kDefaultRPCServerAddr + "/db1";
		{
			reindexer::client::ConnectOpts opts;
			opts.CreateDBIfMissing();
			client::CoroReindexer crx;
			auto err = crx.Connect(dsn, loop, opts);
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
			rxs.Select("Select * from " + kNsName + " order by id", res);
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
	});

	loop.run();
}
