#include "clientsstats_api.h"
#include "core/system_ns_names.h"
#include "coroutine/waitgroup.h"
#include "gason/gason.h"
#include "reindexer_version.h"
#include "tools/clock.h"
#include "tools/semversion.h"
#include "tools/stringstools.h"

using reindexer::net::ev::dynamic_loop;
using reindexer::client::CoroReindexer;
using reindexer::client::CoroQueryResults;
using reindexer::client::CoroTransaction;
using reindexer::coroutine::wait_group;

TEST_F(ClientsStatsApi, ClientsStatsConcurrent) {
	// ClientsStats should work without races in concurrent environment
	RunServerInThread(true);
	dynamic_loop loop;
	bool finished = false;
	loop.spawn([this, &loop, &finished] {
		CoroReindexer reindexer;
		reindexer::client::ConnectOpts opts;
		auto err = reindexer.Connect(GetConnectionString(), loop, opts.CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		SetProfilingFlag(true, "profiling.activitystats", reindexer);

		RunNSelectThread(5, 5);
		RunNReconnectThread(10);
		loop.sleep(std::chrono::seconds(5));
		StopThreads();
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(ClientsStatsApi, ClientsStatsData) {
	// Should be able to get data from #clientsstats for each connection
	RunServerInThread(true);
	dynamic_loop loop;
	bool finished = false;
	loop.spawn([this, &loop, &finished] {
		const size_t kConnectionCount = 10;
		std::vector<std::unique_ptr<CoroReindexer>> nClients;
		nClients.reserve(kConnectionCount);
		wait_group wg;
		for (size_t i = 0; i < kConnectionCount; i++) {
			loop.spawn(wg, [this, &loop, &nClients] {
				std::unique_ptr<CoroReindexer> clientPtr(new CoroReindexer);
				reindexer::client::ConnectOpts opts;
				auto err = clientPtr->Connect(GetConnectionString(), loop, opts.CreateDBIfMissing());
				ASSERT_TRUE(err.ok()) << err.what();
				CoroQueryResults result;
				err = clientPtr->Select(reindexer::Query(reindexer::kNamespacesNamespace), result);
				ASSERT_TRUE(err.ok()) << err.what();
				nClients.emplace_back(std::move(clientPtr));
			});
		}
		wg.wait();
		CoroQueryResults result;
		auto err = nClients[0]->Select(reindexer::Query(reindexer::kClientsStatsNamespace), result);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(result.Count(), kConnectionCount);
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(ClientsStatsApi, ClientsStatsOff) {
	// Should get empty result if #clientsstats are disabled
	RunServerInThread(false);
	dynamic_loop loop;
	bool finished = false;
	loop.spawn([this, &loop, &finished] {
		CoroReindexer reindexer;
		reindexer::client::ConnectOpts opts;
		auto err = reindexer.Connect(GetConnectionString(), loop, opts.CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();
		CoroQueryResults resultNs;
		err = reindexer.Select(reindexer::Query(reindexer::kNamespacesNamespace), resultNs);
		ASSERT_TRUE(err.ok()) << err.what();
		CoroQueryResults resultCs;
		err = reindexer.Select(reindexer::Query(reindexer::kClientsStatsNamespace), resultCs);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(resultCs.Count(), 0);
		finished = true;
	});
	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(ClientsStatsApi, ClientsStatsValues) {
	// Should get correct data for specific connection
	RunServerInThread(true);
	dynamic_loop loop;
	bool finished = false;

	loop.spawn([this, &loop, &finished] {
		reindexer::client::ReindexerConfig config;
		config.AppName = kAppName;
		CoroReindexer reindexer(config);
		reindexer::client::ConnectOpts opts;
		auto err = reindexer.Connect(GetConnectionString(), loop, opts.CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		std::string nsName("ns1");
		err = reindexer.OpenNamespace(nsName);
		ASSERT_TRUE(err.ok()) << err.what();

		auto tx1 = reindexer.NewTransaction(nsName);
		ASSERT_FALSE(tx1.IsFree());
		auto tx2 = reindexer.NewTransaction(nsName);
		ASSERT_FALSE(tx2.IsFree());

		auto beginTs = std::chrono::duration_cast<std::chrono::milliseconds>(reindexer::system_clock_w::now().time_since_epoch()).count();
		loop.sleep(std::chrono::milliseconds(2000));  // Timeout to update send/recv rate
		CoroQueryResults resultNs;
		err = reindexer.Select(reindexer::Query(reindexer::kNamespacesNamespace), resultNs);
		SetProfilingFlag(true, "profiling.activitystats", reindexer);

		CoroQueryResults resultCs;
		err = reindexer.Select(reindexer::Query(reindexer::kClientsStatsNamespace), resultCs);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(resultCs.Count(), 1);
		auto it = resultCs.begin();
		reindexer::WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		auto endTs = std::chrono::duration_cast<std::chrono::milliseconds>(reindexer::system_clock_w::now().time_since_epoch()).count();

		gason::JsonParser parser;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		std::string curActivity = clientsStats["current_activity"].As<std::string>();
		EXPECT_TRUE(curActivity == "SELECT * FROM #clientsstats") << "curActivity = [" << curActivity << "]";
		std::string curIP = clientsStats["ip"].As<std::string>();
		std::vector<std::string> addrParts;
		std::ignore = reindexer::split(curIP, ":", false, addrParts);
		EXPECT_EQ(addrParts.size(), 2);
		EXPECT_EQ(addrParts[0], kipaddress) << curIP;
		int port = std::atoi(addrParts[1].c_str());
		EXPECT_GT(port, 0) << curIP;
		EXPECT_NE(port, kPortI) << curIP;
		int64_t sentBytes = clientsStats["sent_bytes"].As<int64_t>();
		EXPECT_GT(sentBytes, 0);
		int64_t recvBytes = clientsStats["recv_bytes"].As<int64_t>();
		EXPECT_GT(recvBytes, 0);
		std::string userName = clientsStats["user_name"].As<std::string>();
		EXPECT_EQ(userName, kUserName);
		std::string dbName = clientsStats["db_name"].As<std::string>();
		EXPECT_EQ(dbName, kdbName);
		std::string appName = clientsStats["app_name"].As<std::string>();
		EXPECT_EQ(appName, kAppName);
		std::string userRights = clientsStats["user_rights"].As<std::string>();
		EXPECT_EQ(userRights, "owner");
		std::string clientVersion = clientsStats["client_version"].As<std::string>();
		EXPECT_EQ(clientVersion, reindexer::SemVersion(REINDEX_VERSION).StrippedString());
		uint32_t txCount = clientsStats["tx_count"].As<uint32_t>();
		EXPECT_EQ(txCount, 2);
		int64_t sendBufBytes = clientsStats["send_buf_bytes"].As<int64_t>(-1);
		EXPECT_EQ(sendBufBytes, 0);
		int64_t sendRate = clientsStats["send_rate"].As<int64_t>();
		EXPECT_GT(sendRate, 0);
		int64_t recvRate = clientsStats["recv_rate"].As<int64_t>();
		EXPECT_GT(recvRate, 0);
		int64_t lastSendTs = clientsStats["last_send_ts"].As<int64_t>();
		EXPECT_GT(lastSendTs, beginTs);
		EXPECT_LE(lastSendTs, endTs);
		int64_t lastRecvTs = clientsStats["last_recv_ts"].As<int64_t>();
		EXPECT_GT(lastRecvTs, beginTs);
		EXPECT_LE(lastRecvTs, endTs);

		CoroQueryResults qr1;
		err = reindexer.CommitTransaction(tx1, qr1);
		ASSERT_TRUE(err.ok()) << err.what();
		CoroQueryResults qr2;
		err = reindexer.CommitTransaction(tx2, qr2);
		ASSERT_TRUE(err.ok()) << err.what();

		finished = true;
	});

	loop.run();
	ASSERT_TRUE(finished);
}

TEST_F(ClientsStatsApi, TxCountLimitation) {
	// Should get correct data about running client's transactions
	RunServerInThread(true);
	dynamic_loop loop;
	bool finished = false;
	loop.spawn([this, &loop, &finished] {
		const size_t kMaxTxCount = 1024;
		CoroReindexer reindexer;
		reindexer::client::ConnectOpts opts;
		auto err = reindexer.Connect(GetConnectionString(), loop, opts.CreateDBIfMissing());
		ASSERT_TRUE(err.ok()) << err.what();

		std::string nsName("ns1");
		err = reindexer.OpenNamespace(nsName);
		ASSERT_TRUE(err.ok()) << err.what();

		std::vector<CoroTransaction> txs;
		txs.reserve(kMaxTxCount);
		for (size_t i = 0; i < 2 * kMaxTxCount; ++i) {
			auto tx = reindexer.NewTransaction(nsName);
			if (tx.Status().ok()) {
				ASSERT_FALSE(tx.IsFree());
				txs.emplace_back(std::move(tx));
			}
		}

		ASSERT_EQ(txs.size(), kMaxTxCount);
		ASSERT_EQ(StatsTxCount(reindexer), kMaxTxCount);

		CoroQueryResults qrDummy;
		for (size_t i = 0; i < kMaxTxCount / 2; ++i) {
			if (i % 2) {
				CoroQueryResults qr;
				err = reindexer.CommitTransaction(txs[i], qr);
			} else {
				err = reindexer.RollBackTransaction(txs[i]);
			}
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i;
		}

		for (size_t i = 0; i < kMaxTxCount / 4; ++i) {
			auto tx = reindexer.NewTransaction(nsName);
			ASSERT_FALSE(tx.IsFree());
			ASSERT_TRUE(tx.Status().ok());
			txs[i] = std::move(tx);
		}
		ASSERT_EQ(StatsTxCount(reindexer), kMaxTxCount / 2 + kMaxTxCount / 4);
		for (size_t i = 0; i < txs.size(); ++i) {
			if (!txs[i].IsFree() && txs[i].Status().ok()) {
				if (i % 2) {
					CoroQueryResults qr;
					err = reindexer.CommitTransaction(txs[i], qr);
				} else {
					err = reindexer.RollBackTransaction(txs[i]);
				}
				ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i;
			}
		}
		ASSERT_EQ(StatsTxCount(reindexer), 0);
		finished = true;
	});

	loop.run();
	ASSERT_TRUE(finished);
}
