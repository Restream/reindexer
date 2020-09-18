#include "clientsstats_api.h"
#include "gason/gason.h"
#include "reindexer_version.h"
#include "tools/semversion.h"
#include "tools/stringstools.h"

TEST_F(ClientsStatsApi, ClientsStatsConcurrent) {
	RunServerInThread(true);
	RunClient(4, 1);
	RunNSelectThread(10);
	RunNReconnectThread(10);
	std::this_thread::sleep_for(std::chrono::seconds(5));
	StopThreads();
}

TEST_F(ClientsStatsApi, ClientsStatsData) {
	RunServerInThread(true);
	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = 1;
	config.WorkerThreads = 1;
	const int kconnectionCount = 10;
	std::vector<std::unique_ptr<reindexer::client::Reindexer>> NClients;
	for (int i = 0; i < kconnectionCount; i++) {
		std::unique_ptr<reindexer::client::Reindexer> clientPtr(new reindexer::client::Reindexer(config));
		reindexer::client::ConnectOpts opts;
		opts.CreateDBIfMissing();
		auto err = clientPtr->Connect(GetConnectionString(), opts);
		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::client::QueryResults result;
		err = clientPtr->Select(reindexer::Query("#namespaces"), result);
		ASSERT_TRUE(err.ok()) << err.what();
		NClients.push_back(std::move(clientPtr));
	}
	reindexer::client::QueryResults result;
	auto err = NClients[0]->Select(reindexer::Query("#clientsstats"), result);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(result.Count(), kconnectionCount);
}

TEST_F(ClientsStatsApi, ClientsStatsOff) {
	RunServerInThread(false);
	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = 1;
	config.WorkerThreads = 1;
	reindexer::client::Reindexer reindexer(config);
	reindexer::client::ConnectOpts opts;
	opts.CreateDBIfMissing();
	auto err = reindexer.Connect(GetConnectionString(), opts);
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::client::QueryResults resultNs;
	err = reindexer.Select(reindexer::Query("#namespaces"), resultNs);
	reindexer::client::QueryResults resultCs;
	err = reindexer.Select(reindexer::Query("#clientsstats"), resultCs);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(resultCs.Count(), 0);
}

TEST_F(ClientsStatsApi, ClientsStatsValues) {
	RunServerInThread(true);
	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = 1;
	config.WorkerThreads = 1;
	config.AppName = kAppName;
	reindexer::client::Reindexer reindexer(config);
	reindexer::client::ConnectOpts opts;
	opts.CreateDBIfMissing();
	auto err = reindexer.Connect(GetConnectionString(), opts);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string nsName("ns1");
	err = reindexer.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	auto tx1 = reindexer.NewTransaction(nsName);
	ASSERT_FALSE(tx1.IsFree());
	auto tx2 = reindexer.NewTransaction(nsName);
	ASSERT_FALSE(tx2.IsFree());

	TestObserver observer;
	reindexer::UpdatesFilters filters;
	filters.AddFilter(nsName, reindexer::UpdatesFilters::Filter());
	err = reindexer.SubscribeUpdates(&observer, filters, SubscriptionOpts().IncrementSubscription());
	ASSERT_TRUE(err.ok()) << err.what();

	auto beginTs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
	std::this_thread::sleep_for(std::chrono::milliseconds(2000));  // Timeout to update send/recv rate
	reindexer::client::QueryResults resultNs;
	err = reindexer.Select(reindexer::Query("#namespaces"), resultNs);
	SetProfilingFlag(true, "profiling.activitystats", &reindexer);

	reindexer::client::QueryResults resultCs;
	const std::string selectClientsStats = "SELECT * FROM #clientsstats";
	err = reindexer.Select(selectClientsStats, resultCs);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(resultCs.Count(), 1);
	auto it = resultCs.begin();
	reindexer::WrSerializer wrser;
	err = it.GetJSON(wrser, false);
	ASSERT_TRUE(err.ok()) << err.what();
	auto endTs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();

	gason::JsonParser parser;
	gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
	std::string curActivity = clientsStats["current_activity"].As<std::string>();
	EXPECT_TRUE(curActivity == selectClientsStats) << "curActivity = [" << curActivity << "]";
	std::string curIP = clientsStats["ip"].As<std::string>();
	std::vector<std::string> addrParts;
	reindexer::split(curIP, ":", false, addrParts);
	EXPECT_EQ(addrParts.size(), 2);
	EXPECT_EQ(addrParts[0], kipaddress) << curIP;
	int port = std::atoi(addrParts[1].c_str());
	EXPECT_TRUE(port > 0) << curIP;
	EXPECT_NE(port, kPortI) << curIP;
	int64_t sentBytes = clientsStats["sent_bytes"].As<int64_t>();
	EXPECT_TRUE(sentBytes > 0) << "sentBytes = [" << sentBytes << "]";
	int64_t recvBytes = clientsStats["recv_bytes"].As<int64_t>();
	EXPECT_TRUE(recvBytes > 0) << "recvBytes = [" << recvBytes << "]";
	std::string userName = clientsStats["user_name"].As<std::string>();
	EXPECT_TRUE(userName == kUserName) << "userName =[" << userName << "]";
	std::string dbName = clientsStats["db_name"].As<std::string>();
	EXPECT_TRUE(dbName == kdbName) << "dbName =[" << dbName << "]";
	std::string appName = clientsStats["app_name"].As<std::string>();
	EXPECT_TRUE(appName == kAppName) << "appName =[" << appName << "]";
	std::string userRights = clientsStats["user_rights"].As<std::string>();
	EXPECT_TRUE(userRights == "owner") << "userRights =[" << userRights << "]";
	std::string clientVersion = clientsStats["client_version"].As<std::string>();
	EXPECT_TRUE(clientVersion == reindexer::SemVersion(REINDEX_VERSION).StrippedString()) << "clientVersion =[" << clientVersion << "]";
	uint32_t txCount = clientsStats["tx_count"].As<uint32_t>();
	EXPECT_TRUE(txCount == 2) << "tx_count =[" << txCount << "]";
	int64_t sendBufBytes = clientsStats["send_buf_bytes"].As<int64_t>(-1);
	EXPECT_TRUE(sendBufBytes == 0) << "sendBufBytes = [" << sendBufBytes << "]";
	int64_t pendedUpdates = clientsStats["pended_updates"].As<int64_t>(-1);
	EXPECT_TRUE(pendedUpdates == 0) << "pendedUpdates = [" << pendedUpdates << "]";
	int64_t sendRate = clientsStats["send_rate"].As<int64_t>();
	EXPECT_TRUE(sendRate > 0) << "sendRate = [" << sendRate << "]";
	int64_t recvRate = clientsStats["recv_rate"].As<int64_t>();
	EXPECT_TRUE(recvRate > 0) << "recvRate = [" << recvRate << "]";
	int64_t lastSendTs = clientsStats["last_send_ts"].As<int64_t>();
	EXPECT_TRUE(lastSendTs > beginTs) << "lastSendTs = [" << lastSendTs << "], beginTs = [" << beginTs << "]";
	EXPECT_TRUE(lastSendTs <= endTs) << "lastSendTs = [" << lastSendTs << "], endTs = [" << endTs << "]";
	int64_t lastRecvTs = clientsStats["last_recv_ts"].As<int64_t>();
	EXPECT_TRUE(lastRecvTs > beginTs) << "lastRecvTs = [" << lastRecvTs << "], beginTs = [" << beginTs << "]";
	EXPECT_TRUE(lastRecvTs <= endTs) << "lastRecvTs = [" << lastRecvTs << "], endTs = [" << endTs << "]";
	bool isSubscribed = clientsStats["is_subscribed"].As<bool>(false);
	EXPECT_EQ(isSubscribed, true);
	reindexer::UpdatesFilters resultFilters;
	resultFilters.FromJSON(clientsStats["updates_filter"]);
	EXPECT_EQ(resultFilters, filters);

	err = reindexer.CommitTransaction(tx1);
	ASSERT_TRUE(err.ok()) << err.what();
	err = reindexer.CommitTransaction(tx2);
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_F(ClientsStatsApi, UpdatesFilters) {
	RunServerInThread(true);
	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = 1;
	config.WorkerThreads = 1;
	reindexer::client::Reindexer reindexer(config);
	reindexer::client::ConnectOpts opts;
	opts.CreateDBIfMissing();
	auto err = reindexer.Connect(GetConnectionString(), opts);
	ASSERT_TRUE(err.ok()) << err.what();
	std::string ns1Name("ns1");
	err = reindexer.OpenNamespace(ns1Name);
	ASSERT_TRUE(err.ok()) << err.what();
	std::string ns2Name("ns2");
	err = reindexer.OpenNamespace(ns2Name);
	ASSERT_TRUE(err.ok()) << err.what();
	std::string ns3Name("ns3");
	err = reindexer.OpenNamespace(ns3Name);
	ASSERT_TRUE(err.ok()) << err.what();

	{
		reindexer::client::QueryResults resultCs;
		err = reindexer.Select(reindexer::Query("#clientsstats"), resultCs);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(resultCs.Count(), 1);
		auto it = resultCs.begin();
		reindexer::WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		bool isSubscribed = clientsStats["is_subscribed"].As<bool>(true);
		EXPECT_EQ(isSubscribed, false);
		reindexer::UpdatesFilters resultFilters;
		resultFilters.FromJSON(clientsStats["updates_filter"]);
		EXPECT_EQ(resultFilters, reindexer::UpdatesFilters());
	}

	TestObserver observer;
	reindexer::UpdatesFilters filters1;
	filters1.AddFilter(ns1Name, reindexer::UpdatesFilters::Filter());
	filters1.AddFilter(ns2Name, reindexer::UpdatesFilters::Filter());
	err = reindexer.SubscribeUpdates(&observer, filters1, SubscriptionOpts().IncrementSubscription());
	ASSERT_TRUE(err.ok()) << err.what();

	{
		reindexer::client::QueryResults resultCs;
		err = reindexer.Select(reindexer::Query("#clientsstats"), resultCs);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(resultCs.Count(), 1);
		auto it = resultCs.begin();
		reindexer::WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		bool isSubscribed = clientsStats["is_subscribed"].As<bool>(false);
		EXPECT_EQ(isSubscribed, true);
		reindexer::UpdatesFilters resultFilters;
		resultFilters.FromJSON(clientsStats["updates_filter"]);
		EXPECT_EQ(resultFilters, filters1);
	}

	reindexer::UpdatesFilters filters2;
	filters1.AddFilter(ns3Name, reindexer::UpdatesFilters::Filter());
	filters2.AddFilter(ns3Name, reindexer::UpdatesFilters::Filter());
	err = reindexer.SubscribeUpdates(&observer, filters2, SubscriptionOpts().IncrementSubscription());
	ASSERT_TRUE(err.ok()) << err.what();
	{
		reindexer::client::QueryResults resultCs;
		err = reindexer.Select(reindexer::Query("#clientsstats"), resultCs);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(resultCs.Count(), 1);
		auto it = resultCs.begin();
		reindexer::WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		bool isSubscribed = clientsStats["is_subscribed"].As<bool>(false);
		EXPECT_EQ(isSubscribed, true);
		reindexer::UpdatesFilters resultFilters;
		resultFilters.FromJSON(clientsStats["updates_filter"]);
		EXPECT_EQ(resultFilters, filters1);
	}

	err = reindexer.UnsubscribeUpdates(&observer);
	ASSERT_TRUE(err.ok()) << err.what();
	{
		reindexer::client::QueryResults resultCs;
		err = reindexer.Select(reindexer::Query("#clientsstats"), resultCs);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(resultCs.Count(), 1);
		auto it = resultCs.begin();
		reindexer::WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();
		gason::JsonParser parser;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		bool isSubscribed = clientsStats["is_subscribed"].As<bool>(true);
		EXPECT_EQ(isSubscribed, false);
		reindexer::UpdatesFilters resultFilters;
		resultFilters.FromJSON(clientsStats["updates_filter"]);
		EXPECT_EQ(resultFilters, reindexer::UpdatesFilters());
	}
}

TEST_F(ClientsStatsApi, TxCountLimitation) {
	const size_t kMaxTxCount = 1024;
	RunServerInThread(true);
	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = 1;
	config.WorkerThreads = 1;
	reindexer::client::Reindexer reindexer(config);
	reindexer::client::ConnectOpts opts;
	opts.CreateDBIfMissing();
	auto err = reindexer.Connect(GetConnectionString(), opts);
	ASSERT_TRUE(err.ok()) << err.what();

	std::string nsName("ns1");
	err = reindexer.OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();

	std::vector<reindexer::client::Transaction> txs;
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

	for (size_t i = 0; i < kMaxTxCount / 2; ++i) {
		if (i % 2) {
			err = reindexer.CommitTransaction(txs[i]);
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
				err = reindexer.CommitTransaction(txs[i]);
			} else {
				err = reindexer.RollBackTransaction(txs[i]);
			}
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i;
		}
	}
	ASSERT_EQ(StatsTxCount(reindexer), 0);
}
