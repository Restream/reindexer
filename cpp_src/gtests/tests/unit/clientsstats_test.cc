#include "clientsstats_api.h"
#include "gason/gason.h"
#include "reindexer_version.h"
#include "tools/semversion.h"

TEST_F(ClientsStatsApi, ClientsStatsConcurrent) {
	RunServerInThrerad(true);
	RunClient(4, 1);
	RunNSelectThread(10);
	RunNReconnectThread(10);
	std::this_thread::sleep_for(std::chrono::seconds(5));
	StopThreads();
}

TEST_F(ClientsStatsApi, ClientsStatsData) {
	RunServerInThrerad(true);
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
	RunServerInThrerad(false);
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
	RunServerInThrerad(true);
	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = 1;
	config.WorkerThreads = 1;
	config.AppName = kAppName;
	reindexer::client::Reindexer reindexer(config);
	reindexer::client::ConnectOpts opts;
	opts.CreateDBIfMissing();
	auto err = reindexer.Connect(GetConnectionString(), opts);
	ASSERT_TRUE(err.ok()) << err.what();
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
	try {
		gason::JsonParser parser;
		const char* selectData = wrser.c_str();
		(void)selectData;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		std::string curActivity = clientsStats["current_activity"].As<std::string>();
		ASSERT_TRUE(curActivity == selectClientsStats) << "curActivity = [" << curActivity << "]";
		std::string curIP = clientsStats["ip"].As<std::string>();
		ASSERT_TRUE(curIP == kipaddress) << curIP;
		int64_t sentBytes = clientsStats["sent_bytes"].As<int64_t>();
		ASSERT_TRUE(sentBytes > 0) << "sentBytes = [" << sentBytes << "]";
		int64_t recvBytes = clientsStats["recv_bytes"].As<int64_t>();
		ASSERT_TRUE(recvBytes > 0) << "recvBytes = [" << recvBytes << "]";
		std::string userName = clientsStats["user_name"].As<std::string>();
		ASSERT_TRUE(userName == kUserName) << "userName =[" << userName << "]";
		std::string dbName = clientsStats["db_name"].As<std::string>();
		ASSERT_TRUE(dbName == kdbName) << "dbName =[" << dbName << "]";
		std::string appName = clientsStats["app_name"].As<std::string>();
		ASSERT_TRUE(appName == kAppName) << "appName =[" << appName << "]";
		std::string userRights = clientsStats["user_rights"].As<std::string>();
		ASSERT_TRUE(userRights == "owner") << "userRights =[" << userRights << "]";
		std::string clientVersion = clientsStats["client_version"].As<std::string>();
		ASSERT_TRUE(clientVersion == reindexer::SemVersion(REINDEX_VERSION).StrippedString()) << "clientVersion =[" << clientVersion << "]";
	} catch (...) {
		assert(false);
	}
}
