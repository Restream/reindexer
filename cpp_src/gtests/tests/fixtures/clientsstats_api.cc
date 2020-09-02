#include "clientsstats_api.h"

void ClientsStatsApi::SetUp() {}

void ClientsStatsApi::RunServerInThrerad(bool statEnable) {
	reindexer::fs::RmDirAll(kdbPath);
	// clang-format off
	std::string yaml =
		"storage:\n"
		"    path:" + kdbPath +"\n"
		"metrics:\n"
		"   clientsstats: " + (statEnable ? "true" : "false") + "\n"
		"logger:\n"
		"   loglevel: none\n"
		"   rpclog: \n"
		"   serverlog: \n"
		"net:\n"
		"   rpcaddr: " + kipaddress + ":" + kport + "\n"
		"   security: true\n";
	// clang-format on

	auto err = server_.InitFromYAML(yaml);
	EXPECT_TRUE(err.ok()) << err.what();

	serverThread_ = std::unique_ptr<std::thread>(new std::thread([this]() {
		auto res = this->server_.Start();
		(void)res;
		assert(res == EXIT_SUCCESS);
	}));
	while (!server_.IsReady()) {
		std::this_thread::sleep_for(std::chrono::milliseconds(1));
	}
}

void ClientsStatsApi::TearDown() {
	if (server_.IsReady()) {
		server_.Stop();
		serverThread_->join();
	}
}

std::string ClientsStatsApi::GetConnectionString() {
	std::string ret = "cproto://" + kUserName + ":" + kPassword + "@" + kipaddress + ":" + kport + "/" + kdbName;
	return ret;
}

void ClientsStatsApi::SetProfilingFlag(bool val, const std::string& column, reindexer::client::Reindexer* c) {
	reindexer::Query qup = std::move(reindexer::Query("#config").Where("type", CondEq, "profiling").Set(column, val));
	reindexer::client::QueryResults result;
	auto err = c->Update(qup, result);
	ASSERT_TRUE(err.ok()) << err.what();
}

void ClientsStatsApi::RunClient(int connPoolSize, int workerThreads) {
	reindexer::client::ReindexerConfig config;
	config.ConnPoolSize = connPoolSize;
	config.WorkerThreads = workerThreads;
	client_.reset(new reindexer::client::Reindexer(config));
	reindexer::client::ConnectOpts opts;
	opts.CreateDBIfMissing();
	auto err = client_->Connect(GetConnectionString(), opts);
	ASSERT_TRUE(err.ok()) << err.what();
	SetProfilingFlag(true, "profiling.activitystats", client_.get());
}

void ClientsStatsApi::ClientLoopReconnect() {
	while (!stop_) {
		int dt = rand() % 100;
		std::this_thread::sleep_for(std::chrono::milliseconds(dt));
		reindexer::client::Reindexer client;
		auto err = client.Connect(GetConnectionString());
		ASSERT_TRUE(err.ok()) << err.what();
		reindexer::client::QueryResults result;
		err = client.Select(reindexer::Query("#namespaces"), result);
		ASSERT_TRUE(err.ok()) << err.what();
		std::string resString;
		for (auto it = result.begin(); it != result.end(); ++it) {
			reindexer::WrSerializer sr;
			err = it.GetJSON(sr, false);
			ASSERT_TRUE(err.ok()) << err.what();
			reindexer::string_view sv = sr.Slice();
			resString += std::string(sv.data(), sv.size());
		}
	}
}

uint32_t ClientsStatsApi::StatsTxCount(reindexer::client::Reindexer& rx) {
	reindexer::client::QueryResults resultCs;
	auto err = rx.Select("SELECT * FROM #clientsstats", resultCs);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(resultCs.Count(), 1);
	auto it = resultCs.begin();
	reindexer::WrSerializer wrser;
	err = it.GetJSON(wrser, false);
	EXPECT_TRUE(err.ok()) << err.what();
	try {
		gason::JsonParser parser;
		gason::JsonNode clientsStats = parser.Parse(wrser.Slice());
		return clientsStats["tx_count"].As<uint32_t>();
	} catch (...) {
		assert(false);
	}
	EXPECT_TRUE(false);
	return 0;
}

void ClientsStatsApi::ClientSelectLoop() {
	while (!stop_) {
		reindexer::client::QueryResults result;
		auto err = client_->Select(reindexer::Query("#clientsstats"), result);
		ASSERT_TRUE(err.ok()) << err.what();
		std::string resString;
		for (auto it = result.begin(); it != result.end(); ++it) {
			reindexer::WrSerializer sr;
			err = it.GetJSON(sr, false);
			ASSERT_TRUE(err.ok()) << err.what();
			reindexer::string_view sv = sr.Slice();
			resString += std::string(sv.data(), sv.size());
		}
	}
}

void ClientsStatsApi::RunNSelectThread(int N) {
	for (int i = 0; i < N; i++) {
		auto thread_ = std::unique_ptr<std::thread>(new std::thread([this]() { this->ClientSelectLoop(); }));
		clientThreads_.push_back(std::move(thread_));
	}
}

void ClientsStatsApi::RunNReconnectThread(int N) {
	for (int i = 0; i < N; i++) {
		auto thread_ = std::unique_ptr<std::thread>(new std::thread([this]() { this->ClientLoopReconnect(); }));
		reconnectThreads_.push_back(std::move(thread_));
	}
}

void ClientsStatsApi::StopThreads() {
	stop_ = true;
	for (auto& t : clientThreads_) {
		if (t->joinable()) {
			t->join();
		}
	}
	clientThreads_.clear();
	for (auto& t : reconnectThreads_) {
		if (t->joinable()) {
			t->join();
		}
	}
	reconnectThreads_.clear();
}
