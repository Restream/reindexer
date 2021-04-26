#pragma once

#include <gtest/gtest.h>
#include <string>
#include "client/cororeindexer.h"
#include "core/queryresults/queryresults.h"
#include "replicator/updatesobserver.h"
#include "server/dbmanager.h"
#include "server/server.h"
#include "thread"
#include "tools/fsops.h"

class ClientsStatsApi : public ::testing::Test {
public:
	ClientsStatsApi() : stop_(false) {}
	void SetUp();
	void TearDown();

	void RunServerInThread(bool statEnable);

	void RunNSelectThread(size_t threads, size_t coroutines);
	void RunNReconnectThread(size_t N);

	void StopThreads();

	void SetProfilingFlag(bool val, const std::string& column, reindexer::client::CoroReindexer& c);

	const std::string kdbPath = "/tmp/testdb/";
	const std::string kdbName = "test";
	const std::string kipaddress = "127.0.0.1";
	const uint16_t kPortI = 7777;
	const std::string kport = std::to_string(kPortI);
	const std::string kUserName = "reindexer";
	const std::string kPassword = "reindexer";
	const std::string kAppName = "test_app_name";

	std::string GetConnectionString();
	uint32_t StatsTxCount(reindexer::client::CoroReindexer& rx);

protected:
	class TestObserver : public reindexer::IUpdatesObserver {
	public:
		void OnWALUpdate(reindexer::LSNPair, std::string_view, const reindexer::WALRecord&) override {}
		void OnUpdatesLost(std::string_view) override {}
		void OnConnectionState(const reindexer::Error&) override {}
	};

private:
	void ClientSelectLoop(size_t coroutines);
	void ClientLoopReconnect();
	void StartStopCliensStatisticsLoop();
	reindexer_server::Server server_;
	std::unique_ptr<std::thread> serverThread_;
	std::vector<std::unique_ptr<std::thread>> clientThreads_;
	std::atomic_bool stop_;
	std::vector<std::unique_ptr<std::thread>> reconnectThreads_;
	std::vector<std::unique_ptr<std::thread>> startStopStatThreads_;
};
