#pragma once

#include <gtest/gtest.h>
#include <thread>
#include "client/cororeindexer.h"
#include "server/server.h"

class [[nodiscard]] ClientsStatsApi : public ::testing::Test {
public:
	void TearDown() override;

	void RunServerInThread(bool statEnable);

	void RunNSelectThread(size_t threads, size_t coroutines);
	void RunNReconnectThread(size_t N);

	void StopThreads();

	void SetProfilingFlag(bool val, const std::string& column, reindexer::client::CoroReindexer& c);

	const std::string kdbName = "test";
	const std::string kipaddress = "127.0.0.1";
	const uint16_t kPortI = 7777;
	const std::string kRPCPort = std::to_string(kPortI);
	const uint16_t kHttpPortI = 7888;
	const std::string kHttpPort = std::to_string(kHttpPortI);
	const uint16_t kClusterPortI = 7999;
	const std::string kClusterPort = std::to_string(kClusterPortI);
	const std::string kUserName = "reindexer";
	const std::string kPassword = "reindexer";
	const std::string kAppName = "test_app_name";

	std::string GetConnectionString();
	uint32_t StatsTxCount(reindexer::client::CoroReindexer& rx);

private:
	void ClientSelectLoop(size_t coroutines);
	void ClientLoopReconnect();
	void StartStopCliensStatisticsLoop();
	reindexer_server::Server server_;
	std::unique_ptr<std::thread> serverThread_;
	std::vector<std::unique_ptr<std::thread>> clientThreads_;
	std::atomic_bool stop_{false};
	std::vector<std::unique_ptr<std::thread>> reconnectThreads_;
	std::vector<std::unique_ptr<std::thread>> startStopStatThreads_;
};
