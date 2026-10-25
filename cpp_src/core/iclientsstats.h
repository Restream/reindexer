#pragma once

#include <string>
#include <vector>
#include "net/connection.h"

namespace reindexer {

constexpr std::string_view kTcpProtocolName = "tcp";
constexpr std::string_view kUnixProtocolName = "unix";

class WrSerializer;

struct [[nodiscard]] ClientStat {
	void GetJSON(WrSerializer& ser) const;
	int connectionId = 0;
	std::string_view protocol = kTcpProtocolName;
	std::string ip;
	std::string userName;
	std::string dbName;
	int64_t startTime = 0;
	std::string currentActivity;
	int64_t sentBytes = 0;
	int64_t recvBytes = 0;
	int64_t sendBufBytes = 0;
	uint32_t sendRate = 0;
	uint32_t recvRate = 0;
	int64_t lastSendTs = 0;
	int64_t lastRecvTs = 0;
	std::string userRights;
	std::string clientVersion;
	std::string appName;
	uint32_t txCount = 0;
};

struct [[nodiscard]] TxStats {
	std::atomic<uint32_t> txCount = {0};
};

struct [[nodiscard]] ClientConnectionStat {
	std::shared_ptr<reindexer::net::connection_stat> connectionStat;
	std::shared_ptr<reindexer::TxStats> txStats;
	std::string ip;
	std::string_view protocol = kTcpProtocolName;
	std::string userName;
	std::string dbName;
	std::string userRights;
	std::string clientVersion;
	std::string appName;
};

class [[nodiscard]] IClientsStats {
public:
	virtual void GetClientInfo(std::vector<ClientStat>& datas) = 0;
	virtual void AddConnection(int64_t connectionId, ClientConnectionStat&& conn) = 0;
	virtual void DeleteConnection(int64_t id) = 0;

	virtual ~IClientsStats() = default;
};

}  // namespace reindexer
