#pragma once

#include <memory>
#include <unordered_map>
#include "core/iclientsstats.h"
#include "net/connection.h"

namespace reindexer_server {

struct ClientConnectionStat {
	std::shared_ptr<reindexer::net::ConnectionStat> connectionStat;
	int connectionId = -1;
	std::string ip;
	std::string userName;
	std::string dbName;
	std::string userRights;
	std::string clientVersion;
	std::string appName;
};

class ClientsStats : public reindexer::IClientsStats {
public:
	void GetClientInfo(std::vector<reindexer::ClientStat>& datas) override final;
	void AddConnection(std::shared_ptr<reindexer::net::ConnectionStat> connStat, int connectionId, std::string ip, std::string userName,
					   std::string dbName, std::string userRights, std::string clientVersion, std::string appName) override final;
	void DeleteConnection(int64_t connectionId) override final;
	virtual ~ClientsStats() {}

private:
	std::mutex mtx_;
	std::unordered_map<int64_t, ClientConnectionStat> connections_;
};

}  // namespace reindexer_server
