#pragma once

#include <string>
#include <vector>
#include "net/connection.h"

namespace reindexer {

class WrSerializer;

struct ClientStat {
	void GetJSON(WrSerializer& ser) const;
	int connectionId = 0;
	std::string ip;
	std::string userName;
	std::string dbName;
	int64_t startTime = 0;
	std::string currentActivity;
	int64_t sentBytes = 0;
	int64_t recvBytes = 0;
	std::string userRights;
	std::string clientVersion;
	std::string appName;
};

class IClientsStats {
public:
	virtual void GetClientInfo(std::vector<ClientStat>& datas) = 0;
	virtual void AddConnection(std::shared_ptr<reindexer::net::ConnectionStat> connStat, int connectionId, std::string ip,
							   std::string userName, std::string dbName, std::string userRights, std::string clientVersion,
							   std::string appName) = 0;
	virtual void DeleteConnection(int64_t id) = 0;
};

}  // namespace reindexer
