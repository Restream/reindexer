#include "clientsstats.h"

namespace reindexer_server {

void ClientsStats::GetClientInfo(std::vector<reindexer::ClientStat>& datas) {
	datas.clear();
	std::lock_guard<std::mutex> lck(mtx_);

	datas.reserve(connections_.size());
	for (auto& c : connections_) {
		reindexer::ClientStat d;
		d.connectionId = c.second.connectionId;
		if (c.second.connectionStat) {
			d.recvBytes = c.second.connectionStat->recvBytes.load();
			d.sentBytes = c.second.connectionStat->sentBytes.load();
		}
		d.startTime = c.second.connectionStat->startTime;
		d.dbName = c.second.dbName;
		d.ip = c.second.ip;
		d.userName = c.second.userName;
		d.userRights = c.second.userRights;
		d.clientVersion = c.second.clientVersion;
		d.appName = c.second.appName;
		datas.emplace_back(std::move(d));
	}
}

void ClientsStats::AddConnection(std::shared_ptr<reindexer::net::ConnectionStat> connStat, int connectionId, std::string ip,
								 std::string userName, std::string dbName, std::string userRights, std::string clientVersion,
								 std::string appName) {
	ClientConnectionStat client;
	client.connectionStat = std::move(connStat);
	client.connectionId = connectionId;
	client.ip = std::move(ip);
	client.userName = std::move(userName);
	client.dbName = std::move(dbName);
	client.userRights = std::move(userRights);
	client.clientVersion = std::move(clientVersion);
	client.appName = std::move(appName);
	std::lock_guard<std::mutex> lck(mtx_);
	connections_.emplace(connectionId, std::move(client));
}

void ClientsStats::DeleteConnection(int64_t connectionId) {
	std::lock_guard<std::mutex> lck(mtx_);
	connections_.erase(connectionId);
}

}  // namespace reindexer_server
