#include "clientsstats.h"

namespace reindexer_server {

void ClientsStats::GetClientInfo(std::vector<reindexer::ClientStat>& datas) {
	datas.clear();
	std::lock_guard<std::mutex> lck(mtx_);

	for (auto i = connections_.begin(); i != connections_.end(); ++i) {
		reindexer::ClientStat d;
		d.connectionId = i->second.connectionId;
		if (i->second.connectionStat) {
			d.recvBytes = i->second.connectionStat->recvBytes.load();
			d.sentBytes = i->second.connectionStat->sentBytes.load();
		}
		d.startTime = i->second.connectionStat->startTime;
		d.dbName = i->second.dbName;
		d.ip = i->second.ip;
		d.userName = i->second.userName;
		d.userRights = i->second.userRights;
		d.clientVersion = i->second.clientVersion;
		datas.push_back(d);
	}
}

void ClientsStats::AddConnection(std::shared_ptr<reindexer::net::ConnectionStat> connStat, int connectionId, const std::string& ip,
								 const std::string& userName, const std::string& dbName, const std::string& userRights,
								 const std::string& clientVersion) {
	ClientConnectionStat client;
	client.connectionStat = connStat;
	client.connectionId = connectionId;
	client.ip = ip;
	client.userName = userName;
	client.dbName = dbName;
	client.userRights = userRights;
	client.clientVersion = clientVersion;
	std::lock_guard<std::mutex> lck(mtx_);
	connections_.insert(std::make_pair(connectionId, client));
}

void ClientsStats::DeleteConnection(int64_t connectionId) {
	std::lock_guard<std::mutex> lck(mtx_);
	connections_.erase(connectionId);
}

}  // namespace reindexer_server
