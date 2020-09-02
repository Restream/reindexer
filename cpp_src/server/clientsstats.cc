#include "clientsstats.h"

namespace reindexer_server {

void ClientsStats::GetClientInfo(std::vector<reindexer::ClientStat>& datas) {
	datas.clear();
	std::lock_guard<std::mutex> lck(mtx_);

	datas.reserve(connections_.size());
	for (auto& c : connections_) {
		reindexer::ClientStat d;
		d.connectionId = c.first;
		if (c.second.connectionStat) {
			d.recvBytes = c.second.connectionStat->recvBytes.load();
			d.sentBytes = c.second.connectionStat->sentBytes.load();
		}
		if (c.second.txStats) {
			d.txCount = c.second.txStats->txCount.load();
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

void ClientsStats::AddConnection(int64_t connectionId, reindexer::ClientConnectionStat&& conn) {
	std::lock_guard<std::mutex> lck(mtx_);
	connections_.emplace(connectionId, std::move(conn));
}

void ClientsStats::DeleteConnection(int64_t connectionId) {
	std::lock_guard<std::mutex> lck(mtx_);
	connections_.erase(connectionId);
}

}  // namespace reindexer_server
