#include "clientsstats.h"
#include "estl/lock.h"
#include "tools/stringstools.h"

namespace reindexer_server {

void ClientsStats::GetClientInfo(std::vector<reindexer::ClientStat>& datas) {
	datas.clear();
	reindexer::lock_guard lck(mtx_);

	datas.reserve(connections_.size());
	for (auto& c : connections_) {
		reindexer::ClientStat d;
		d.connectionId = c.first;
		if (c.second.connectionStat) {
			d.recvBytes = c.second.connectionStat->recv_bytes.load(std::memory_order_relaxed);
			d.sentBytes = c.second.connectionStat->sent_bytes.load(std::memory_order_relaxed);
			d.sendBufBytes = c.second.connectionStat->send_buf_bytes.load(std::memory_order_relaxed);
			d.sendRate = c.second.connectionStat->send_rate.load(std::memory_order_relaxed);
			d.recvRate = c.second.connectionStat->recv_rate.load(std::memory_order_relaxed);
			d.lastSendTs = c.second.connectionStat->last_send_ts.load(std::memory_order_relaxed);
			d.lastRecvTs = c.second.connectionStat->last_recv_ts.load(std::memory_order_relaxed);
			d.startTime = c.second.connectionStat->start_time;
		}
		if (c.second.txStats) {
			d.txCount = c.second.txStats->txCount.load();
		}
		reindexer::deepCopy(d.dbName, c.second.dbName);
		reindexer::deepCopy(d.ip, c.second.ip);
		reindexer::deepCopy(d.userName, c.second.userName);
		d.protocol = c.second.protocol;
		reindexer::deepCopy(d.userRights, c.second.userRights);
		reindexer::deepCopy(d.clientVersion, c.second.clientVersion);
		reindexer::deepCopy(d.appName, c.second.appName);
		datas.emplace_back(std::move(d));
	}
}

void ClientsStats::AddConnection(int64_t connectionId, reindexer::ClientConnectionStat&& conn) {
	reindexer::lock_guard lck(mtx_);
	connections_.emplace(connectionId, std::move(conn));
}

void ClientsStats::DeleteConnection(int64_t connectionId) {
	reindexer::lock_guard lck(mtx_);
	connections_.erase(connectionId);
}

}  // namespace reindexer_server
