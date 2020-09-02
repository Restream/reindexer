#pragma once

#include <memory>
#include <unordered_map>
#include "core/iclientsstats.h"

namespace reindexer_server {

class ClientsStats : public reindexer::IClientsStats {
public:
	void GetClientInfo(std::vector<reindexer::ClientStat>& datas) override final;
	void AddConnection(int64_t connectionId, reindexer::ClientConnectionStat&& conn) override final;
	void DeleteConnection(int64_t connectionId) override final;

private:
	std::mutex mtx_;
	std::unordered_map<int64_t, reindexer::ClientConnectionStat> connections_;
};

}  // namespace reindexer_server
