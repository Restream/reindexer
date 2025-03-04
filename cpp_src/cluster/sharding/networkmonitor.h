#pragma once

#include <chrono>
#include <condition_variable>
#include "estl/contexted_cond_var.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "estl/shared_mutex.h"
#include "tools/errors.h"

namespace reindexer {

class RdxContext;

namespace sharding {

class Connections;
using ConnectionsMap = fast_hash_map<int, Connections>;

class NetworkMonitor {
public:
	void Configure(ConnectionsMap& hostsConnections, std::chrono::seconds defaultTimeout, std::chrono::milliseconds statusCallTimeout);
	Error AwaitShards(const RdxContext& ctx) noexcept;
	void Shutdown();

private:
	void sendStatusRequests();
	Error awaitStatuses(std::unique_lock<std::recursive_mutex>& lck, const RdxContext& ctx);
	bool areStatusesReady() const noexcept;

	bool inProgress_ = false;
	bool terminated_ = false;
	std::recursive_mutex mtx_;
	contexted_cond_var cv_;
	fast_hash_set<int> succeed_;
	size_t executed_ = 0;
	size_t connectionsTotal_ = 0;
	ConnectionsMap* hostsConnections_ = nullptr;
	std::chrono::seconds defaultTimeout_;
	std::chrono::milliseconds statusCallTimeout_;
	Error lastCompletionError_{};
};

}  // namespace sharding
}  // namespace reindexer
