#pragma once

#include <string_view>
#include "estl/lock.h"
#include "estl/mutex.h"
#include "tools/assertrx.h"

namespace reindexer_server {

struct [[nodiscard]] IStatsStarter {
	virtual void Restart(reindexer::unique_lock<reindexer::mutex>&& lck) noexcept = 0;
	virtual ~IStatsStarter() = default;
};

class [[nodiscard]] StatsWatcherSuspend {
public:
	StatsWatcherSuspend(reindexer::unique_lock<reindexer::mutex>&& lck, IStatsStarter& owner, bool wasRunning)
		: lck_(std::move(lck)), owner_(owner), wasRunning_(wasRunning) {
		assertrx(lck_.owns_lock());
	}
	~StatsWatcherSuspend() {
		if (wasRunning_) {
			owner_.Restart(std::move(lck_));
		}
	}

private:
	reindexer::unique_lock<reindexer::mutex> lck_;
	IStatsStarter& owner_;
	bool wasRunning_;
};

struct [[nodiscard]] IStatsWatcher {
	virtual StatsWatcherSuspend SuspendStatsThread() = 0;
	virtual void OnInputTraffic(const std::string& db, std::string_view source, std::string_view protocol, size_t bytes) noexcept = 0;
	virtual void OnOutputTraffic(const std::string& db, std::string_view source, std::string_view protocol, size_t bytes) noexcept = 0;
	virtual void OnClientConnected(const std::string& db, std::string_view source, std::string_view protocol) noexcept = 0;
	virtual void OnClientDisconnected(const std::string& db, std::string_view source, std::string_view protocol) noexcept = 0;
	virtual ~IStatsWatcher() noexcept = default;
};

}  // namespace reindexer_server
