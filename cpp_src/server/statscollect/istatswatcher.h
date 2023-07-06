#pragma once

#include <mutex>
#include <string_view>
#include "tools/assertrx.h"

namespace reindexer_server {

struct IStatsStarter {
	virtual void Restart(std::unique_lock<std::mutex>&& lck) noexcept = 0;
	virtual ~IStatsStarter() = default;
};

class StatsWatcherSuspend {
public:
	StatsWatcherSuspend(std::unique_lock<std::mutex>&& lck, IStatsStarter& owner, bool wasRunning)
		: lck_(std::move(lck)), owner_(owner), wasRunning_(wasRunning) {
		assertrx(lck_.owns_lock());
	}
	~StatsWatcherSuspend() {
		if (wasRunning_) {
			owner_.Restart(std::move(lck_));
		}
	}

private:
	std::unique_lock<std::mutex> lck_;
	IStatsStarter& owner_;
	bool wasRunning_;
};

struct IStatsWatcher {
	[[nodiscard]] virtual StatsWatcherSuspend SuspendStatsThread() = 0;
	virtual void OnInputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept = 0;
	virtual void OnOutputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept = 0;
	virtual void OnClientConnected(const std::string& db, std::string_view source) noexcept = 0;
	virtual void OnClientDisconnected(const std::string& db, std::string_view source) noexcept = 0;
	virtual ~IStatsWatcher() noexcept = default;
};

}  // namespace reindexer_server
