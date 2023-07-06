#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "core/namespacedef.h"
#include "estl/fast_hash_map.h"
#include "istatswatcher.h"
#include "loggerwrapper.h"
#include "tools/stringstools.h"

namespace reindexer_server {

class DBManager;
class Prometheus;

class StatsCollector : public IStatsWatcher, public IStatsStarter {
public:
	StatsCollector(DBManager& dbMngr, Prometheus* prometheus, std::chrono::milliseconds collectPeriod, LoggerWrapper logger)
		: dbMngr_(dbMngr),
		  prometheus_(prometheus),
		  terminate_(false),
		  enabled_(false),
		  collectPeriod_(collectPeriod),
		  logger_(std::move(logger)) {}
	~StatsCollector() override { Stop(); }
	void Start();
	void Restart(std::unique_lock<std::mutex>&& lck) noexcept override final;
	void Stop();

	[[nodiscard]] StatsWatcherSuspend SuspendStatsThread() override final;
	void OnInputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept override final;
	void OnOutputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept override final;
	void OnClientConnected(const std::string& db, std::string_view source) noexcept override final;
	void OnClientDisconnected(const std::string& db, std::string_view source) noexcept override final;

private:
	void startImpl();
	bool isTerminating() const noexcept { return terminate_.load(std::memory_order_acquire); }

	using NSMap = reindexer::fast_hash_map<std::string, std::vector<reindexer::NamespaceDef>>;
	struct DBCounters {
		size_t clients{0};
		uint64_t inputTraffic{0};
		uint64_t outputTraffic{0};
	};
	using CountersByDB = std::unordered_map<std::string, DBCounters, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;
	using Counters = std::vector<std::pair<std::string, CountersByDB>>;

	void collectStats(DBManager& dbMngr);
	DBCounters& getCounters(const std::string& db, std::string_view source);

	DBManager& dbMngr_;
	Prometheus* prometheus_;
	std::thread statsCollectingThread_;
	std::atomic<bool> terminate_;
	std::atomic<bool> enabled_;
	std::chrono::milliseconds collectPeriod_;
	Counters counters_;
	std::mutex countersMtx_;
	std::mutex threadMtx_;
	LoggerWrapper logger_;
};

}  // namespace reindexer_server
