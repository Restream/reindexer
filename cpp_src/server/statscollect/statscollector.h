#pragma once

#include <atomic>
#include <thread>
#include <unordered_map>
#include "core/namespacedef.h"
#include "estl/fast_hash_map.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "istatswatcher.h"
#include "loggerwrapper.h"
#include "tools/stringstools.h"

namespace reindexer_server {

class DBManager;
class Prometheus;

class [[nodiscard]] StatsCollector final : public IStatsWatcher, public IStatsStarter {
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
	void Restart(reindexer::unique_lock<reindexer::mutex>&& lck) noexcept override;
	void Stop();

	StatsWatcherSuspend SuspendStatsThread() override;
	void OnInputTraffic(const std::string& db, std::string_view source, std::string_view protocol, size_t bytes) noexcept override;
	void OnOutputTraffic(const std::string& db, std::string_view source, std::string_view protocol, size_t bytes) noexcept override;
	void OnClientConnected(const std::string& db, std::string_view source, std::string_view protocol) noexcept override;
	void OnClientDisconnected(const std::string& db, std::string_view source, std::string_view protocol) noexcept override;

private:
	void startImpl();
	bool isTerminating() const noexcept { return terminate_.load(std::memory_order_acquire); }

	using NSMap = reindexer::fast_hash_map<std::string, std::vector<reindexer::NamespaceDef>>;
	struct [[nodiscard]] DBCounters {
		size_t clients{0};
		uint64_t inputTraffic{0};
		uint64_t outputTraffic{0};
	};

	using CountersByDB = std::unordered_map<std::string, DBCounters, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;
	struct [[nodiscard]] SourceCounters {
		std::string source;
		std::string protocol;
		CountersByDB counters;
	};
	using Counters = std::vector<SourceCounters>;

	void collectStats(DBManager& dbMngr);
	DBCounters& getCounters(const std::string& db, std::string_view source, std::string_view protocol);

	DBManager& dbMngr_;
	Prometheus* prometheus_;
	std::thread statsCollectingThread_;
	std::atomic<bool> terminate_;
	std::atomic<bool> enabled_;
	std::chrono::milliseconds collectPeriod_;
	Counters counters_;
	reindexer::mutex countersMtx_;
	reindexer::mutex threadMtx_;
	LoggerWrapper logger_;
};

}  // namespace reindexer_server
