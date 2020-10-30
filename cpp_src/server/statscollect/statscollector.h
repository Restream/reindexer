#pragma once

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include "core/namespacedef.h"
#include "estl/fast_hash_map.h"
#include "istatswatcher.h"
#include "tools/stringstools.h"

namespace reindexer_server {

class DBManager;
class Prometheus;

class StatsCollector : public IStatsWatcher {
public:
	StatsCollector(Prometheus* prometheus, std::chrono::milliseconds collectPeriod)
		: prometheus_(prometheus), terminate_(false), enabled_(false), collectPeriod_(collectPeriod) {}
	~StatsCollector() override { Stop(); }
	void Start(DBManager& dbMngr);
	void Stop();

	void OnInputTraffic(const std::string& db, string_view source, size_t bytes) noexcept override final;
	void OnOutputTraffic(const std::string& db, string_view source, size_t bytes) noexcept override final;
	void OnClientConnected(const std::string& db, string_view source) noexcept override final;
	void OnClientDisconnected(const std::string& db, string_view source) noexcept override final;

private:
	using NSMap = reindexer::fast_hash_map<std::string, std::vector<reindexer::NamespaceDef>>;
	struct DBCounters {
		size_t clients{0};
		uint64_t inputTraffic{0};
		uint64_t outputTraffic{0};
	};
	using CountersByDB = std::unordered_map<std::string, DBCounters, reindexer::nocase_hash_str, reindexer::nocase_equal_str>;
	using Counters = std::vector<std::pair<std::string, CountersByDB>>;

	void collectStats(DBManager& dbMngr);
	DBCounters& getCounters(const std::string& db, string_view source);

	Prometheus* prometheus_;
	std::thread statsCollectingThread_;
	std::atomic<bool> terminate_;
	std::atomic<bool> enabled_;
	std::chrono::milliseconds collectPeriod_;
	Counters counters_;
	std::mutex mtx_;
};

}  // namespace reindexer_server
