#include "statscollector.h"
#include "dbmanager.h"
#include "prometheus.h"
#include "tools/alloc_ext/je_malloc_extension.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/errors.h"

namespace reindexer_server {

void StatsCollector::Start() {
	std::lock_guard lck(threadMtx_);
	if (statsCollectingThread_.joinable()) {
		throw reindexer::Error(errLogic, "Stats collectiong thread is already running");
	}
	if (prometheus_) {
		startImpl();
		enabled_.store(true, std::memory_order_release);
	}
}

void StatsCollector::Restart(std::unique_lock<std::mutex>&& lck) noexcept {
	try {
#ifdef RX_WITH_STDLIB_DEBUG
		assertrx(lck.mutex() == &threadMtx_);
		assertrx(lck.owns_lock());
		assertrx(enabled_.load(std::memory_order_acquire));
		assertrx(!statsCollectingThread_.joinable());
#endif
		if (lck.mutex() != &threadMtx_) {
			logger_.error("Unable to restart prometheus stats collecting thread (internal logic error: incorrect mutex ptr)");
			return;
		}
		if (!lck.owns_lock()) {
			logger_.error("Unable to restart prometheus stats collecting thread (internal logic error: mutex was not locked)");
			return;
		}
		if (!enabled_.load(std::memory_order_acquire)) {
			logger_.error("Attempt to restart stats collector, which was prevously disabled");
			return;
		}
		if (statsCollectingThread_.joinable()) {
			logger_.error("Attempt to restart stats collector, which is already running");
			return;
		}
		logger_.info("Restarting stats collector...");
		startImpl();
		logger_.info("Stats collector was started successfully");
	} catch (...) {
		if (!statsCollectingThread_.joinable()) {
			logger_.error("Unhandled exception during stats collector restarting. Stats collector was not restarted");
		}
#ifdef RX_WITH_STDLIB_DEBUG
		assertrx(false);
#endif
	}
}

void StatsCollector::Stop() {
	std::lock_guard lck(threadMtx_);
	if (statsCollectingThread_.joinable()) {
		terminate_.store(true, std::memory_order_release);
		statsCollectingThread_.join();
	}
	enabled_.store(false, std::memory_order_release);
}

StatsWatcherSuspend StatsCollector::SuspendStatsThread() {
	std::unique_lock lck(threadMtx_);
	if (statsCollectingThread_.joinable()) {
		logger_.info("Suspending stats collector...");
		terminate_.store(true, std::memory_order_release);
		statsCollectingThread_.join();
		terminate_.store(false, std::memory_order_release);
		return StatsWatcherSuspend(std::move(lck), *this, true);
	}
	return StatsWatcherSuspend(std::move(lck), *this, false);
}

void StatsCollector::OnInputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		std::lock_guard lck(countersMtx_);
		getCounters(db, source).inputTraffic += bytes;
	}
}

void StatsCollector::OnOutputTraffic(const std::string& db, std::string_view source, size_t bytes) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		std::lock_guard lck(countersMtx_);
		getCounters(db, source).outputTraffic += bytes;
	}
}

void StatsCollector::OnClientConnected(const std::string& db, std::string_view source) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		std::lock_guard lck(countersMtx_);
		++(getCounters(db, source).clients);
	}
}

void StatsCollector::OnClientDisconnected(const std::string& db, std::string_view source) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		std::lock_guard lck(countersMtx_);
		--(getCounters(db, source).clients);
	}
}

void StatsCollector::startImpl() {
	statsCollectingThread_ = std::thread([this]() {
		const auto kSleepTime = std::chrono::milliseconds(100);
		std::chrono::milliseconds now{0};
		while (!terminate_.load(std::memory_order_acquire)) {
			std::this_thread::sleep_for(kSleepTime);
			now += kSleepTime;
			if (now.count() % collectPeriod_.count() == 0) {
				this->collectStats(dbMngr_);
			}
		}
	});
}

void StatsCollector::collectStats(DBManager& dbMngr) {
	using namespace std::string_view_literals;
	auto dbNames = dbMngr.EnumDatabases();
	NSMap collectedDBs;
	for (auto& dbName : dbNames) {
		if (isTerminating()) {
			return;
		}

		auto ctx = MakeSystemAuthContext();
		auto status = dbMngr.OpenDatabase(dbName, ctx, false);
		if (!status.ok()) {
			continue;
		}

		reindexer::Reindexer* db = nullptr;
		status = ctx.GetDB(kRoleSystem, &db);
		assertrx(status.ok());
		assertrx(db);
		(void)status;

		{
			std::vector<NamespaceDef> nsDefs;
			status = db->EnumNamespaces(nsDefs, EnumNamespacesOpts().OnlyNames().WithClosed());
			if (!status.ok()) {
				collectedDBs.emplace(dbName, std::vector<NamespaceDef>());
				continue;
			}
			collectedDBs.emplace(dbName, std::move(nsDefs));
		}

		constexpr static auto kPerfstatsNs = "#perfstats"sv;
		constexpr static auto kMemstatsNs = "#memstats"sv;
		QueryResults qr;
		status = db->Select(Query(std::string(kPerfstatsNs)), qr);
		if (status.ok() && qr.Count()) {
			for (auto it = qr.begin(); it != qr.end(); ++it) {
				auto item = it.GetItem(false);
				std::string nsName = item["name"].As<std::string>();
				constexpr auto kSelectQueryType = "select"sv;
				constexpr auto kUpdateQueryType = "update"sv;
				prometheus_->RegisterQPS(dbName, nsName, kSelectQueryType, item["selects.last_sec_qps"].As<int64_t>());
				prometheus_->RegisterQPS(dbName, nsName, kUpdateQueryType, item["updates.last_sec_qps"].As<int64_t>());
				prometheus_->RegisterLatency(dbName, nsName, kSelectQueryType, item["selects.last_sec_avg_latency_us"].As<int64_t>());
				prometheus_->RegisterLatency(dbName, nsName, kUpdateQueryType, item["updates.last_sec_avg_latency_us"].As<int64_t>());
			}
		}

		if (isTerminating()) {
			return;
		}

		qr.Clear();
		status = db->Select(Query(std::string(kMemstatsNs)), qr);
		if (status.ok() && qr.Count()) {
			for (auto it = qr.begin(); it != qr.end(); ++it) {
				auto item = it.GetItem(false);
				auto nsName = item["name"].As<std::string>();
				prometheus_->RegisterCachesSize(dbName, nsName, item["total.cache_size"].As<int64_t>());
				prometheus_->RegisterIndexesSize(dbName, nsName, item["total.indexes_size"].As<int64_t>());
				prometheus_->RegisterDataSize(dbName, nsName, item["total.data_size"].As<int64_t>());
				prometheus_->RegisterItemsCount(dbName, nsName, item["items_count"].As<int64_t>());
				prometheus_->RegisterStorageStatus(dbName, nsName, item["storage_ok"].As<bool>());
			}
		}
	}

	if (isTerminating()) {
		return;
	}

#if REINDEX_WITH_GPERFTOOLS
	if (reindexer::alloc_ext::TCMallocIsAvailable()) {
		size_t memoryConsumationBytes = 0;
		reindexer::alloc_ext::instance()->GetNumericProperty("generic.current_allocated_bytes", &memoryConsumationBytes);
		prometheus_->RegisterAllocatedMemory(memoryConsumationBytes);
	}
#elif REINDEX_WITH_JEMALLOC
	if (reindexer::alloc_ext::JEMallocIsAvailable()) {
		size_t memoryConsumationBytes = 0;
		size_t sz = sizeof(memoryConsumationBytes);
		alloc_ext::mallctl("stats.allocated", &memoryConsumationBytes, &sz, NULL, 0);
		prometheus_->RegisterAllocatedMemory(memoryConsumationBytes);
	}
#endif	// REINDEX_WITH_JEMALLOC

	{
		std::lock_guard lck(countersMtx_);
		for (const auto& dbCounters : counters_) {
			for (const auto& counter : dbCounters.second) {
				if (std::string_view(dbCounters.first) == "rpc"sv) {
					prometheus_->RegisterRPCClients(counter.first, counter.second.clients);
				}
				prometheus_->RegisterInputTraffic(counter.first, dbCounters.first, counter.second.inputTraffic);
				prometheus_->RegisterOutputTraffic(counter.first, dbCounters.first, counter.second.outputTraffic);
			}
		}
	}

	prometheus_->NextEpoch();
}

StatsCollector::DBCounters& StatsCollector::getCounters(const std::string& db, std::string_view source) {
	for (auto& el : counters_) {
		if (std::string_view(el.first) == source) {
			return el.second[db];
		}
	}
	counters_.emplace_back(std::string(source), CountersByDB());
	auto& sourceMap = counters_.back().second;
	return sourceMap[db];
}

}  // namespace reindexer_server
