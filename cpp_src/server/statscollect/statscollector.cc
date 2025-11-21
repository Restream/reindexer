#include "statscollector.h"
#include "core/system_ns_names.h"
#include "dbmanager.h"
#include "estl/lock.h"
#include "estl/mutex.h"
#include "prometheus.h"
#include "tools/alloc_ext/je_malloc_extension.h"
#include "tools/alloc_ext/tc_malloc_extension.h"
#include "tools/errors.h"
#include "tools/jsontools.h"

namespace reindexer_server {

void StatsCollector::Start() {
	reindexer::lock_guard lck(threadMtx_);
	if (statsCollectingThread_.joinable()) {
		throw reindexer::Error(errLogic, "Stats collectiong thread is already running");
	}
	if (prometheus_) {
		startImpl();
		enabled_.store(true, std::memory_order_release);
	}
}

void StatsCollector::Restart(reindexer::unique_lock<reindexer::mutex>&& lck) noexcept {
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
	reindexer::lock_guard lck(threadMtx_);
	if (statsCollectingThread_.joinable()) {
		terminate_.store(true, std::memory_order_release);
		statsCollectingThread_.join();
	}
	enabled_.store(false, std::memory_order_release);
}

StatsWatcherSuspend StatsCollector::SuspendStatsThread() {
	reindexer::unique_lock lck(threadMtx_);
	if (statsCollectingThread_.joinable()) {
		logger_.info("Suspending stats collector...");
		terminate_.store(true, std::memory_order_release);
		statsCollectingThread_.join();
		terminate_.store(false, std::memory_order_release);
		return StatsWatcherSuspend(std::move(lck), *this, true);
	}
	return StatsWatcherSuspend(std::move(lck), *this, false);
}

void StatsCollector::OnInputTraffic(const std::string& db, std::string_view source, std::string_view protocol, size_t bytes) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		reindexer::lock_guard lck(countersMtx_);
		getCounters(db, source, protocol).inputTraffic += bytes;
	}
}

void StatsCollector::OnOutputTraffic(const std::string& db, std::string_view source, std::string_view protocol, size_t bytes) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		reindexer::lock_guard lck(countersMtx_);
		getCounters(db, source, protocol).outputTraffic += bytes;
	}
}

void StatsCollector::OnClientConnected(const std::string& db, std::string_view source, std::string_view protocol) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		reindexer::lock_guard lck(countersMtx_);
		++(getCounters(db, source, protocol).clients);
	}
}

void StatsCollector::OnClientDisconnected(const std::string& db, std::string_view source, std::string_view protocol) noexcept {
	if (prometheus_ && enabled_.load(std::memory_order_acquire)) {
		reindexer::lock_guard lck(countersMtx_);
		auto& counters = getCounters(db, source, protocol);
		if (counters.clients) {
			--counters.clients;
		}
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
		status = ctx.GetDB<AuthContext::CalledFrom::Core>(kRoleSystem, &db);
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

		static const auto kPerfstatsQuery = Query(kPerfStatsNamespace);
		QueryResults qr;
		status = db->Select(kPerfstatsQuery, qr);
		if (status.ok()) {
			for (auto it = qr.begin(); it != qr.end(); ++it) {
				try {
					auto item = it.GetItem(false);
					std::string nsName = item["name"].As<std::string>();
					constexpr auto kSelectQueryType = "select"sv;
					constexpr auto kUpdateQueryType = "update"sv;
					prometheus_->RegisterQPS(dbName, nsName, kSelectQueryType, item["selects.last_sec_qps"].As<int64_t>());
					prometheus_->RegisterQPS(dbName, nsName, kUpdateQueryType, item["updates.last_sec_qps"].As<int64_t>());
					prometheus_->RegisterLatency(dbName, nsName, kSelectQueryType, item["selects.last_sec_avg_latency_us"].As<int64_t>());
					prometheus_->RegisterLatency(dbName, nsName, kUpdateQueryType, item["updates.last_sec_avg_latency_us"].As<int64_t>());
					std::string_view json = item.GetJSON();
					gason::JsonParser parser;
					auto root = parser.Parse(json);
					auto nodeSelect = root["selects"];
					if (!nodeSelect.empty() && nodeSelect.isObject()) {
						prometheus_->RegisterQPS(dbName, nsName, kSelectQueryType, nodeSelect["last_sec_qps"].As<int64_t>());
						prometheus_->RegisterLatency(dbName, nsName, kSelectQueryType, nodeSelect["last_sec_avg_latency_us"].As<int64_t>());
					}
					auto nodeUpdates = root["updates"];
					if (!nodeUpdates.empty() && nodeUpdates.isObject()) {
						prometheus_->RegisterQPS(dbName, nsName, kSelectQueryType, nodeUpdates["last_sec_qps"].As<int64_t>());
						prometheus_->RegisterLatency(dbName, nsName, kSelectQueryType,
													 nodeUpdates["last_sec_avg_latency_us"].As<int64_t>());
					}
					auto nodeIndexes = root["indexes"];
					if (!nodeIndexes.empty() && nodeIndexes.isArray()) {
						for (auto it = gason::begin(nodeIndexes); it != gason::end(nodeIndexes); ++it) {
							std::string indexName;
							Error err = tryReadRequiredJsonValue(nullptr, *it, "name", indexName);
							if (!err.ok()) {
								continue;
							}

							auto processEmbed = [&](std::string_view name) {
								Error err;
								auto nodeUpEmb = (*it)[name];
								if (!nodeUpEmb.empty()) {
									int64_t val = 0;
									err = tryReadRequiredJsonValue(nullptr, nodeUpEmb, "last_sec_qps", val);
									if (err.ok()) {
										prometheus_->RegisterEmbedderLastSecQps(dbName, nsName, indexName, name, val);
									}
									err = tryReadRequiredJsonValue(nullptr, nodeUpEmb, "last_sec_dps", val);
									if (err.ok()) {
										prometheus_->RegisterEmbedderLastSecDps(dbName, nsName, indexName, name, val);
									}
									err = tryReadRequiredJsonValue(nullptr, nodeUpEmb, "last_sec_errors_count", val);
									if (err.ok()) {
										prometheus_->RegisterEmbedderLastSecErrorsCount(dbName, nsName, indexName, name, val);
									}
									err = tryReadRequiredJsonValue(nullptr, nodeUpEmb, "conn_in_use", val);
									if (err.ok()) {
										prometheus_->RegisterEmbedderConnInUse(dbName, nsName, indexName, name, val);
									}
									err = tryReadRequiredJsonValue(nullptr, nodeUpEmb, "last_sec_avg_embed_latency_us", val);
									if (err.ok()) {
										prometheus_->RegisterEmbedderLastSecAvgLatencyUs(dbName, nsName, indexName, name, val);
									}
									err = tryReadRequiredJsonValue(nullptr, nodeUpEmb, "last_sec_avg_embed_latency_us", val);
									if (err.ok()) {
										prometheus_->RegisterEmbedderLastSecAvgEmbedLatencyUs(dbName, nsName, indexName, name, val);
									}
								}
							};
							processEmbed("upsert_embedder");
							processEmbed("query_embedder");
						}
					}

				}  // NOLINTBEGIN(bugprone-empty-catch)
				catch (std::exception&) {
				}
				// NOLINTEND(bugprone-empty-catch)
			}
		}

		if (isTerminating()) {
			return;
		}

		qr.Clear();
		static const auto kMemstatsQuery = Query(kMemStatsNamespace);
		status = db->Select(kMemstatsQuery, qr);
		if (status.ok()) {
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
		std::ignore = alloc_ext::mallctl("stats.allocated", &memoryConsumationBytes, &sz, NULL, 0);
		prometheus_->RegisterAllocatedMemory(memoryConsumationBytes);
	}
#endif	// REINDEX_WITH_JEMALLOC

	{
		reindexer::lock_guard lck(countersMtx_);
		for (const auto& dbCounters : counters_) {
			for (const auto& counter : dbCounters.counters) {
				if (std::string_view(dbCounters.source) == "rpc"sv) {
					prometheus_->RegisterRPCClients(counter.first, dbCounters.protocol, counter.second.clients);
				}
				prometheus_->RegisterInputTraffic(counter.first, dbCounters.source, dbCounters.protocol, counter.second.inputTraffic);
				prometheus_->RegisterOutputTraffic(counter.first, dbCounters.source, dbCounters.protocol, counter.second.outputTraffic);
			}
		}
	}

	prometheus_->NextEpoch();
}

StatsCollector::DBCounters& StatsCollector::getCounters(const std::string& db, std::string_view source, std::string_view protocol) {
	for (auto& el : counters_) {
		if (std::string_view(el.source) == source && std::string_view(el.protocol) == protocol) {
			return el.counters[db];
		}
	}
	return counters_
		.emplace_back(SourceCounters{.source = std::string(source), .protocol = std::string(protocol), .counters = CountersByDB()})
		.counters[db];
}

}  // namespace reindexer_server
