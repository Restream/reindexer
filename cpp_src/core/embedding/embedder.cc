#include "embedder.h"

#include <algorithm>

#include "core/embedding/embedderscache.h"
#include "core/embedding/httpconnector.h"
#include "core/namespace/namespacestat.h"
#include "estl/chunk.h"
#include "tools/logger.h"

namespace reindexer {

namespace {
constexpr std::string_view kFormatText("text");
constexpr std::string_view kFormatJson("json");
constexpr std::string_view kServerPathFormat("/api/v1/embedder/{}/produce?format={}");
}  // namespace

EmbedderBase::EmbedderBase(std::string_view name, std::string_view format, std::string_view fieldName, EmbedderConfig&& config,
						   PoolConfig&& poolConfig, const std::shared_ptr<EmbeddersCache>& cache)
	: name_{name},
	  fieldName_{fieldName},
	  serverPath_{fmt::format(kServerPathFormat, name, format)},
	  cache_{cache},
	  config_{std::move(config)} {
	pool_ = std::make_unique<ConnectorPool>(std::move(poolConfig));
}

EmbedderPerfStat EmbedderBase::GetPerfStat(std::string_view tag) const {
	EmbedderPerfStat stat;
	PerfStat statEmbedderTimes = statistic_.embedderTimes.Get<PerfStat>();
	PerfStat statLastSecErrorsCount = statistic_.lastSecErrorsCount.Get<PerfStat>();
	PerfStat statConnectionAwait = statistic_.connectionAwait.Get<PerfStat>();
	PerfStat statEmbedderTimesCacheMiss = statistic_.embedderTimesCacheMiss.Get<PerfStat>();
	PerfStat statEmbedderTimesCache = statistic_.embedderTimesCache.Get<PerfStat>();

	stat.totalQueriesCount = statistic_.totalQueriesCount.load(std::memory_order_relaxed);
	stat.totalEmbedDocumentsCount = statistic_.totalEmbedDocumentsCount.load(std::memory_order_relaxed);
	stat.lastSecQps = statEmbedderTimes.lastSecHitCount;
	stat.lastSecDps = statistic_.embedderDps.Get();
	stat.totalErrorsCount = statistic_.totalErrorsCount.load(std::memory_order_relaxed);
	stat.lastSecErrorsCount = statLastSecErrorsCount.lastSecHitCount;
	stat.connInUse = pool_->ConnectionInUse();
	stat.lastSecAvgConnInUse = statistic_.avgConnInUse.Get();
	stat.totalAvgLatencyUs = statEmbedderTimes.totalAvgTimeUs;
	stat.lastSecAvgLatencyUs = statEmbedderTimes.lastSecAvgTimeUs;
	stat.maxLatencyUs = statEmbedderTimes.maxTimeUs;
	stat.minLatencyUs = statEmbedderTimes.minTimeUs;
	stat.totalAvgConnAwaitLatencyUs = statConnectionAwait.totalAvgTimeUs;
	stat.lastSecAvgConnAwaitLatencyUs = statConnectionAwait.lastSecAvgTimeUs;
	stat.totalAvgEmbedLatencyUs = statEmbedderTimesCacheMiss.totalAvgTimeUs;
	stat.lastSecAvgEmbedLatencyUs = statEmbedderTimesCacheMiss.lastSecAvgTimeUs;
	stat.maxEmbedLatencyUs = statEmbedderTimesCacheMiss.maxTimeUs;
	stat.minEmbedLatencyUs = statEmbedderTimesCacheMiss.minTimeUs;
	stat.totalAvgCacheLatencyUs = statEmbedderTimesCache.totalAvgTimeUs;
	stat.lastSecAvgCacheLatencyUs = statEmbedderTimesCache.lastSecAvgTimeUs;
	stat.maxCacheLatencyUs = statEmbedderTimesCache.maxTimeUs;
	stat.minCacheLatencyUs = statEmbedderTimesCache.minTimeUs;

	if (cache_ && !tag.empty()) {
		stat.cacheStat = cache_->GetPerfStat(tag);
	}
	return stat;
}

void EmbedderBase::ResetPerfStat() const noexcept { statistic_.Reset(); }

void EmbedderBase::Statistic::Reset() {
	embedderTimes.Reset();
	embedderDps.Reset();
	embedderTimesCacheMiss.Reset();
	embedderTimesCache.Reset();
	avgConnInUse.Reset();
	totalErrorsCount.store(0, std::memory_order_relaxed);
	lastSecErrorsCount.Reset();
	totalQueriesCount.store(0, std::memory_order_relaxed);
	totalEmbedDocumentsCount.store(0, std::memory_order_relaxed);
	connectionAwait.Reset();
}

Error EmbedderBase::GetLastError() const { return statistic_.lastError.GetLastError(); }
bool EmbedderBase::GetLastStatus() const noexcept { return statistic_.lastStatus.load(std::memory_order_relaxed); }

void EmbedderBase::calculate(const RdxContext& ctx, const embedding::Adapter& srcAdapter, system_clock_w::time_point tmStart,
							 bool enablePerfStat, h_vector<ConstFloatVector, 1>& products) const {
	if (enablePerfStat) {
		statistic_.totalQueriesCount.fetch_add(1, std::memory_order_relaxed);
	}

	products.resize(0);

	logFmt(LogTrace, "Embedding src: {}", srcAdapter.View());

	if (cache_) {
		auto product = cache_->Get(config_.tag, srcAdapter);
		if (product.has_value()) {
			PerfStatCalculatorMT embedderTimesCachCalculator(statistic_.embedderTimesCache, tmStart, enablePerfStat);
			products.emplace_back(std::move(product.value()));
			return;	 // NOTE: stop calculation
		}
	}
	PerfStatCalculatorMT embedderTimesCacheMissCalculator(statistic_.embedderTimesCacheMiss, tmStart, enablePerfStat);
	PerfStatCalculatorMT connectionAwaitCalculator(statistic_.connectionAwait, enablePerfStat);
	auto res = pool_->GetConnector(ctx);
	if (!res.first.ok()) {
		throw res.first;
	}
	if (enablePerfStat) {
		connectionAwaitCalculator.HitManualy();
		statistic_.avgConnInUse.Hit(pool_->ConnectionInUse());
	}
	auto response = (*res.second).Send(serverPath_, srcAdapter.Content());
	if (!response.ok) {
		throw Error{errNetwork, "Failed to get embedding for '{}'. Problem with client: {}", fieldName_, response.content};
	}

	logFmt(LogTrace, "Embedding data: {}", response.content);
	auto error = embedding::Adapter::VectorFromJSON(response.content, products);
	if (!error.ok()) {
		throw error;
	}

	if (cache_) {
		cache_->Put(config_.tag, srcAdapter, products);
	}
}

UpsertEmbedder::UpsertEmbedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
							   const std::shared_ptr<EmbeddersCache>& cache, bool enablePerfStat)
	: EmbedderBase(name, kFormatJson, fieldName, std::move(config), std::move(poolConfig), cache), enablePerfStat_(enablePerfStat) {}

bool UpsertEmbedder::IsAuxiliaryField(std::string_view fieldName) const noexcept {
	return std::ranges::find(config_.fields, fieldName) != config_.fields.end();
}

void UpsertEmbedder::Calculate(const RdxContext& ctx, std::span<const std::vector<std::pair<std::string, VariantArray>>> sources,
							   h_vector<ConstFloatVector, 1>& products) const {
	const bool enablePerfStat = enablePerfStat_.load(std::memory_order_relaxed);
	PerfStatCalculatorMT allTime(statistic_.embedderTimes, enablePerfStat);
	system_clock_w::time_point tmStart;
	if (enablePerfStat) {
		tmStart = system_clock_w::now();
		statistic_.totalEmbedDocumentsCount.fetch_add(sources.size(), std::memory_order_relaxed);
		statistic_.embedderDps.Hit(sources.size());
	}

	try {
		assertrx_dbg(!sources.empty());
		checkFields(sources);
		embedding::Adapter srcAdapter(sources);
		calculate(ctx, srcAdapter, tmStart, enablePerfStat, products);
		statistic_.lastStatus.store(true, std::memory_order_relaxed);
	} catch (const std::exception& e) {
		statistic_.lastError.SetError(e);
		statistic_.lastStatus.store(false, std::memory_order_relaxed);
		if (enablePerfStat) {
			statistic_.totalErrorsCount.fetch_add(1, std::memory_order_relaxed);
			statistic_.lastSecErrorsCount.Hit(std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - tmStart));
		}
		throw;
	}
}

void UpsertEmbedder::checkFields(std::span<const std::vector<std::pair<std::string, VariantArray>>> sources) const {
	assertrx_dbg(!config_.fields.empty());

	const auto fieldsSize = config_.fields.size();
	for (size_t idx = 0; const auto& src : sources) {
		if (fieldsSize != src.size()) {
			throw Error{
				errLogic,	"Failed to get embedding for '{}'. Not all required data was requested. Requesting {}, expecting {}, source {}",
				name_,		src.size(),
				fieldsSize, idx};
		}
		++idx;
	}
}

QueryEmbedder::QueryEmbedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
							 const std::shared_ptr<EmbeddersCache>& cache, bool enablePerfStat)
	: EmbedderBase(name, kFormatText, fieldName, std::move(config), std::move(poolConfig), cache), enablePerfStat_(enablePerfStat) {}

void QueryEmbedder::Calculate(const RdxContext& ctx, const std::string& text, h_vector<ConstFloatVector, 1>& products) const {
	const bool enablePerfStat = enablePerfStat_.load(std::memory_order_relaxed);
	PerfStatCalculatorMT allTime(statistic_.embedderTimes, enablePerfStat);
	system_clock_w::time_point tmStart;
	if (enablePerfStat) {
		tmStart = system_clock_w::now();
		statistic_.totalEmbedDocumentsCount.fetch_add(1, std::memory_order_relaxed);
		statistic_.embedderDps.Hit(1);
	}

	try {
		embedding::Adapter srcAdapter(text);
		calculate(ctx, srcAdapter, tmStart, enablePerfStat, products);
		statistic_.lastStatus.store(true, std::memory_order_relaxed);
	} catch (const std::exception& e) {
		statistic_.lastError.SetError(e);
		statistic_.lastStatus.store(false, std::memory_order_relaxed);
		if (enablePerfStat) {
			statistic_.totalErrorsCount.fetch_add(1, std::memory_order_relaxed);
			statistic_.lastSecErrorsCount.Hit(std::chrono::duration_cast<std::chrono::microseconds>(system_clock_w::now() - tmStart));
		}
		throw;
	}
}

}  // namespace reindexer
