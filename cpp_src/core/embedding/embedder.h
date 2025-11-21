#pragma once

#include <span>
#include <string_view>
#include "core/embedding/connectorpool.h"
#include "core/embedding/embeddingconfig.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "core/perfstatcounter.h"
#include "estl/h_vector.h"
#include "tools/errors.h"

namespace reindexer {

class RdxContext;
class EmbeddersCache;
struct EmbedderPerfStat;
namespace embedding {
class Adapter;
}  // namespace embedding

class [[nodiscard]] EmbedderBase {
public:
	virtual ~EmbedderBase() noexcept = default;

	EmbedderBase() = delete;
	EmbedderBase(const EmbedderBase&) = delete;
	EmbedderBase(EmbedderBase&&) noexcept = delete;
	EmbedderBase& operator=(const EmbedderBase&) = delete;
	EmbedderBase& operator=(EmbedderBase&&) noexcept = delete;

	EmbedderPerfStat GetPerfStat(std::string_view tag) const;
	void ResetPerfStat() const noexcept;

	Error GetLastError() const;
	bool GetLastStatus() const noexcept;

protected:
	EmbedderBase(std::string_view name, std::string_view format, std::string_view fieldName, EmbedderConfig&& config,
				 PoolConfig&& poolConfig, const std::shared_ptr<EmbeddersCache>& cache);

	void calculate(const RdxContext& ctx, const embedding::Adapter& srcAdapter, system_clock_w::time_point tmStart, bool enablePerfStat,
				   h_vector<ConstFloatVector, 1>& products) const;

	const std::string name_;
	const std::string fieldName_;
	const std::string serverPath_;
	const std::shared_ptr<EmbeddersCache> cache_;
	const EmbedderConfig config_;
	std::unique_ptr<ConnectorPool> pool_;

	class [[nodiscard]] LastError {
	public:
		Error GetLastError() const {
			std::lock_guard lock(mtx_);
			return err_;
		}
		void SetError(Error e) {
			std::lock_guard lock(mtx_);
			err_ = std::move(e);
		}

	private:
		mutable std::mutex mtx_;
		Error err_;
	};

	struct [[nodiscard]] Statistic {
		void Reset();
		PerfStatCounterMT embedderTimes;
		PerfStatCounterCountAvgMT embedderDps;
		PerfStatCounterMT embedderTimesCacheMiss;
		PerfStatCounterMT embedderTimesCache;
		PerfStatCounterMT connectionAwait;
		PerfStatCounterCountAvgMT avgConnInUse;
		PerfStatCounterMT lastSecErrorsCount;
		std::atomic<uint64_t> totalErrorsCount{0};
		std::atomic<uint64_t> totalQueriesCount{0};
		std::atomic<uint64_t> totalEmbedDocumentsCount{0};
		LastError lastError;
		std::atomic_bool lastStatus{true};
	};
	mutable Statistic statistic_;
};

class [[nodiscard]] UpsertEmbedder final : public EmbedderBase {
public:
	UpsertEmbedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
				   const std::shared_ptr<EmbeddersCache>& cache, bool enablePerfStat);
	~UpsertEmbedder() noexcept override = default;

	UpsertEmbedder() = delete;
	UpsertEmbedder(const UpsertEmbedder&) = delete;
	UpsertEmbedder(UpsertEmbedder&&) noexcept = delete;
	UpsertEmbedder& operator=(const UpsertEmbedder&) = delete;
	UpsertEmbedder& operator=(UpsertEmbedder&&) noexcept = delete;

	std::string_view FieldName() const& noexcept { return fieldName_; }
	auto FieldName() const&& noexcept = delete;

	void Calculate(const RdxContext& ctx, std::span<const std::vector<std::pair<std::string, VariantArray>>> sources,
				   h_vector<ConstFloatVector, 1>& products) const;

	const h_vector<std::string, 1>& Fields() const& noexcept { return config_.fields; }
	auto Fields() const&& noexcept = delete;

	EmbedderConfig::Strategy Strategy() const noexcept { return config_.strategy; }
	bool IsAuxiliaryField(std::string_view fieldName) const noexcept;
	void EnablePerfStat(bool enable) const noexcept { enablePerfStat_.store(enable, std::memory_order_relaxed); }

private:
	void checkFields(std::span<const std::vector<std::pair<std::string, VariantArray>>> sources) const;
	mutable std::atomic_bool enablePerfStat_;  // must be read once per call calculate
};

class [[nodiscard]] QueryEmbedder final : public EmbedderBase {
public:
	QueryEmbedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
				  const std::shared_ptr<EmbeddersCache>& cache, bool enablePerfStat);
	~QueryEmbedder() noexcept override = default;

	QueryEmbedder() = delete;
	QueryEmbedder(const UpsertEmbedder&) = delete;
	QueryEmbedder(QueryEmbedder&&) noexcept = delete;
	QueryEmbedder& operator=(const QueryEmbedder&) = delete;
	QueryEmbedder& operator=(QueryEmbedder&&) noexcept = delete;

	void Calculate(const RdxContext& ctx, const std::string& text, h_vector<ConstFloatVector, 1>& products) const;
	void EnablePerfStat(bool enable) const noexcept { enablePerfStat_.store(enable, std::memory_order_relaxed); }

private:
	mutable std::atomic_bool enablePerfStat_;  // must be read once per call calculate
};

}  // namespace reindexer
