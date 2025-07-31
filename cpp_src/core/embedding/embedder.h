#pragma once

#include <span>
#include <string_view>
#include "core/embedding/connectorpool.h"
#include "core/embedding/embeddingconfig.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"
#include "tools/errors.h"

namespace reindexer {

class RdxContext;
class EmbeddersCache;
struct EmbedderCachePerfStat;

class [[nodiscard]] EmbedderBase {
public:
	virtual ~EmbedderBase() noexcept = default;

	EmbedderBase() = delete;
	EmbedderBase(const EmbedderBase&) = delete;
	EmbedderBase(EmbedderBase&&) noexcept = delete;
	EmbedderBase& operator=(const EmbedderBase&) = delete;
	EmbedderBase& operator=(EmbedderBase&&) noexcept = delete;

	[[nodiscard]] EmbedderCachePerfStat GetPerfStat(std::string_view tag) const;

protected:
	EmbedderBase(std::string_view name, std::string_view format, std::string_view fieldName, EmbedderConfig&& config,
				 PoolConfig&& poolConfig, const std::shared_ptr<EmbeddersCache>& cache);

	const std::string name_;
	const std::string fieldName_;
	const std::string serverPath_;
	const std::shared_ptr<EmbeddersCache> cache_;
	const EmbedderConfig config_;
	std::unique_ptr<ConnectorPool> pool_;
};

class [[nodiscard]] UpsertEmbedder final : public EmbedderBase {
public:
	UpsertEmbedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
				   const std::shared_ptr<EmbeddersCache>& cache);
	~UpsertEmbedder() noexcept override = default;

	UpsertEmbedder() = delete;
	UpsertEmbedder(const UpsertEmbedder&) = delete;
	UpsertEmbedder(UpsertEmbedder&&) noexcept = delete;
	UpsertEmbedder& operator=(const UpsertEmbedder&) = delete;
	UpsertEmbedder& operator=(UpsertEmbedder&&) noexcept = delete;

	[[nodiscard]] std::string_view FieldName() const& noexcept { return fieldName_; }
	[[nodiscard]] auto FieldName() const&& noexcept = delete;

	void Calculate(const RdxContext& ctx, std::span<const std::vector<std::pair<std::string, VariantArray>>> sources,
				   h_vector<ConstFloatVector, 1>& products) const;

	[[nodiscard]] const h_vector<std::string, 1>& Fields() const& noexcept { return config_.fields; }
	[[nodiscard]] auto Fields() const&& noexcept = delete;

	[[nodiscard]] EmbedderConfig::Strategy Strategy() const noexcept { return config_.strategy; }
	[[nodiscard]] bool IsAuxiliaryField(std::string_view fieldName) const noexcept;

private:
	void checkFields(std::span<const std::vector<std::pair<std::string, VariantArray>>> sources) const;
};

class [[nodiscard]] QueryEmbedder final : public EmbedderBase {
public:
	QueryEmbedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
				  const std::shared_ptr<EmbeddersCache>& cache);
	~QueryEmbedder() noexcept override = default;

	QueryEmbedder() = delete;
	QueryEmbedder(const UpsertEmbedder&) = delete;
	QueryEmbedder(QueryEmbedder&&) noexcept = delete;
	QueryEmbedder& operator=(const QueryEmbedder&) = delete;
	QueryEmbedder& operator=(QueryEmbedder&&) noexcept = delete;

	void Calculate(const RdxContext& ctx, const std::string& text, h_vector<ConstFloatVector, 1>& products) const;
};

}  // namespace reindexer
