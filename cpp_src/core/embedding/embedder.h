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

class [[nodiscard]] Embedder final {
public:
	Embedder(std::string_view name, std::string_view fieldName, int field, EmbedderConfig&& config, PoolConfig&& poolConfig,
			 const std::shared_ptr<EmbeddersCache>& cache);
	~Embedder() noexcept = default;

	Embedder() = delete;
	Embedder(const Embedder&) = delete;
	Embedder(Embedder&&) noexcept = delete;
	Embedder& operator=(const Embedder&) = delete;
	Embedder& operator=(Embedder&&) noexcept = delete;

	[[nodiscard]] std::string_view Name() const& noexcept { return name_; }
	[[nodiscard]] auto Name() const&& noexcept = delete;
	[[nodiscard]] std::string_view FieldName() const& noexcept { return fieldName_; }
	[[nodiscard]] auto FieldName() const&& noexcept = delete;

	[[nodiscard]] int Field() const noexcept { return field_; }

	void Calculate(const RdxContext& ctx, std::span<const std::vector<VariantArray>> sources, h_vector<ConstFloatVector, 1>& products);

	[[nodiscard]] const h_vector<std::string, 1>& Fields() const& noexcept { return config_.fields; }
	[[nodiscard]] auto Fields() const&& noexcept = delete;

	[[nodiscard]] EmbedderConfig::Strategy Strategy() const noexcept { return config_.strategy; }
	[[nodiscard]] bool IsAuxiliaryField(std::string_view fieldName) const noexcept;

	[[nodiscard]] EmbedderCachePerfStat GetPerfStat(std::string_view tag) const;

private:
	void checkFields(std::span<const std::vector<VariantArray>> sources) const;

	const std::string name_;
	const std::string fieldName_;
	const int field_{0};
	const std::string serverPath_;
	const std::shared_ptr<EmbeddersCache> cache_;
	const EmbedderConfig config_;
	std::unique_ptr<ConnectorPool> pool_;
};

}  // namespace reindexer
