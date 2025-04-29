#pragma once

#include <span>
#include <string_view>
#include "core/embedding/connectorpool.h"
#include "core/embedding/embeddingconfig.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"

namespace reindexer {

class JsonBuilder;
class RdxContext;

class Embedder final {
public:
	Embedder(std::string_view name, std::string_view fieldName, int field, EmbedderConfig&& config, PoolConfig&& poolConfig);
	~Embedder() noexcept = default;

	Embedder() = delete;
	Embedder(const Embedder&) = delete;
	Embedder(Embedder&&) noexcept = delete;
	Embedder& operator=(const Embedder&) = delete;
	Embedder& operator=(Embedder&&) noexcept = delete;

	std::string_view Name() const& noexcept { return name_; }
	auto Name() const&& noexcept = delete;
	std::string_view FieldName() const& noexcept { return fieldName_; }
	auto FieldName() const&& noexcept = delete;

	int Field() const noexcept { return field_; }

	Error Calculate(const RdxContext& ctx, std::span<const std::vector<VariantArray>> sources, h_vector<ConstFloatVector, 1>& products);

	const h_vector<std::string, 1>& Fields() const& noexcept { return config_.fields; }
	auto Fields() const&& noexcept = delete;

	EmbedderConfig::Strategy Strategy() const noexcept { return config_.strategy; }

	bool IsAuxiliaryField(std::string_view fieldName) const noexcept;

private:
	void getJson(std::span<const std::vector<VariantArray>> sources, JsonBuilder& json) const;
	std::string getJson(std::span<const std::vector<VariantArray>> sources) const;

	const std::string name_;
	const std::string fieldName_;
	const int field_{0};
	std::string path_;
	const EmbedderConfig config_;
	std::unique_ptr<ConnectorPool> pool_;
};

}  // namespace reindexer
