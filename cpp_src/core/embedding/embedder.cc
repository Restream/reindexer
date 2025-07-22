#include "embedder.h"

#include <algorithm>
#include "core/embedding/embedderscache.h"
#include "core/embedding/httpconnector.h"
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

EmbedderCachePerfStat EmbedderBase::GetPerfStat(std::string_view tag) const {
	if (cache_) {
		return cache_->GetPerfStat(tag);
	}
	return {};
}

UpsertEmbedder::UpsertEmbedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
							   const std::shared_ptr<EmbeddersCache>& cache)
	: EmbedderBase(name, kFormatJson, fieldName, std::move(config), std::move(poolConfig), cache) {}

bool UpsertEmbedder::IsAuxiliaryField(std::string_view fieldName) const noexcept {
	return std::ranges::find(config_.fields, fieldName) != config_.fields.end();
}

void UpsertEmbedder::Calculate(const RdxContext& ctx, std::span<const std::vector<std::pair<std::string, VariantArray>>> sources,
							   h_vector<ConstFloatVector, 1>& products) const {
	assertrx_dbg(!sources.empty());

	products.resize(0);

	embedding::Adapter srcAdapter(sources);
	logFmt(LogTrace, "Embedding src: {}", srcAdapter.View());

	checkFields(sources);
	if (cache_) {
		auto product = cache_->Get(config_.tag, srcAdapter);
		if (product.has_value()) {
			products.emplace_back(std::move(product.value()));
			return;	 // NOTE: stop calculation
		}
	}

	auto res = pool_->GetConnector(ctx);
	if (!res.first.ok()) {
		throw res.first;
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
							 const std::shared_ptr<EmbeddersCache>& cache)
	: EmbedderBase(name, kFormatText, fieldName, std::move(config), std::move(poolConfig), cache) {}

void QueryEmbedder::Calculate(const RdxContext& ctx, const std::string& text, h_vector<ConstFloatVector, 1>& products) const {
	products.resize(0);

	embedding::Adapter srcAdapter(text);
	logFmt(LogTrace, "Embedding src: {}", srcAdapter.View());

	if (cache_) {
		auto product = cache_->Get(config_.tag, srcAdapter);
		if (product.has_value()) {
			products.emplace_back(std::move(product.value()));
			return;	 // NOTE: stop calculation
		}
	}

	auto res = pool_->GetConnector(ctx);
	if (!res.first.ok()) {
		throw res.first;
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

}  // namespace reindexer
