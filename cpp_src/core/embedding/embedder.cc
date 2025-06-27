#include "embedder.h"

#include <algorithm>
#include "core/embedding/embedderscache.h"
#include "core/embedding/httpconnector.h"
#include "tools/logger.h"

namespace reindexer {

namespace {
constexpr std::string_view kServerPathFormat("/api/v1/embedder/{}/produce?format=text");
}  // namespace

Embedder::Embedder(std::string_view name, std::string_view fieldName, EmbedderConfig&& config, PoolConfig&& poolConfig,
				   const std::shared_ptr<EmbeddersCache>& cache)
	: name_{name}, fieldName_{fieldName}, serverPath_{fmt::format(kServerPathFormat, name)}, cache_{cache}, config_{std::move(config)} {
	pool_ = std::make_unique<ConnectorPool>(std::move(poolConfig));
}

void Embedder::Calculate(const RdxContext& ctx, std::span<const std::vector<VariantArray>> sources,
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

	auto response = (*res.second).Send(serverPath_, srcAdapter.View());
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

bool Embedder::IsAuxiliaryField(std::string_view fieldName) const noexcept {
	return std::ranges::find(config_.fields, fieldName) != config_.fields.end();
}

EmbedderCachePerfStat Embedder::GetPerfStat(std::string_view tag) const {
	if (cache_) {
		return cache_->GetPerfStat(tag);
	}
	return {};
}

bool Embedder::IsEqual(const EmbedderConfig& embedderCfg, const PoolConfig& poolCfg) const noexcept {
	return (config_ == embedderCfg && pool_->IsEqual(poolCfg));
}

void Embedder::checkFields(std::span<const std::vector<VariantArray>> sources) const {
	if (config_.fields.empty()) {
		return;
	}

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

}  // namespace reindexer
