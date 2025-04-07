#include "embedder.h"

#include "core/cjson/jsonbuilder.h"
#include "core/embedding/httpconnector.h"
#include "estl/gift_str.h"
#include "tools/logger.h"
#include "vendor/gason/gason.h"

namespace reindexer {

namespace {

using namespace std::string_view_literals;

constexpr std::string_view kDataFieldName("data"sv);
constexpr std::string_view kResultDataName("products"sv);
constexpr std::string_view kServerPathFormat("/api/v1/embedder/{}/produce?format=text");

h_vector<ConstFloatVector, 1> FromJSON(const gason::JsonNode& root) {
	h_vector<ConstFloatVector, 1> result;

	constexpr size_t kProductDimension = 1024;
	std::vector<float> values;
	values.reserve(kProductDimension);
	for (auto products : root[kResultDataName]) {
		values.resize(0);
		for (auto product : products) {
			values.push_back(product.As<double>());
		}
		result.emplace_back(std::span<float>{values});
	}

	return result;
}

Error FromJSON(std::span<char> json, h_vector<ConstFloatVector, 1>& vector) {
	try {
		gason::JsonParser parser;
		auto root = parser.Parse(json);
		vector = FromJSON(root);
	} catch (const gason::Exception& ex) {
		return {errParseJson, "Embedder parse products: {}", ex.what()};
	} catch (const Error& err) {
		return err;
	}
	return {};
}

}  // namespace

Embedder::Embedder(std::string_view name, int field, EmbedderConfig&& config, PoolConfig&& poolConfig)
	: name_(name), field_(field), config_(std::move(config)) {
	pool_ = std::make_unique<ConnectorPool>(std::move(poolConfig));
	path_ = fmt::format(kServerPathFormat, name);
}

Error Embedder::Calculate(const RdxContext& ctx, std::span<const std::vector<VariantArray>> sources,
						  h_vector<ConstFloatVector, 1>& products) {
	assertrx_dbg(!sources.empty());

	if (!config_.fields.empty()) {
		const auto fldSize = config_.fields.size();
		size_t idx = 0;
		for (const auto& src : sources) {
			if (fldSize != src.size()) {
				return {
					errLogic,
					"Failed to get embedding for '{}'. Not all required data was requested. Requesting {}, expecting {}, source {}",
					name_, src.size(), fldSize, idx};
			}
			++idx;
		}
	}

	products.clear();

	auto sourcesJson = getJson(sources);
	logFmt(LogTrace, "Embedding src: {}", sourcesJson);

	auto res = pool_->GetConnector(ctx);
	if (!res.first.ok()) {
		return res.first;
	}

	auto response = (*res.second).Send(path_, sourcesJson);
	if (response.ok) {
		logFmt(LogTrace, "Embedding data: {}", response.content);
		return FromJSON(giftStr(response.content), products);
	}

	return {errNetwork, "Failed to get embedding for '{}'. Problem with client: {}", name_, response.content};
}

bool Embedder::IsAuxiliaryField(std::string_view fieldName) const noexcept {
	auto res = std::find(config_.fields.cbegin(), config_.fields.cend(), fieldName);
	return res != config_.fields.end();
}

void Embedder::getJson(std::span<const std::vector<VariantArray>> sources, JsonBuilder& json) const {
	auto arrNodeDoc = json.Array(kDataFieldName);
	for (const auto& docSource : sources) {
		auto arrNodeItem = arrNodeDoc.Array(TagName::Empty());
		for (const auto& itemSource : docSource) {
			for (const auto& item : itemSource) {
				arrNodeItem.Put(TagName::Empty(), item.As<std::string>());
			}
		}
		arrNodeItem.End();
	}
	arrNodeDoc.End();
}

std::string Embedder::getJson(std::span<const std::vector<VariantArray>> sources) const {
	WrSerializer ser;
	{
		JsonBuilder json{ser};
		getJson(sources, json);
	}
	return std::string{ser.Slice()};
}

}  // namespace reindexer
