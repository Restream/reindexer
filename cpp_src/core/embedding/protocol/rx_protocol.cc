#include "rx_protocol.h"

#include "core/cjson/jsonbuilder.h"
#include "core/embedding/protocol/protocol_common.h"
#include "estl/chunk.h"
#include "fmt/format.h"
#include "tools/errors.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer::embedding {
namespace {

constexpr std::string_view kEmbedding{"embedding"};
constexpr std::string_view kEmbedderModel{"model"};
constexpr std::string_view kEmbedderFieldsFormat{"fields_format"};
constexpr std::string_view kEmbedderProtocolOpenAI{"openai"};
constexpr std::string_view kEmbedderURL{"URL"};
constexpr std::string_view kDataFieldName{"data"};
constexpr std::string_view kResultDataName{"products"};
constexpr std::string_view kEmbeddingField{"embedding"};
constexpr std::string_view kServerPathFormat{"/api/v1/embedder/{}/produce?format={}"};
constexpr size_t kProductDimension{1024};

}  // namespace

void RxEmbedProtocol::Validate(const ProtocolConfigView& cfg, std::string_view embedderName) const {
	if (!cfg.model.empty()) {
		throw Error{errParams,		"Configuration '{}:{}' field '{}' is only supported with protocol '{}'",
					kEmbedding,		embedderName,
					kEmbedderModel, kEmbedderProtocolOpenAI};
	}
	if (cfg.fieldsFormat != EmbedderConfig::FieldsFormat::Stringify) {
		throw Error{errParams,
					"Configuration '{}:{}' field '{}' is only supported with protocol '{}'",
					kEmbedding,
					embedderName,
					kEmbedderFieldsFormat,
					kEmbedderProtocolOpenAI};
	}
	if (!MatchHttpUrl(cfg.endpointUrl)) {
		throw Error{errParams,	  "Configuration '{}:{}' contain field '{}' with unexpected value: '{}'",
					kEmbedding,	  embedderName,
					kEmbedderURL, cfg.endpointUrl};
	}
}

EndpointParts RxEmbedProtocol::ResolveEndpoint(const std::string& endpointUrl, std::string_view embedderName,
											   std::string_view format) const {
	return EndpointParts{endpointUrl, fmt::format(kServerPathFormat, embedderName, format)};
}

void RxEmbedProtocol::PrepareQuery(std::string_view text, std::string_view /*model*/, PreparedEmbedderRequest& out) const {
	BuildQueryView(text, out.view);
	out.cacheKey.clear();
}

void RxEmbedProtocol::PrepareUpsert(std::span<const DocSource> sources, EmbedderConfig::FieldsFormat /*fieldsFormat*/,
									std::string_view /*model*/, PreparedEmbedderRequest& out) const {
	BuildUpsertView(sources, out.view);
	out.cacheKey.clear();
}

chunk RxEmbedProtocol::BuildRequest(const PreparedEmbedderRequest& request) const {
	WrSerializer ser;
	{  // {'data':[*view_*]}
		JsonBuilder json{ser};
		auto arrNodeDoc = json.Array(kDataFieldName);
		arrNodeDoc.Raw(request.view);
	}
	return ser.DetachChunk();
}

void RxEmbedProtocol::ParseResponse(const gason::JsonNode& root, ValueT& result) const {
	if (!root.isObject()) [[unlikely]] {
		throw Error{errParseJson, "Embedding service response must be a JSON object"};
	}
	const auto productsNode = root[kResultDataName];
	if (productsNode.isEmpty()) [[unlikely]] {
		throw Error{errParseJson, "Embedding service response does not contain 'products'"};
	}
	if (!productsNode.isArray()) [[unlikely]] {
		throw Error{errParseJson, "Embedding service response field 'products' must be an array"};
	}

	static thread_local std::vector<float> values(kProductDimension);
	for (auto group : productsNode) {
		if (!group.isArray()) [[unlikely]] {
			throw Error{errParseJson, "Embedding service response item in 'products' must be an array"};
		}
		for (auto product : group) {
			if (!product.isObject()) [[unlikely]] {
				throw Error{errParseJson, "Embedding service response product must be an object"};
			}
			const auto embeddingNode = product[kEmbeddingField];
			if (embeddingNode.isEmpty()) [[unlikely]] {
				throw Error{errParseJson, "Embedding service response product does not contain 'embedding'"};
			}
			if (embeddingNode.isNull()) {
				result.emplace_back();
			} else if (embeddingNode.isArray()) {
				values.resize(0);
				for (auto val : embeddingNode) {
					values.emplace_back(val.As<double>());
				}
				result.emplace_back(values);
			} else [[unlikely]] {
				throw Error{errParseJson, "Embedding service response product field 'embedding' must be an array or null"};
			}
		}
	}
}

}  // namespace reindexer::embedding
