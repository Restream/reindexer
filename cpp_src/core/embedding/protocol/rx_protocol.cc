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
	static thread_local std::vector<float> values(kProductDimension);
	for (auto products : root[kResultDataName]) {
		for (auto product : products) {
			values.resize(0);
			// auto chunk = product["chunk"sv].As<std::string>();
			for (auto val : product[kEmbeddingField]) {
				values.emplace_back(val.As<double>());
			}
			result.emplace_back(values);
		}
	}
}

}  // namespace reindexer::embedding
