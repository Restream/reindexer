#include "openai_protocol.h"

#include <cctype>
#include <optional>
#include <vector>
#include "core/cjson/jsonbuilder.h"
#include "core/embedding/protocol/protocol_common.h"
#include "core/enums.h"
#include "estl/chunk.h"
#include "tools/assertrx.h"
#include "tools/errors.h"
#include "tools/serilize/wrserializer.h"
#include "tools/stringstools.h"
#include "vendor/urlparser/urlparser.h"

namespace reindexer::embedding {
namespace {

constexpr std::string_view kEmbedding{"embedding"};
constexpr std::string_view kEmbedderModel{"model"};
constexpr std::string_view kEmbedderFieldsFormat{"fields_format"};
constexpr std::string_view kEmbedderProtocolOpenAI{"openai"};
constexpr std::string_view kEmbedderURL{"URL"};
constexpr std::string_view kUpsertEmbedder{"upsert_embedder"};
constexpr std::string_view kOpenAIModelField{"model"};
constexpr std::string_view kOpenAIInputField{"input"};
constexpr std::string_view kDataFieldName{"data"};
constexpr std::string_view kEmbeddingField{"embedding"};
constexpr std::string_view kOpenAIIndexField{"index"};
constexpr std::string_view kOpenAIDefaultPath{"/v1/embeddings"};
constexpr std::string_view kOpenAICacheKeyPrefix{"openai-v1:"};
constexpr size_t kProductDimension{1024};

bool isHttpsUrl(std::string_view url) noexcept { return url.size() >= 8 && iequals(url.substr(0, 8), "https://"); }

void appendVariantText(std::string& out, const Variant& value) {
	if (value.Type().Is<KeyValueType::String>()) {
		out.append(std::string_view{value});
	} else {
		out.append(value.As<std::string>());
	}
}

void joinFieldValues(std::string& out, const VariantArray& values) {
	for (size_t i = 0; i < values.size(); ++i) {
		if (i != 0) {
			out.push_back(' ');
		}
		appendVariantText(out, values[i]);
	}
}

void joinDocFields(std::string& out, const DocSource& docSource) {
	out.clear();
	for (size_t i = 0; i < docSource.size(); ++i) {
		if (i != 0) {
			out.push_back('\n');
		}
		joinFieldValues(out, docSource[i].second);
	}
}

void buildOpenAIRequest(std::string_view model, std::string_view input, WrSerializer& ser) {
	JsonBuilder json{ser};
	json.Put(kOpenAIModelField, model);
	json.Put(kOpenAIInputField, input);
}

// OpenAI request input: single string for one document / query.
void assignOpenAICacheKey(std::string_view model, std::string_view input, std::string& out) {
	WrSerializer ser;
	ser.Write(kOpenAICacheKeyPrefix);
	buildOpenAIRequest(model, input, ser);

	const auto slice = ser.Slice();
	out.assign(slice.data(), slice.size());
}

void appendLower(std::string& out, std::string_view src) {
	out.reserve(out.size() + src.size());
	for (unsigned char ch : src) {
		out.push_back(char(std::tolower(ch)));
	}
}

}  // namespace

void OpenAIEmbedProtocol::Validate(const ProtocolConfigView& cfg, std::string_view embedderName) const {
	if (cfg.model.empty()) {
		throw Error{errParams,
					"Configuration '{}:{}' with protocol '{}' must contain field '{}'",
					kEmbedding,
					embedderName,
					kEmbedderProtocolOpenAI,
					kEmbedderModel};
	}
	if (!cfg.isUpsert && cfg.fieldsFormat != EmbedderConfig::FieldsFormat::Stringify) {
		throw Error{
			errParams,		"Configuration '{}:{}' field '{}' is only supported for '{}'", kEmbedding, embedderName, kEmbedderFieldsFormat,
			kUpsertEmbedder};
	}
	if (isHttpsUrl(cfg.endpointUrl)) {
		throw Error{errParams, "Configuration '{}:{}' protocol '{}' does not support HTTPS endpoints", kEmbedding, embedderName,
					kEmbedderProtocolOpenAI};
	}
	if (!MatchHttpUrl(cfg.endpointUrl)) {
		throw Error{errParams,	  "Configuration '{}:{}' contain field '{}' with unexpected value: '{}'",
					kEmbedding,	  embedderName,
					kEmbedderURL, cfg.endpointUrl};
	}
}

EndpointParts OpenAIEmbedProtocol::ResolveEndpoint(const std::string& endpointUrl, std::string_view embedderName,
												   std::string_view /*format*/) const {
	if (isHttpsUrl(endpointUrl)) {
		throw Error{errParams, "Configuration '{}:{}' protocol '{}' does not support HTTPS endpoints", kEmbedding, embedderName,
					kEmbedderProtocolOpenAI};
	}
	if (!MatchHttpUrl(endpointUrl)) {
		throw Error{errParams,	  "Configuration '{}:{}' contain field '{}' with unexpected value: '{}'",
					kEmbedding,	  embedderName,
					kEmbedderURL, endpointUrl};
	}
	httpparser::UrlParser uri{endpointUrl};
	if (!uri.isValid()) {
		throw Error{errParams,	  "Configuration '{}:{}' contain field '{}' with unexpected value: '{}'",
					kEmbedding,	  embedderName,
					kEmbedderURL, endpointUrl};
	}

	EndpointParts parts;
	parts.baseUrl.clear();
	parts.baseUrl.reserve(uri.scheme().size() + 3 + uri.hostname().size() + (uri.port().empty() ? 0 : uri.port().size() + 1));
	appendLower(parts.baseUrl, uri.scheme());
	parts.baseUrl.append("://");
	parts.baseUrl.append(uri.hostname());
	if (!uri.port().empty()) {
		parts.baseUrl.push_back(':');
		parts.baseUrl.append(uri.port());
	}

	if (uri.path().empty() || uri.path() == "/") {
		parts.path.assign(kOpenAIDefaultPath);
	} else {
		parts.path = uri.path();
	}
	if (!uri.query().empty()) {
		parts.path.push_back('?');
		parts.path.append(uri.query());
	}
	return parts;
}

void OpenAIEmbedProtocol::PrepareQuery(std::string_view text, std::string_view model, PreparedEmbedderRequest& out) const {
	BuildQueryView(text, out.view);
	assignOpenAICacheKey(model, text, out.cacheKey);
}

void OpenAIEmbedProtocol::PrepareUpsert(std::span<const DocSource> sources, EmbedderConfig::FieldsFormat fieldsFormat,
										std::string_view model, PreparedEmbedderRequest& out) const {
	if (sources.size() != 1) {
		throw Error{errLogic, "OpenAI embedding supports exactly one document per request"};
	}
	BuildUpsertView(sources, out.view);
	std::string input;
	switch (fieldsFormat) {
		case EmbedderConfig::FieldsFormat::Join:
			joinDocFields(input, sources.front());
			break;
		case EmbedderConfig::FieldsFormat::Stringify:
			input = out.view;
			break;
	}
	assignOpenAICacheKey(model, input, out.cacheKey);
}

chunk OpenAIEmbedProtocol::BuildRequest(const PreparedEmbedderRequest& request) const {
	assertrx(request.cacheKey.starts_with(kOpenAICacheKeyPrefix));
	chunk result;
	result.append_strict(std::string_view{request.cacheKey}.substr(kOpenAICacheKeyPrefix.size()));
	return result;
}

void OpenAIEmbedProtocol::ParseResponse(const gason::JsonNode& root, ValueT& result) const {
	const auto data = root[kDataFieldName];
	if (data.isEmpty()) {
		throw Error{errParseJson, "OpenAI embedding response does not contain 'data'"};
	}

	std::optional<FloatVector> resultVector;
	static thread_local std::vector<float> values(kProductDimension);
	for (auto product : data) {
		const auto indexNode = product[kOpenAIIndexField];
		if (indexNode.isEmpty()) {
			throw Error{errParseJson, "OpenAI embedding response item does not contain 'index'"};
		}
		if (indexNode.As<size_t>(CheckUnsigned_True) != 0) {
			throw Error{errParseJson, "OpenAI embedding response item index must be 0"};
		}
		if (resultVector.has_value()) {
			throw Error{errParseJson, "OpenAI embedding response must contain exactly one item"};
		}

		const auto embeddingNode = product[kEmbeddingField];
		if (embeddingNode.isEmpty()) {
			throw Error{errParseJson, "OpenAI embedding response item does not contain 'embedding'"};
		}
		values.resize(0);
		for (auto val : embeddingNode) {
			values.emplace_back(val.As<double>());
		}
		if (values.empty()) {
			throw Error{errParseJson, "OpenAI embedding response item contains an empty embedding"};
		}
		resultVector.emplace(values);
	}
	if (!resultVector.has_value()) {
		throw Error{errParseJson, "OpenAI embedding response must contain exactly one item"};
	}

	result.emplace_back(std::move(*resultVector));
}

}  // namespace reindexer::embedding
