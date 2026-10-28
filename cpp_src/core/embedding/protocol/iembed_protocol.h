#pragma once

#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include "core/embedding/embeddingconfig.h"
#include "core/keyvalue/float_vector.h"
#include "core/keyvalue/variant.h"
#include "estl/chunk.h"
#include "estl/h_vector.h"
#include "vendor/gason/gason.h"

namespace reindexer::embedding {

using ValueT = h_vector<FloatVector, 1>;
using DocSource = std::vector<std::pair<std::string, VariantArray>>;

struct [[nodiscard]] EndpointParts {
	std::string baseUrl;
	std::string path;
};

struct [[nodiscard]] ProtocolConfigView {
	const std::string& endpointUrl;
	std::string_view model;
	EmbedderConfig::FieldsFormat fieldsFormat{EmbedderConfig::FieldsFormat::Stringify};
	bool isUpsert{false};
};

struct [[nodiscard]] PreparedEmbedderRequest {
	std::string view;
	std::string cacheKey;
};

class [[nodiscard]] IEmbedProtocol {
public:
	virtual ~IEmbedProtocol() = default;

	// Throws Error on invalid protocol-specific options / URL.
	virtual void Validate(const ProtocolConfigView& cfg, std::string_view embedderName) const = 0;

	virtual EndpointParts ResolveEndpoint(const std::string& endpointUrl, std::string_view embedderName, std::string_view format) const = 0;

	virtual void PrepareQuery(std::string_view text, std::string_view model, PreparedEmbedderRequest& out) const = 0;
	virtual void PrepareUpsert(std::span<const DocSource> sources, EmbedderConfig::FieldsFormat fieldsFormat, std::string_view model,
							   PreparedEmbedderRequest& out) const = 0;

	virtual chunk BuildRequest(const PreparedEmbedderRequest& request) const = 0;

	virtual void ParseResponse(const gason::JsonNode& root, ValueT& result) const = 0;
};

const IEmbedProtocol& GetEmbedProtocol(EmbedderConfig::Protocol protocol);

}  // namespace reindexer::embedding
