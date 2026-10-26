#pragma once

#include "core/embedding/protocol/iembed_protocol.h"

namespace reindexer::embedding {

class [[nodiscard]] OpenAIEmbedProtocol final : public IEmbedProtocol {
public:
	void Validate(const ProtocolConfigView& cfg, std::string_view embedderName) const override;
	EndpointParts ResolveEndpoint(const std::string& endpointUrl, std::string_view embedderName, std::string_view format) const override;
	void PrepareQuery(std::string_view text, std::string_view model, PreparedEmbedderRequest& out) const override;
	void PrepareUpsert(std::span<const DocSource> sources, EmbedderConfig::FieldsFormat fieldsFormat, std::string_view model,
					   PreparedEmbedderRequest& out) const override;
	chunk BuildRequest(const PreparedEmbedderRequest& request) const override;
	void ParseResponse(const gason::JsonNode& root, ValueT& result) const override;
};

}  // namespace reindexer::embedding
