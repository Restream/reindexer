#include "shardingcontrolrequest.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"
#include "tools/catch_and_return.h"

namespace reindexer::sharding {

void ShardingControlRequestData::GetJSON(WrSerializer& ser) const {
	JsonBuilder request(ser);
	request.Put("type", int(type));
	{
		auto payloadBuilder = request.Object("payload");
		std::visit([&payloadBuilder](const auto& d) { d.GetJSON(payloadBuilder); }, data);
	}
}

[[nodiscard]] Error ShardingControlRequestData::FromJSON(span<char> json) noexcept {
	try {
		gason::JsonParser parser;
		auto node = parser.Parse(json);
		Type commandType = Type(node["type"].As<int>());

		switch (commandType) {
			case Type::SaveCandidate:
				data = SaveConfigCommand{};
				break;
			case Type::ApplyLeaderConfig:
				data = ApplyLeaderConfigCommand{};
				break;
			case Type::ApplyNew:
				data = ApplyConfigCommand{};
				break;
			case Type::ResetOldSharding:
			case Type::ResetCandidate:
			case Type::RollbackCandidate:
				data = ResetConfigCommand{};
				break;
			default:
				return Error(errParams, "Unknown sharding command request. Command type [%d].", int(commandType));
		}

		const auto& payloadNode = node["payload"];
		std::visit([&payloadNode](auto& d) { d.FromJSON(payloadNode); }, data);
		type = commandType;
	}
	CATCH_AND_RETURN
	return errOK;
}

void ApplyLeaderConfigCommand::GetJSON(JsonBuilder& json) const {
	json.Put("config", config);
	if (sourceId.has_value()) {
		json.Put("source_id", *sourceId);
	}
}

void ApplyLeaderConfigCommand::FromJSON(const gason::JsonNode& payload) {
	config = payload["config"].As<std::string_view>();
	auto& sourceIdNode = payload["source_id"];
	sourceId = sourceIdNode.empty() ? std::optional<int64_t>() : sourceIdNode.As<int64_t>();
}

void SaveConfigCommand::GetJSON(JsonBuilder& json) const {
	json.Put("config", config);
	json.Put("source_id", sourceId);
}

void SaveConfigCommand::FromJSON(const gason::JsonNode& payload) {
	config = payload["config"].As<std::string_view>();
	sourceId = payload["source_id"].As<int64_t>();
}

void ApplyConfigCommand::GetJSON(JsonBuilder& json) const { json.Put("source_id", sourceId); }

void ApplyConfigCommand::FromJSON(const gason::JsonNode& payload) { sourceId = payload["source_id"].As<int64_t>(); }

void ResetConfigCommand::GetJSON(JsonBuilder& json) const { json.Put("source_id", sourceId); }

void ResetConfigCommand::FromJSON(const gason::JsonNode& payload) { sourceId = payload["source_id"].As<int64_t>(); }

}  // namespace reindexer::sharding
