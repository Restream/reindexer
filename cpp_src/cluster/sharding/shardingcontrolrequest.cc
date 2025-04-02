#include "shardingcontrolrequest.h"
#include "core/cjson/jsonbuilder.h"
#include "tools/catch_and_return.h"
#include "vendor/gason/gason.h"

namespace reindexer::sharding {

template <typename ControlDataT>
static void getJSON(const ControlDataT& shardingControl, WrSerializer& ser) {
	JsonBuilder request(ser);
	request.Put("type", int(shardingControl.type));
	{
		auto payloadBuilder = request.Object("payload");
		std::visit([&payloadBuilder](const auto& d) { d.GetJSON(payloadBuilder); }, shardingControl.data);
	}
}

template <typename ControlDataT>
static Error fromJSON(ControlDataT& shardingControl, std::span<char> json) noexcept {
	try {
		gason::JsonParser parser;
		auto node = parser.Parse(json);
		shardingControl = ControlDataT(ControlCmdType(node["type"].As<int>()));
		std::visit([&node](auto& d) { d.FromJSON(node["payload"]); }, shardingControl.data);
	}
	CATCH_AND_RETURN
	return errOK;
}

void ShardingControlRequestData::GetJSON(WrSerializer& ser) const { return getJSON(*this, ser); }
void ShardingControlResponseData::GetJSON(WrSerializer& ser) const { return getJSON(*this, ser); }

Error ShardingControlRequestData::FromJSON(std::span<char> json) noexcept { return fromJSON(*this, json); }
Error ShardingControlResponseData::FromJSON(std::span<char> json) noexcept { return fromJSON(*this, json); }

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

void GetNodeConfigCommand::GetJSON(JsonBuilder& json) const { json.Put("config", config.GetJSON(cluster::MaskingDSN(masking))); }

void GetNodeConfigCommand::FromJSON(const gason::JsonNode& payload) {
	auto cfg = payload["config"].As<std::string>();
	auto err = config.FromJSON(std::span(cfg));
	if (!err.ok()) {
		throw err;
	}
}

}  // namespace reindexer::sharding
