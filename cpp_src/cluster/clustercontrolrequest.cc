#include "clustercontrolrequest.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"

namespace reindexer {

void ClusterControlRequestData::GetJSON(WrSerializer& ser) const {
	JsonBuilder request(ser);
	request.Put("type", int(type));
	{
		auto payloadBuilder = request.Object("payload");
		std::visit([&payloadBuilder](const auto& d) { d.GetJSON(payloadBuilder); }, data);
	}
}
Error ClusterControlRequestData::FromJSON(span<char> json) {
	try {
		gason::JsonParser parser;
		auto node = parser.Parse(json);
		Type commandType = Type(node["type"].As<int>());
		switch (commandType) {
			case Type::ChangeLeader: {
				data = SetClusterLeaderCommand{};
				const auto& payloadNode = node["payload"];
				std::visit([&payloadNode](auto& d) { d.FromJSON(payloadNode); }, data);
				break;
			}
			case Type::Empty:
				return Error(errParams, "Unknown cluster command request. Command type [%d].", int(commandType));
		}
		type = commandType;
	} catch (Error& e) {
		return e;
	} catch (...) {
		return Error(errLogic, "Unknown error while processing cluster command request.");
	}
	return errOK;
}

void SetClusterLeaderCommand::GetJSON(JsonBuilder& json) const { json.Put("leaderId", leaderServerId); }
void SetClusterLeaderCommand::FromJSON(const gason::JsonNode& payload) { leaderServerId = payload["leaderId"].As<int>(); }

}  // namespace reindexer
