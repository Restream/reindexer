#include "clustercontrolrequest.h"
#include "core/cjson/jsonbuilder.h"
#include "gason/gason.h"

namespace reindexer {

void ClusterControlRequestData::GetJSON(WrSerializer& ser) const {
	JsonBuilder request(ser);
	request.Put("type", int(type));

	std::visit(overloaded{[&request, this](const SetClusterLeaderCommand& d) {
							  assertrx_throw(Type::ChangeLeader == Type(type));
							  auto payloadBuilder = request.Object("payload");
							  d.GetJSON(payloadBuilder);
						  },
						  [this](const ForceElectionsCommand) { assertrx_throw(Type::ForceEletions == Type(type)); }},
			   data);
}
Error ClusterControlRequestData::FromJSON(std::span<char> json) noexcept {
	try {
		gason::JsonParser parser;
		auto node = parser.Parse(json);
		Type commandType = Type(node["type"].As<int>());
		switch (commandType) {
			case Type::ChangeLeader: {
				data = SetClusterLeaderCommand{};
				const auto& payloadNode = node["payload"];
				std::get<SetClusterLeaderCommand>(data).FromJSON(payloadNode);
				break;
			}
			case Type::ForceEletions: {
				data = ForceElectionsCommand{};
				break;
			}
			case Type::Empty:
			default:
				return Error(errParams, "Unknown cluster command request. Command type [{}].", int(commandType));
		}
		type = commandType;
	} catch (std::exception& e) {
		return e;
	} catch (...) {
		return Error(errLogic, "Unknown error while processing cluster command request.");
	}
	return errOK;
}

void SetClusterLeaderCommand::GetJSON(JsonBuilder& json) const { json.Put("leaderId", leaderServerId); }
void SetClusterLeaderCommand::FromJSON(const gason::JsonNode& payload) { leaderServerId = payload["leaderId"].As<int>(); }

}  // namespace reindexer
