#pragma once
#include <variant>
#include "estl/span.h"
#include "tools/errors.h"
#include "tools/serializer.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class JsonBuilder;

struct SetClusterLeaderCommand {
	int leaderServerId = -1;
	void GetJSON(JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct ClusterControlRequestData {
	enum class Type { Empty = 0, ChangeLeader = 1 };

	ClusterControlRequestData() = default;
	ClusterControlRequestData(SetClusterLeaderCommand&& value) : type(Type::ChangeLeader), data(std::move(value)) {}
	void GetJSON(WrSerializer& ser) const;
	Error FromJSON(span<char> json);

	Type type = Type::Empty;
	std::variant<SetClusterLeaderCommand> data;
};
}  // namespace reindexer
