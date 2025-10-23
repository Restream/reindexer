#pragma once
#include <span>
#include <variant>
#include "tools/errors.h"
#include "tools/serializer.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

struct [[nodiscard]] SetClusterLeaderCommand {
	int leaderServerId = -1;
	void GetJSON(JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct [[nodiscard]] ForceElectionsCommand {};

struct [[nodiscard]] ClusterControlRequestData {
	enum class [[nodiscard]] Type { Empty = 0, ChangeLeader = 1, ForceEletions = 2 };

	ClusterControlRequestData() = default;
	ClusterControlRequestData(SetClusterLeaderCommand&& value) : type(Type::ChangeLeader), data(std::move(value)) {}
	ClusterControlRequestData(ForceElectionsCommand&& value) : type(Type::ForceEletions), data(std::move(value)) {}
	void GetJSON(WrSerializer& ser) const;
	Error FromJSON(std::span<char> json) noexcept;

	Type type = Type::Empty;
	std::variant<SetClusterLeaderCommand, ForceElectionsCommand> data;
};
}  // namespace reindexer
