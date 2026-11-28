#pragma once
#include <span>
#include <string>
#include <variant>
#include "core/type_consts.h"
#include "tools/errors.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class WrSerializer;

namespace builders {
class JsonBuilder;
}  // namespace builders

struct [[nodiscard]] SetClusterLeaderCommand {
	int leaderServerId = -1;
	void GetJSON(builders::JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct [[nodiscard]] ForceElectionsCommand {};

struct [[nodiscard]] AsyncReplConfigSource {
	std::string config;
	int64_t sourceId = ShardingSourceId::NotSet;
	void GetJSON(builders::JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct [[nodiscard]] ApplyLeaderAsyncReplConfigCommand : AsyncReplConfigSource {};

struct [[nodiscard]] GetAsyncReplConfigSourceCommand : AsyncReplConfigSource {};

struct [[nodiscard]] ClusterControlRequestData {
	enum class [[nodiscard]] Type {
		Empty = 0,
		ChangeLeader = 1,
		ForceElections = 2,
		ApplyLeaderAsyncReplConfig = 3,
		GetAsyncReplConfigSource = 4
	};

	ClusterControlRequestData() = default;
	ClusterControlRequestData(SetClusterLeaderCommand&& value) : type(Type::ChangeLeader), data(std::move(value)) {}
	ClusterControlRequestData(ForceElectionsCommand&& value) : type(Type::ForceElections), data(std::move(value)) {}
	ClusterControlRequestData(ApplyLeaderAsyncReplConfigCommand&& value) : type(Type::ApplyLeaderAsyncReplConfig), data(std::move(value)) {}
	ClusterControlRequestData(GetAsyncReplConfigSourceCommand&& value) : type(Type::GetAsyncReplConfigSource), data(std::move(value)) {}
	void GetJSON(WrSerializer& ser) const;
	Error FromJSON(std::span<char> json) noexcept;

	Type type = Type::Empty;
	std::variant<SetClusterLeaderCommand, ForceElectionsCommand, ApplyLeaderAsyncReplConfigCommand, GetAsyncReplConfigSourceCommand> data;
};

struct [[nodiscard]] ClusterControlResponseData : ClusterControlRequestData {};
}  // namespace reindexer
