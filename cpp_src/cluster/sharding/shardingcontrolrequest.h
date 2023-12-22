#pragma once
#include <variant>
#include "estl/span.h"
#include "tools/compiletimemap.h"
#include "tools/errors.h"
#include "tools/serializer.h"

namespace reindexer {
class JsonBuilder;
}

namespace gason {
struct JsonNode;
}
namespace reindexer::sharding {

struct ApplyLeaderConfigCommand {
	ApplyLeaderConfigCommand() = default;
	ApplyLeaderConfigCommand(std::string_view config, std::optional<int64_t> sourceId) noexcept : config(config), sourceId(sourceId) {}

	std::string_view config;
	std::optional<int64_t> sourceId;

	void GetJSON(JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct SaveConfigCommand {
	SaveConfigCommand() = default;
	SaveConfigCommand(std::string_view config, int64_t sourceId) noexcept : config(config), sourceId(sourceId) {}

	std::string_view config;
	int64_t sourceId;

	void GetJSON(JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct ApplyConfigCommand {
	ApplyConfigCommand() = default;
	ApplyConfigCommand(int64_t sourceId) noexcept : sourceId(sourceId) {}

	int64_t sourceId;

	void GetJSON(JsonBuilder&) const;
	void FromJSON(const gason::JsonNode&);
};

struct ResetConfigCommand {
	ResetConfigCommand() = default;
	ResetConfigCommand(int64_t sourceId) noexcept : sourceId(sourceId) {}

	int64_t sourceId;

	void GetJSON(JsonBuilder&) const;
	void FromJSON(const gason::JsonNode&);
};

struct ShardingControlRequestData {
	enum class Type {
		SaveCandidate = 0,
		ResetOldSharding = 1,
		ResetCandidate = 2,
		RollbackCandidate = 3,
		ApplyNew = 4,
		ApplyLeaderConfig = 5
	};

private:
	using CommandDataType = std::variant<SaveConfigCommand, ApplyConfigCommand, ResetConfigCommand, ApplyLeaderConfigCommand>;
	using Enum2Type = meta::Map<RDX_META_PAIR(Type::SaveCandidate, sharding::SaveConfigCommand),
								RDX_META_PAIR(Type::ResetOldSharding, sharding::ResetConfigCommand),
								RDX_META_PAIR(Type::ApplyNew, sharding::ApplyConfigCommand),
								RDX_META_PAIR(Type::ResetCandidate, sharding::ResetConfigCommand),
								RDX_META_PAIR(Type::RollbackCandidate, sharding::ResetConfigCommand),
								RDX_META_PAIR(Type::ApplyLeaderConfig, sharding::ApplyLeaderConfigCommand)>;

	template <Type type, typename... Args>
	friend ShardingControlRequestData MakeRequestData(Args&&... args) noexcept;

	// this constructor required only for support MSVC-compiler
	template <typename T>
	ShardingControlRequestData(Type type, T&& data) : type(type), data(std::move(data)) {}

public:
	ShardingControlRequestData() = default;

	void GetJSON(WrSerializer& ser) const;
	[[nodiscard]] Error FromJSON(span<char> json) noexcept;

	Type type;
	CommandDataType data;
};

template <ShardingControlRequestData::Type type, typename... Args>
ShardingControlRequestData MakeRequestData(Args&&... args) noexcept {
	using DataType = ShardingControlRequestData::Enum2Type::GetType<type>;
	static_assert(std::is_nothrow_constructible_v<DataType, Args...>);
	return {type, DataType(std::forward<Args>(args)...)};
}

}  // namespace reindexer::sharding
