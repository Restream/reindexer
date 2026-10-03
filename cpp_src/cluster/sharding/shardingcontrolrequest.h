#pragma once
#include <variant>
#include "cluster/config.h"

namespace gason {
struct JsonNode;
}  // namespace gason

namespace reindexer::sharding {

struct [[nodiscard]] EmptyCommand {
	void GetJSON(JsonBuilder&) const {}
	void FromJSON(const gason::JsonNode&) {}
};

struct [[nodiscard]] ApplyLeaderConfigCommand {
	ApplyLeaderConfigCommand() = default;
	ApplyLeaderConfigCommand(std::string_view config, std::optional<int64_t> sourceId) noexcept : config(config), sourceId(sourceId) {}

	std::string_view config;
	std::optional<int64_t> sourceId;

	void GetJSON(JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct [[nodiscard]] SaveConfigCommand {
	SaveConfigCommand() = default;
	SaveConfigCommand(std::string_view config, int64_t sourceId) noexcept : config(config), sourceId(sourceId) {}

	std::string_view config;
	int64_t sourceId;

	void GetJSON(JsonBuilder& json) const;
	void FromJSON(const gason::JsonNode& payload);
};

struct [[nodiscard]] ApplyConfigCommand {
	ApplyConfigCommand() = default;
	ApplyConfigCommand(int64_t sourceId) noexcept : sourceId(sourceId) {}

	int64_t sourceId;

	void GetJSON(JsonBuilder&) const;
	void FromJSON(const gason::JsonNode&);
};

struct [[nodiscard]] ResetConfigCommand {
	ResetConfigCommand() = default;
	ResetConfigCommand(int64_t sourceId) noexcept : sourceId(sourceId) {}

	int64_t sourceId;

	void GetJSON(JsonBuilder&) const;
	void FromJSON(const gason::JsonNode&);
};

struct [[nodiscard]] GetNodeConfigCommand {
	GetNodeConfigCommand() = default;
	GetNodeConfigCommand(cluster::ShardingConfig config) noexcept : config(std::move(config)) {}

	cluster::ShardingConfig config;
	bool masking = true;

	void GetJSON(JsonBuilder&) const;
	void FromJSON(const gason::JsonNode&);
};

enum class [[nodiscard]] ControlCmdType : int {
	SaveCandidate = 0,
	ResetOldSharding = 1,
	ResetCandidate = 2,
	RollbackCandidate = 3,
	ApplyNew = 4,
	ApplyLeaderConfig = 5,
	GetNodeConfig = 6,
};

using ShargindCommandDataType =
	std::variant<EmptyCommand, SaveConfigCommand, ApplyConfigCommand, ResetConfigCommand, ApplyLeaderConfigCommand, GetNodeConfigCommand>;

template <typename T, typename... Args>
void assign_if_constructible(T& data, Args&&... args) {
	if constexpr (std::is_constructible_v<T, Args...>) {
		data = T(std::forward<Args>(args)...);
	}
}

struct [[nodiscard]] ShardingControlRequestData {
	ShardingControlRequestData() noexcept = default;

	Error FromJSON(std::span<char> json) noexcept;
	void GetJSON(WrSerializer& ser) const;

	template <typename... Args>
	constexpr ShardingControlRequestData(ControlCmdType type, Args&&... args)
		: type(type), data([type]() -> ShargindCommandDataType {
			  switch (type) {
				  case ControlCmdType::SaveCandidate:
					  return SaveConfigCommand();
				  case ControlCmdType::ResetOldSharding:
				  case ControlCmdType::ResetCandidate:
				  case ControlCmdType::RollbackCandidate:
					  return ResetConfigCommand();
				  case ControlCmdType::ApplyNew:
					  return ApplyConfigCommand();
				  case ControlCmdType::ApplyLeaderConfig:
					  return ApplyLeaderConfigCommand();
				  case ControlCmdType::GetNodeConfig:
					  return GetNodeConfigCommand();
				  default:
					  assertrx(false);
					  return {};
			  }
		  }()) {
		std::visit([&](auto& d) { assign_if_constructible(d, std::forward<Args>(args)...); }, data);
	}

	ControlCmdType type;
	ShargindCommandDataType data;
};

struct [[nodiscard]] ShardingControlResponseData {
	ShardingControlResponseData() noexcept = default;

	Error FromJSON(std::span<char> json) noexcept;
	void GetJSON(WrSerializer& ser) const;

	template <typename... Args>
	constexpr ShardingControlResponseData(ControlCmdType type, Args&&... args)
		: type(type), data([type]() -> ShargindCommandDataType {
			  switch (type) {
				  case ControlCmdType::GetNodeConfig:
					  return GetNodeConfigCommand();
				  case ControlCmdType::SaveCandidate:
				  case ControlCmdType::ResetOldSharding:
				  case ControlCmdType::ResetCandidate:
				  case ControlCmdType::RollbackCandidate:
				  case ControlCmdType::ApplyNew:
				  case ControlCmdType::ApplyLeaderConfig:
					  return EmptyCommand();
				  default:
					  assertrx(false);
					  return {};
			  }
		  }()) {
		std::visit([&](auto& d) { assign_if_constructible(d, std::forward<Args>(args)...); }, data);
	}

	ControlCmdType type;
	ShargindCommandDataType data;
};

}  // namespace reindexer::sharding
