#pragma once
#include <estl/fast_hash_map.h>
#include <unordered_map>
#include <variant>
#include "tools/errors.h"

namespace reindexer {
class RdxContext;
}

namespace reindexer::proxycb {

enum class [[nodiscard]] Type {
	kSharding,
	kCluster,
};

enum class [[nodiscard]] ActionType {
	kApplyShardingConfig,
	kNone,
};

const fast_hash_map<std::string_view, std::pair<Type, ActionType>> kActions = {
	{"apply_sharding_config", {Type::kSharding, ActionType::kApplyShardingConfig}}};

using CallbackFT = std::function<Error(ActionType actionType, std::string_view info, const RdxContext& ctx)>;
using CallbackMap = fast_hash_map<Type, CallbackFT>;
}  // namespace reindexer::proxycb