#pragma once

#include <span>
#include "tools/errors.h"

namespace reindexer {

class WrSerializer;

struct [[nodiscard]] ShardedMeta {
	ShardedMeta(int _shardId = ShardingKeyType::ProxyOff, std::string&& _data = std::string())
		: shardId(_shardId), data(std::move(_data)) {}

	int shardId;
	std::string data;

	Error FromJSON(std::span<char> json);
	void GetJSON(WrSerializer& ser) const;
};

}  // namespace reindexer
