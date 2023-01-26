#pragma once

#include "estl/span.h"
#include "tools/errors.h"

namespace reindexer {

class WrSerializer;

struct ShardedMeta {
	ShardedMeta(int _shardId = ShardingKeyType::ProxyOff, std::string&& _data = std::string())
		: shardId(_shardId), data(std::move(_data)) {}

	int shardId;
	std::string data;

	Error FromJSON(span<char> json);
	void GetJSON(WrSerializer& ser) const;
};

}  // namespace reindexer
