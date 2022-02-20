#pragma once

#include "estl/span.h"
#include "tools/errors.h"

namespace reindexer {

class WrSerializer;

struct ShardedMeta {
	int shardId = ShardingKeyType::ProxyOff;
	std::string data;

	Error FromJSON(span<char> json);
	void GetJSON(WrSerializer &ser) const;
};

}  // namespace reindexer
