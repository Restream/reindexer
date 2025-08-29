#pragma once

#include "estl/h_vector.h"
#include "tools/lsn.h"

namespace reindexer {

struct [[nodiscard]] NsIncarnationTagData {
	int shardId = ShardingKeyType::NotSetShard;
	h_vector<lsn_t, 3> tags;
};

using NsShardsIncarnationTags = h_vector<NsIncarnationTagData, 1>;

}  // namespace reindexer
