#pragma once
#include <functional>
#include "core/queryresults/aggregationresult.h"
#include "tools/serializer.h"

struct msgpack_object;

namespace reindexer {
namespace client {

class ResultSerializer : public Serializer {
public:
	using Serializer::Serializer;

	struct ItemParams {
		int id = 0;
		int16_t nsid = 0;
		int16_t proc = 0;
		int64_t lsn = 0;
		std::string_view data;
		bool raw = false;
		int shardId = ShardingKeyType::ProxyOff;
	};

	struct QueryParams {
		int totalcount = 0;
		int qcount = 0;
		int count = 0;
		int flags = 0;
		std::vector<AggregationResult> aggResults;
		string explainResults;
		int64_t shardingConfigVersion = -1;
		int shardId = ShardingKeyType::ProxyOff;
	};

	void GetRawQueryParams(QueryParams &ret, std::function<void(int nsId)> updatePayloadFunc);
	ItemParams GetItemData(int flags, int shardId);
};

}  // namespace client
}  // namespace reindexer
