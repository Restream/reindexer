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
	enum class AggsFlag { DontClearAggregations = 0, ClearAggregations = 1 };

	struct ItemParams {
		int id = 0;
		int16_t nsid = 0;
		int16_t proc = 0;
		int64_t lsn = 0;
		std::string_view data;
		bool raw = false;
	};

	struct QueryParams {
		int totalcount = 0;
		int qcount = 0;
		int count = 0;
		int flags = 0;
		std::vector<AggregationResult> aggResults;
		std::string explainResults;
	};

	void GetRawQueryParams(QueryParams& ret, const std::function<void(int nsId)>& updatePayloadFunc, AggsFlag clearAggs);
	ItemParams GetItemParams(int flags);
};
}  // namespace client
}  // namespace reindexer
