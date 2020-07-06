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
		string_view data;
		bool raw = false;
	};

	struct QueryParams {
		int totalcount = 0;
		int qcount = 0;
		int count = 0;
		int flags = 0;
		std::vector<AggregationResult> aggResults;
		string explainResults;
	};

	void GetRawQueryParams(QueryParams &ret, std::function<void(int nsId)> updatePayloadFunc);
	ItemParams GetItemParams(int flags);
};
}  // namespace client
}  // namespace reindexer
