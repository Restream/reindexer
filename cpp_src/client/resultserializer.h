#pragma once
#include <functional>
#include "tools/serializer.h"
namespace reindexer {
namespace client {

class ResultSerializer : public Serializer {
public:
	using Serializer::Serializer;
	struct ItemParams {
		int id;
		int version;
		int nsid;
		int proc;
		string_view data;
	};

	struct QueryParams {
		int totalcount;
		int qcount;
		int count;
		bool haveProcent;
		bool nonCacheableData;
		bool nsCount;
		h_vector<double, 4> aggResults;
	};

	QueryParams GetRawQueryParams(std::function<void(int nsId)> updatePayloadFunc);
	ItemParams GetItemParams();
};
}  // namespace client
}  // namespace reindexer
