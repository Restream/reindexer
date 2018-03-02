#pragma once
#include "tools/serializer.h"
namespace reindexer {

class QueryResults;

struct ResultFetchOpts {
	int flags;
	const int32_t* ptVersions;
	unsigned fetchOffset;
	unsigned fetchLimit;
	int64_t fetchDataMask;
};

class ResultSerializer : public WrSerializer {
public:
	ResultSerializer(bool allowInBuf, const ResultFetchOpts& opts = {0, nullptr, 0, 0, 0});

	bool PutResults(const QueryResults* results);

private:
	void putQueryParams(const QueryResults* query);
	void putItemParams(const QueryResults* result, int idx, bool useOffset);
	void putAggregationParams(const QueryResults* query);
	void putPayloadType(const QueryResults* results, int nsId);
	ResultFetchOpts opts_;
};
}  // namespace reindexer
