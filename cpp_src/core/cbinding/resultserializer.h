#pragma once
#include "core/query/queryresults.h"
#include "tools/serializer.h"
namespace reindexer {

class ResultSerializer : public WrSerializer {
public:
	ResultSerializer(bool allowInBuf = true);
	void PutQueryParams(const QueryResults* query);
	void PutItemParams(const ItemRef& it);
	void PutAggregationParams(const QueryResults* query);

private:
};
}  // namespace reindexer
