#include "resultserializer.h"
namespace reindexer {

ResultSerializer::ResultSerializer(bool allowInBuf) : WrSerializer(allowInBuf) {}

void ResultSerializer::PutQueryParams(const QueryResults* results) {
	// Pointer to query results
	PutUInt64(ptrdiff_t(results));

	// Total
	PutInt(results->totalCount);
	// Count of returned items
	PutInt(results->size());
	PutInt(results->haveProcent);

	// Versions of tags matcher
	PutInt(results->ctxs.size());
	for (auto& ctx : results->ctxs) {
		PutInt(ctx.tagsMatcher_.version());
	}
	PutAggregationParams(results);
}

void ResultSerializer::PutAggregationParams(const QueryResults* results) {
	PutInt(results->aggregationResults.size());
	for (auto ar : results->aggregationResults) PutDouble(ar);
}

void ResultSerializer::PutItemParams(const ItemRef& it) {
	PutInt(it.id);
	PutInt16(it.version);
	PutUInt8(it.nsid);
	PutUInt8(it.proc);
	PutUInt64(uint64_t(it.value.Ptr()));
}
}  // namespace reindexer
