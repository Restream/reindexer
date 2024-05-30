#pragma once
#include "estl/span.h"
#include "tools/serializer.h"
namespace reindexer {

class QueryResults;

struct ResultFetchOpts {
	int flags;
	span<int32_t> ptVersions;
	unsigned fetchOffset;
	unsigned fetchLimit;
	bool withAggregations;
};

class WrResultSerializer : public WrSerializer {
public:
	WrResultSerializer(
		const ResultFetchOpts& opts = {.flags = 0, .ptVersions = {}, .fetchOffset = 0, .fetchLimit = 0, .withAggregations = true})
		: opts_(opts) {
		resetUnknownFlags();
	}
	template <unsigned N>
	WrResultSerializer(
		uint8_t (&buf)[N],
		const ResultFetchOpts& opts = {.flags = 0, .ptVersions = {}, .fetchOffset = 0, .fetchLimit = 0, .withAggregations = true})
		: WrSerializer(buf), opts_(opts) {
		resetUnknownFlags();
	}

	bool PutResults(const QueryResults* results);
	void SetOpts(const ResultFetchOpts& opts) { opts_ = opts; }

private:
	void resetUnknownFlags() noexcept;
	void putQueryParams(const QueryResults* query);
	void putItemParams(const QueryResults* result, int idx, bool useOffset);
	void putExtraParams(const QueryResults* query);
	void putPayloadType(const QueryResults* results, int nsId);
	ResultFetchOpts opts_;
};

}  // namespace reindexer
