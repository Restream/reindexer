#pragma once
#include <vector>
#include "core/queryresults/queryresults.h"
#include "estl/h_vector.h"
#include "estl/span.h"
#include "tools/semversion.h"
#include "tools/serializer.h"

namespace reindexer {

class QueryResults;

struct ResultFetchOpts {
	int flags;
	span<int32_t> ptVersions;
	unsigned fetchOffset;
	unsigned fetchLimit;
};

class WrResultSerializer : public WrSerializer {
public:
	WrResultSerializer(const ResultFetchOpts& opts = {0, {}, 0, 0});

	bool PutResults(const QueryResults* results, const SemVersion& rxVersion, QueryResults::ProxiedRefsStorage* storage = nullptr);
	void SetOpts(const ResultFetchOpts& opts) { opts_ = opts; }

private:
	void putQueryParams(const QueryResults* query);
	template <typename ItT>
	void putItemParams(ItT& it, int shardId, QueryResults::ProxiedRefsStorage* storage);
	void putExtraParams(const QueryResults* query);
	void putPayloadType(const QueryResults* results, int nsId);
	ResultFetchOpts opts_;
};

}  // namespace reindexer
