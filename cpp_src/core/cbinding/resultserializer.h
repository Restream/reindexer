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

	bool PutResults(QueryResults* results, const BindingCapabilities& caps, QueryResults::ProxiedRefsStorage* storage = nullptr);
	bool PutResultsRaw(QueryResults* results, std::string_view* rawBufOut = nullptr);
	void SetOpts(const ResultFetchOpts& opts) noexcept { opts_ = opts; }
	static bool IsRawResultsSupported(const BindingCapabilities& caps, const QueryResults& results) noexcept {
		return !results.HaveShardIDs() || caps.HasResultsWithShardIDs();
	}

private:
	void resetUnknownFlags() noexcept;
	void putQueryParams(const BindingCapabilities& caps, QueryResults* query);
	template <typename ItT>
	void putItemParams(ItT& it, int shardId, QueryResults::ProxiedRefsStorage* storage, const QueryResults* result);
	void putExtraParams(const BindingCapabilities& caps, QueryResults* query);
	static void putPayloadTypes(WrSerializer& ser, const QueryResults* results, const ResultFetchOpts& opts, int cnt, int totalCnt);
	std::pair<int, int> getPtUpdatesCount(const QueryResults* results);
	ResultFetchOpts opts_;
};

}  // namespace reindexer
