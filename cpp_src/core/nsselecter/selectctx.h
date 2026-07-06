#pragma once

#include "core/enums.h"
#include "core/query/query.h"
#include "core/query/queryentry.h"
#include "explaincalc.h"
#include "selectctx_traits.h"
#include "sortingcontext.h"

namespace reindexer {

class Query;
class FtFunctionsHolder;
class FloatVectorsHolderMap;

struct [[nodiscard]] SelectCtx {
	explicit SelectCtx(const Query& query_, const Query* parentQuery_, FloatVectorsHolderMap* fvHolder) noexcept
		: query(query_), offset(query.Offset()), limit(query.Limit()), parentQuery(parentQuery_), floatVectorsHolder(fvHolder) {}
	const Query& query;
	ItemsProcessors* joinItemsProcessors = nullptr;
	FtFunctionsHolder* functions = nullptr;
	bool HasOffset() const noexcept { return offset != QueryEntry::kDefaultOffset; }
	bool HasLimit() const noexcept { return limit != QueryEntry::kDefaultLimit; }

	Explain::Duration joinPreSelectTimeTotal = Explain::Duration::zero();
	SortingContext sortingContext;
	uint8_t nsid = 0;
	bool isForceAll = false;
	bool skipIndexesLookup = false;
	bool matchedAtLeastOnce = false;
	bool reqMatchedOnceFlag = false;
	bool contextCollectingMode = false;
	bool inTransaction = false;
	bool selectBeforeUpdate = false;
	IsMergeQuery isMergeQuery = IsMergeQuery_False;
	QueryRankType queryRankType = QueryRankType::NotSet;
	QueryType crashReporterQueryType = QuerySelect;
	unsigned offset = QueryEntry::kDefaultOffset;
	unsigned limit = QueryEntry::kDefaultLimit;

	const Query* parentQuery = nullptr;
	Explain* explain = nullptr;
	bool requiresCrashTracking = false;
	std::vector<SubQueryExplain> subQueriesExplains;
	FloatVectorsHolderMap* floatVectorsHolder = nullptr;

	RX_ALWAYS_INLINE bool isMergeQuerySubQuery() const noexcept { return isMergeQuery == IsMergeQuery_True && parentQuery; }
};

template <typename JoinPreSelCtx>
struct [[nodiscard]] SelectAndPreSelectCtx : public SelectCtx {
	explicit SelectAndPreSelectCtx(const Query& query, const Query* parentQuery, JoinPreSelCtx preSel,
								   FloatVectorsHolderMap* fvHolder) noexcept
		: SelectCtx(query, parentQuery, fvHolder), preSelect{std::move(preSel)} {}
	JoinPreSelCtx preSelect;
};

template <>
struct [[nodiscard]] SelectAndPreSelectCtx<void> : public SelectCtx {
	explicit SelectAndPreSelectCtx(const Query& query, const Query* parentQuery, FloatVectorsHolderMap* fvHolder) noexcept
		: SelectCtx(query, parentQuery, fvHolder) {}
};
SelectAndPreSelectCtx(const Query&, const Query*, FloatVectorsHolderMap*) -> SelectAndPreSelectCtx<void>;

}  // namespace reindexer
