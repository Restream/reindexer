#pragma once

#include "core/enums.h"
#include "explaincalc.h"
#include "sortingcontext.h"

namespace reindexer {

class Query;
class SelectFunctionsHolder;
class FloatVectorsHolderMap;

struct SelectCtx {
	explicit SelectCtx(const Query& query_, const Query* parentQuery_, FloatVectorsHolderMap* fvHolder) noexcept
		: query(query_), parentQuery(parentQuery_), floatVectorsHolder(fvHolder) {}
	const Query& query;
	JoinedSelectors* joinedSelectors = nullptr;
	SelectFunctionsHolder* functions = nullptr;

	ExplainCalc::Duration preResultTimeTotal = ExplainCalc::Duration::zero();
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
	RankedTypeQuery rankedTypeQuery = RankedTypeQuery::NotSet;
	QueryType crashReporterQueryType = QuerySelect;

	const Query* parentQuery = nullptr;
	ExplainCalc explain;
	bool requiresCrashTracking = false;
	std::vector<SubQueryExplain> subQueriesExplains;
	FloatVectorsHolderMap* floatVectorsHolder;

	RX_ALWAYS_INLINE bool isMergeQuerySubQuery() const noexcept { return isMergeQuery == IsMergeQuery_True && parentQuery; }
};
}  // namespace reindexer
