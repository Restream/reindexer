#pragma once

#include <optional>
#include "core/query/query_impl.h"
#include "explaincalc.h"
#include "sortingcontext.h"

namespace reindexer {

class FtFunctionsHolder;
class FloatVectorsHolderMap;

struct [[nodiscard]] SelectCtx {
	explicit SelectCtx(ConstQueryImpl query_, std::optional<ConstQueryImpl> parentQuery_, FloatVectorsHolderMap* fvHolder) noexcept
		: query(query_), offset(query.Offset()), limit(query.Limit()), parentQuery(parentQuery_), floatVectorsHolder(fvHolder) {}

	SelectCtx& operator=(const SelectCtx&) = delete;
	SelectCtx& operator=(SelectCtx&&) = delete;

	ConstQueryImpl query;
	std::span<joins::ItemsProcessor> joinItemsProcessors;
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
	IsMergeQuery isMergeQuery = IsMergeQuery_False;
	QueryRankType queryRankType = QueryRankType::NotSet;
	QueryType crashReporterQueryType = QuerySelect;
	unsigned offset = QueryEntry::kDefaultOffset;
	unsigned limit = QueryEntry::kDefaultLimit;

	std::optional<ConstQueryImpl> parentQuery;
	Explain* explain = nullptr;
	bool requiresCrashTracking = false;
	std::vector<SubQueryExplain> subQueriesExplains;
	FloatVectorsHolderMap* floatVectorsHolder = nullptr;

	RX_ALWAYS_INLINE bool isMergeQuerySubQuery() const noexcept { return isMergeQuery == IsMergeQuery_True && parentQuery; }
};

template <typename JoinPreSelCtx>
struct [[nodiscard]] SelectAndPreSelectCtx : public SelectCtx {
	explicit SelectAndPreSelectCtx(ConstQueryImpl query, std::optional<ConstQueryImpl> parentQuery, JoinPreSelCtx preSel,
								   FloatVectorsHolderMap* fvHolder) noexcept
		: SelectCtx(query, parentQuery, fvHolder), preSelect{std::move(preSel)} {}
	JoinPreSelCtx preSelect;
};

template <>
struct [[nodiscard]] SelectAndPreSelectCtx<void> : public SelectCtx {
	explicit SelectAndPreSelectCtx(ConstQueryImpl query, std::optional<ConstQueryImpl> parentQuery,
								   FloatVectorsHolderMap* fvHolder) noexcept
		: SelectCtx(query, parentQuery, fvHolder) {}
};
SelectAndPreSelectCtx(ConstQueryImpl, std::optional<ConstQueryImpl>, FloatVectorsHolderMap*) -> SelectAndPreSelectCtx<void>;

}  // namespace reindexer
