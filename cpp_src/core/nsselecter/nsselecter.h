#pragma once
#include <chrono>
#include <functional>
#include "core/aggregator.h"
#include "core/index/index.h"
#include "core/nsselecter/selectiterator.h"
#include "core/query/query.h"
#include "core/query/queryresults.h"
#include "core/selectfunc/ctx/basefunctionctx.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfunc.h"

namespace reindexer {

using std::string;
using std::vector;
using std::chrono::duration_cast;
using std::chrono::microseconds;

struct JoinedSelector {
	typedef std::function<bool(IdType, int nsId, ConstPayload, bool)> FuncType;
	JoinType type;
	bool nodata;
	FuncType func;
	int called, matched;
	string ns;
};

typedef vector<JoinedSelector> JoinedSelectors;

struct SelectCtx {
	explicit SelectCtx(const Query &query_) : query(query_) {}
	const Query &query;
	JoinedSelectors *joinedSelectors = nullptr;

	SelectFunctionsHolder *functions = nullptr;
	struct PreResult {
		enum Mode { ModeBuild, ModeIterators, ModeIdSet, ModeEmpty };

		typedef shared_ptr<PreResult> Ptr;
		IdSet ids;
		h_vector<SelectIterator, 0> iterators;
		Mode mode = ModeEmpty;
		bool enableSortOrders = false;
	};
	struct SortingCtx {
		struct Entry {
			const SortingEntry *data = nullptr;
			Index *index = nullptr;
			const CollateOpts *opts = nullptr;
		};
		h_vector<Entry, 1> entries;
		Index *sortIndex() { return entries.empty() ? nullptr : entries[0].index; }
		int sortId() { return sortIndex() ? sortIndex()->SortId() : 0; }
	};
	PreResult::Ptr preResult;
	SortingCtx sortingCtx;
	uint8_t nsid = 0;
	bool isForceAll = false;
	bool skipIndexesLookup = false;
	bool matchedAtLeastOnce = false;
	bool reqMatchedOnceFlag = false;
	bool enableSortOrders = false;
};

class NsSelecter {
public:
	NsSelecter(Namespace *parent) : ns_(parent) {}
	struct RawQueryResult : public h_vector<SelectIterator> {};

	void operator()(QueryResults &result, SelectCtx &ctx);

private:
	struct LoopCtx {
		LoopCtx(SelectCtx &ctx) : sctx(ctx) {}
		RawQueryResult *qres = nullptr;
		bool calcTotal = false;
		SelectCtx &sctx;
	};

	template <bool reverse, bool haveComparators, bool haveDistinct>
	void selectLoop(LoopCtx &ctx, QueryResults &result);
	void applyCustomSort(ItemRefVector &result, const SelectCtx &ctx);

	using ItemIterator = ItemRefVector::iterator;
	using ConstItemIterator = const ItemIterator &;
	void applyGeneralSort(ConstItemIterator itFirst, ConstItemIterator itLast, ConstItemIterator itEnd, const SelectCtx &ctx);

	bool containsFullTextIndexes(const QueryEntries &entries);
	void prepareIteratorsForSelectLoop(const QueryEntries &entries, RawQueryResult &result, SortType sortId, bool is_ft);
	void prepareEqualPositionComparator(const Query &query, const QueryEntries &entries, RawQueryResult &result);
	void addSelectResult(uint8_t proc, IdType rowId, IdType properRowId, const SelectCtx &sctx, h_vector<Aggregator, 4> &aggregators,
						 QueryResults &result);
	QueryEntries lookupQueryIndexes(const QueryEntries &entries);
	void convertWhereValues(QueryEntries &entries);
	void substituteCompositeIndexes(QueryEntries &entries);
	SortingEntries detectOptimalSortOrder(const QueryEntries &entries);
	h_vector<Aggregator, 4> getAggregators(const Query &q);
	int getCompositeIndex(const FieldsSet &fieldsmask);
	bool mergeQueryEntries(QueryEntry *lhs, QueryEntry *rhs);
	void setLimitAndOffset(ItemRefVector &result, size_t offset, size_t limit);
	KeyValueType detectQueryEntryIndexType(const QueryEntry &qentry) const;
	void prepareSortingContext(const SortingEntries &sortBy, SelectCtx &ctx, bool isFt);
	void prepareSortingIndexes(SortingEntries &sortBy);
	void getSortIndexValue(const SelectCtx::SortingCtx::Entry *sortCtx, IdType rowId, VariantArray &value);
	bool proccessJoin(SelectCtx &sctx, IdType properRowId, bool found, bool match, bool hasInnerJoin);

	Namespace *ns_;
	SelectFunction::Ptr fnc_;
	FtCtx::Ptr ft_ctx_;
};
}  // namespace reindexer
