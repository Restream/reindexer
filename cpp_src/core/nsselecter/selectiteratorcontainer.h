#pragma once
#include <limits>
#include "core/expressiontree.h"
#include "core/index/index.h"
#include "core/nsselecter/fieldscomparator.h"
#include "core/nsselecter/selectiterator.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfunc.h"

namespace reindexer {

struct SelectCtx;
class RdxContext;
class JoinedSelector;
class QueryPreprocessor;

struct JoinSelectIterator {
	size_t joinIndex;
	double Cost() const noexcept { return std::numeric_limits<float>::max(); }
	void Dump(WrSerializer &, const std::vector<JoinedSelector> &) const;
};

struct SelectIteratorsBracket : private Bracket {
	using Bracket::Bracket;
	using Bracket::Size;
	using Bracket::Append;
	void CopyPayloadFrom(const SelectIteratorsBracket &other) noexcept { haveJoins = other.haveJoins; }
	bool haveJoins = false;
};

class SelectIteratorContainer
	: public ExpressionTree<OpType, SelectIteratorsBracket, 2, SelectIterator, JoinSelectIterator, FieldsComparator, AlwaysFalse> {
	using Base = ExpressionTree<OpType, SelectIteratorsBracket, 2, SelectIterator, JoinSelectIterator, FieldsComparator, AlwaysFalse>;

public:
	SelectIteratorContainer(PayloadType pt = PayloadType(), SelectCtx *ctx = nullptr)
		: pt_(std::move(pt)), ctx_(ctx), maxIterations_(std::numeric_limits<int>::max()), wasZeroIterations_(false) {}

	void SortByCost(int expectedIterations);
	bool HasIdsets() const;
	// Check NOT or comparator must not be 1st
	void CheckFirstQuery();
	// Let iterators choose most effecive algorith
	void SetExpectMaxIterations(int expectedIterations);
	void PrepareIteratorsForSelectLoop(QueryPreprocessor &, unsigned sortId, bool isFt, const NamespaceImpl &, SelectFunction::Ptr &,
									   FtCtx::Ptr &, const RdxContext &);
	template <bool reverse, bool hasComparators>
	bool Process(PayloadValue &, bool *finish, IdType *rowId, IdType, bool match);

	bool IsSelectIterator(size_t i) const noexcept {
		assertrx(i < Size());
		return container_[i].HoldsOrReferTo<SelectIterator>();
	}
	bool IsJoinIterator(size_t i) const noexcept {
		assertrx(i < container_.size());
		return container_[i].HoldsOrReferTo<JoinSelectIterator>();
	}
	void ExplainJSON(int iters, JsonBuilder &builder, const std::vector<JoinedSelector> *js) const {
		explainJSON(cbegin(), cend(), iters, builder, js);
	}

	void Clear() {
		clear();
		maxIterations_ = std::numeric_limits<int>::max();
		wasZeroIterations_ = false;
	}
	int GetMaxIterations(bool withZero = false) { return (withZero && wasZeroIterations_) ? 0 : maxIterations_; }
	std::string Dump() const;
	static bool IsExpectingOrderedResults(const QueryEntry &qe) noexcept {
		return IsOrderedCondition(qe.condition) || (qe.condition != CondAny && qe.values.size() <= 1);
	}

private:
	bool prepareIteratorsForSelectLoop(QueryPreprocessor &, size_t begin, size_t end, unsigned sortId, bool isFt, const NamespaceImpl &,
									   SelectFunction::Ptr &, FtCtx::Ptr &, const RdxContext &);
	void sortByCost(span<unsigned> indexes, span<double> costs, unsigned from, unsigned to, int expectedIterations);
	double fullCost(span<unsigned> indexes, unsigned i, unsigned from, unsigned to, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned cur, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const;
	void moveJoinsToTheBeginingOfORs(span<unsigned> indexes, unsigned from, unsigned to);
	// Check idset must be 1st
	static void checkFirstQuery(Container &);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyCondition(SelectIterator &, PayloadValue &, bool *finish, IdType rowId, IdType properRowId);
	bool checkIfSatisfyCondition(JoinSelectIterator &, PayloadValue &, IdType properRowId, bool match);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue &, bool *finish, IdType rowId, IdType properRowId,
									 bool match);
	static std::string explainJSON(const_iterator it, const_iterator to, int iters, JsonBuilder &builder,
								   const std::vector<JoinedSelector> *);
	template <bool reverse>
	static IdType next(const_iterator, IdType from);
	template <bool reverse>
	static IdType getNextItemId(const_iterator begin, const_iterator end, IdType from);
	static bool isIdset(const_iterator it, const_iterator end);
	static bool markBracketsHavingJoins(iterator begin, iterator end) noexcept;
	bool haveJoins(size_t i) const noexcept;

	SelectKeyResults processQueryEntry(const QueryEntry &qe, const NamespaceImpl &ns, StrictMode strictMode);
	SelectKeyResults processQueryEntry(const QueryEntry &qe, bool enableSortIndexOptimize, const NamespaceImpl &ns, unsigned sortId,
									   bool isQueryFt, SelectFunction::Ptr &selectFnc, bool &isIndexFt, bool &isIndexSparse, FtCtx::Ptr &,
									   QueryPreprocessor &qPreproc, const RdxContext &);
	template <bool left>
	void processField(FieldsComparator &, std::string_view field, int idxNo, const NamespaceImpl &ns) const;
	void processJoinEntry(const JoinQueryEntry &, OpType);
	void processQueryEntryResults(SelectKeyResults &selectResults, OpType, const NamespaceImpl &ns, const QueryEntry &qe, bool isIndexFt,
								  bool isIndexSparse, bool nonIndexField, std::optional<OpType> nextOp);
	struct EqualPositions {
		h_vector<size_t, 4> queryEntriesPositions;
		size_t positionToInsertIterator = 0;
		bool foundOr = false;
	};
	void processEqualPositions(const std::vector<EqualPositions> &equalPositions, const NamespaceImpl &ns, const QueryEntries &queries);
	static std::vector<EqualPositions> prepareEqualPositions(const QueryEntries &queries, size_t begin, size_t end);

	/// @return end() if empty or last opened bracket is empty
	iterator lastAppendedOrClosed() {
		typename Container::iterator it = this->container_.begin(), end = this->container_.end();
		if (!this->activeBrackets_.empty()) it += (this->activeBrackets_.back() + 1);
		if (it == end) return this->end();
		iterator i = it, i2 = it, e = end;
		while (++i2 != e) i = i2;
		return i;
	}
	static void dump(size_t level, const_iterator begin, const_iterator end, const std::vector<JoinedSelector> &, WrSerializer &);

	PayloadType pt_;
	SelectCtx *ctx_;
	int maxIterations_;
	bool wasZeroIterations_;
};

}  // namespace reindexer
