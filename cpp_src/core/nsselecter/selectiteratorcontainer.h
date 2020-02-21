#pragma once
#include "core/expressiontree.h"
#include "core/nsselecter/selectiterator.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfunc.h"

namespace reindexer {

struct SelectCtx;
class RdxContext;

class SelectIteratorContainer : public ExpressionTree<OpType, Bracket, 2, SelectIterator> {
public:
	SelectIteratorContainer(PayloadType pt = PayloadType(), SelectCtx *ctx = nullptr) : pt_(pt), ctx_(ctx){};

	void ForEachIterator(const std::function<void(const SelectIterator &)> &func) const { ExecuteAppropriateForEach(func); }
	void ForEachIterator(const std::function<void(SelectIterator &)> &func) { ExecuteAppropriateForEach(func); }
	const SelectIterator &operator[](size_t i) const {
		assert(i < container_.size());
		return container_[i].Value();
	}
	SelectIterator &operator[](size_t i) {
		assert(i < container_.size());
		return container_[i].Value();
	}

	void SortByCost(int expectedIterations);
	bool HasIdsets() const;
	// Check NOT or comparator must not be 1st
	void CheckFirstQuery();
	// Let iterators choose most effecive algorith
	void SetExpectMaxIterations(int expectedIterations);
	void PrepareIteratorsForSelectLoop(const QueryEntries &, size_t queriesBegin, size_t queriesEnd,
									   const std::multimap<unsigned, EqualPosition> &equalPositions, unsigned sortId, bool isFt,
									   const NamespaceImpl &, SelectFunction::Ptr selectFnc, FtCtx::Ptr &ftCtx, const RdxContext &);
	template <bool reverse, bool hasComparators>
	bool Process(PayloadValue &, bool *finish, IdType *rowId, IdType, bool match);

	bool IsIterator(size_t i) const { return IsValue(i); }
	void ExplainJSON(int iters, JsonBuilder &builder) const { explainJSON(cbegin(), cend(), iters, builder); }

private:
	void sortByCost(span<unsigned> indexes, span<double> costs, unsigned from, unsigned to, int expectedIterations);
	double fullCost(span<unsigned> indexes, unsigned i, unsigned from, unsigned to, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned cur, int expectedIterations) const;
	double cost(span<unsigned> indexes, unsigned from, unsigned to, int expectedIterations) const;
	void moveJoinsToTheBeginingOfORs(span<unsigned> indexes, unsigned from, unsigned to);
	// Check idset must be 1st
	static void checkFirstQuery(Container &);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyCondition(SelectIterator &, PayloadValue &, bool *finish, IdType rowId, IdType properRowId, bool match);
	template <bool reverse, bool hasComparators>
	bool checkIfSatisfyAllConditions(iterator begin, iterator end, PayloadValue &, bool *finish, IdType rowId, IdType properRowId,
									 bool match);
	static void explainJSON(const_iterator it, const_iterator to, int iters, JsonBuilder &builder);
	template <bool reverse>
	static IdType next(const_iterator, IdType from);
	template <bool reverse>
	static IdType getNextItemId(const_iterator begin, const_iterator end, IdType from);
	static bool isIdset(const_iterator it, const_iterator end);

	SelectKeyResults processQueryEntry(const QueryEntry &qe, const NamespaceImpl &ns);
	SelectKeyResults processQueryEntry(const QueryEntry &qe, bool enableSortIndexOptimize, const NamespaceImpl &ns, unsigned sortId,
									   bool isQueryFt, SelectFunction::Ptr selectFnc, bool &isIndexFt, bool &isIndexSparse, FtCtx::Ptr &,
									   const RdxContext &);
	void processJoinEntry(const QueryEntry &qe, OpType op);
	void processQueryEntryResults(SelectKeyResults &selectResults, OpType, const NamespaceImpl &ns, const QueryEntry &qe, bool isIndexFt,
								  bool isIndexSparse, bool nonIndexField);
	void processEqualPositions(const std::multimap<unsigned, EqualPosition> &equalPositions, size_t begin, size_t end, const NamespaceImpl &ns,
							   const QueryEntries &queries);
	bool processJoins(SelectIterator &it, const ConstPayload &pl, IdType properRowId, bool match);

	/// @return end() if empty or last opened bracket is empty
	iterator lastAppendedOrClosed() {
		typename Container::iterator it = this->container_.begin(), end = this->container_.end();
		if (!this->activeBrackets_.empty()) it += (this->activeBrackets_.back() + 1);
		if (it == end) return this->end();
		iterator i = it, e = end;
		while (i + 1 != e) ++i;
		return i;
	}

	PayloadType pt_;
	SelectCtx *ctx_;
};

}  // namespace reindexer
