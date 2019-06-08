#pragma once
#include "core/nsselecter/selectiterator.h"
#include "core/query/querytree.h"
#include "core/selectfunc/ctx/ftctx.h"
#include "core/selectfunc/selectfunc.h"

namespace reindexer {

template <>
bool QueryTree<SelectIterator, 2>::Leaf::IsEqual(const Node &) const;

class SelectIteratorContainer : public QueryTree<SelectIterator, 2> {
public:
	void ForeachIterator(const std::function<void(const SelectIterator &, OpType)> &func) const { ForeachValue(func); }
	void ForeachIterator(const std::function<void(SelectIterator &)> &func) { ForeachValue(func); }

	void SortByCost(int expectedIterations) { sortByCost(begin(), end(), expectedIterations); }
	bool HasIdsets() const;
	// Check NOT or comparator must not be 1st
	void CheckFirstQuery();
	// Let iterators choose most effecive algorith
	void SetExpectMaxIterations(int expectedIterations);
	void PrepareIteratorsForSelectLoop(const QueryEntries &, size_t queriesBegin, size_t queriesEnd,
									   const std::multimap<unsigned, EqualPosition> &equalPositions, unsigned sortId, bool isFt,
									   const Namespace &, SelectFunction::Ptr selectFnc, FtCtx::Ptr &ftCtx);
	template <bool reverse, bool hasComparators>
	bool Process(PayloadValue &, bool *finish, IdType *rowId, IdType properRowId);

	bool IsIterator(size_t i) const { return IsValue(i); }
	void ExplainJSON(int iters, JsonBuilder &builder) const { explainJSON(cbegin(), cend(), iters, builder); }

private:
	void sortByCost(iterator from, iterator to, int expectedIterations);
	double fullCost(const_iterator it, const_iterator begin, const_iterator end, int expectedIterations) const;
	double cost(const_iterator, int expectedIterations) const;
	double cost(const_iterator from, const_iterator to, int expectedIterations) const;
	// Check idset must be 1st
	static void checkFirstQuery(Container &);
	template <bool reverse, bool hasComparators>
	static bool find(SelectIterator &, PayloadValue &, bool *finish, IdType rowId, IdType properRowId);
	template <bool reverse, bool hasComparators>
	static bool find(iterator begin, iterator end, PayloadValue &, bool *finish, IdType rowId, IdType properRowId);
	static void explainJSON(const_iterator it, const_iterator to, int iters, JsonBuilder &builder);
	template <bool reverse>
	static IdType next(const_iterator, IdType from);
	template <bool reverse>
	static IdType iterate(const_iterator begin, const_iterator end, IdType from);
	static bool isIdset(const_iterator it, const_iterator end);
};

extern template bool SelectIteratorContainer::Process<false, false>(PayloadValue &, bool *, IdType *, IdType);
extern template bool SelectIteratorContainer::Process<false, true>(PayloadValue &, bool *, IdType *, IdType);
extern template bool SelectIteratorContainer::Process<true, false>(PayloadValue &, bool *, IdType *, IdType);
extern template bool SelectIteratorContainer::Process<true, true>(PayloadValue &, bool *, IdType *, IdType);

}  // namespace reindexer
