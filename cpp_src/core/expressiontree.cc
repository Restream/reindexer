#include "expressiontree.h"
#include "core/nsselecter/selectiterator.h"
#include "core/nsselecter/sortexpression.h"
#include "core/query/queryentry.h"

namespace reindexer {

template <typename T, typename OperationType, int holdSize>
bool ExpressionTree<T, OperationType, holdSize>::Leaf::IsEqual(const Node &other) const {
	const Leaf *otherPtr = dynamic_cast<const Leaf *>(&other);
	return otherPtr && other.Op == this->Op && otherPtr->value_ == value_;
}

template <>
bool ExpressionTree<SelectIterator, OpType, 2>::Leaf::IsEqual(const Node &) const {
	throw std::runtime_error("Cannot compare");
}

template bool ExpressionTree<QueryEntry, OpType, 4>::Leaf::IsEqual(const Node &) const;
template bool ExpressionTree<SortExpressionValue, SortExpressionOperation, 2>::Leaf::IsEqual(const Node &) const;
}  // namespace reindexer
