#include "querytree.h"
#include "core/nsselecter/selectiterator.h"
#include "queryentry.h"

namespace reindexer {

template <typename T, int holdSize>
bool QueryTree<T, holdSize>::Leaf::IsEqual(const Node &other) const {
	const Leaf *otherPtr = dynamic_cast<const Leaf *>(&other);
	return otherPtr && other.Op == this->Op && otherPtr->value_ == value_;
}

template <>
bool QueryTree<SelectIterator, 2>::Leaf::IsEqual(const Node &) const {
	throw std::runtime_error("Cannot compare");
}

template bool QueryTree<QueryEntry, 4>::Leaf::IsEqual(const Node &) const;
}  // namespace reindexer
