#include "expressiontree.h"
#include "core/nsselecter/selectiterator.h"

namespace reindexer {

template <>
bool ExpressionTree<OpType, Bracket, 2, SelectIterator>::Node::operator==(const Node&) const {
	throw std::runtime_error("Cannot compare");
}

}  // namespace reindexer
