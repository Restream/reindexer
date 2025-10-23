#include "selectiterator.h"

namespace reindexer {

std::string_view SelectIterator::TypeName() const noexcept {
	using namespace std::string_view_literals;
	switch (type_) {
		case Forward:
			return "Forward"sv;
		case Reverse:
			return "Reverse"sv;
		case SingleRange:
			return "SingleRange"sv;
		case SingleIdset:
			return "SingleIdset"sv;
		case SingleIdSetWithDeferedSort:
			return "SingleIdSetWithDeferedSort"sv;
		case RevSingleRange:
			return "RevSingleRange"sv;
		case RevSingleIdset:
			return "RevSingleIdset"sv;
		case RevSingleIdSetWithDeferedSort:
			return "RevSingleIdSetWithDeferedSort"sv;
		case Unsorted:
			return "Unsorted"sv;
		case UnbuiltSortOrdersIndex:
			return "UnbuiltSortOrdersIndex"sv;
		default:
			return "<unknown>"sv;
	}
}

std::string SelectIterator::Dump() const {
	std::string ret = name + ' ' + std::string(TypeName()) + "(";

	for (auto& it : *this) {
		if (it.useBtree_) {
			ret += "btree;";
		}
		if (it.isRange_) {
			ret += "range;";
		}
		if (it.bsearch_) {
			ret += "bsearch;";
		}
		ret += ",";
		if (ret.length() > 256) {
			ret += "...";
			break;
		}
	}
	ret += ")";
	return ret;
}

}  // namespace reindexer
