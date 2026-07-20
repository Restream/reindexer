#include "selectiterator.h"
#include "knn_streaming_index_iterator.h"

namespace reindexer {

StreamingKnnIndexIterator* SelectIterator::TryGetKnnStreamingIterator() const noexcept {
	if (GetType() != Type::UnbuiltSortOrdersIndex || empty()) {
		return nullptr;
	}
	const auto& res = operator[](0);
	if (res.collectionType_ != SingleSelectKeyResult::Collection::SingleIterator) {
		return nullptr;
	}
	return dynamic_cast<StreamingKnnIndexIterator*>(res.idxFwdIter_.get());
}

std::string_view SelectIterator::TypeName() const noexcept {
	using namespace std::string_view_literals;
	switch (type_) {
		case Type::None:
			return "None"sv;
		case Type::Forward:
			return "Forward"sv;
		case Type::Reverse:
			return "Reverse"sv;
		case Type::SingleRange:
			return "SingleRange"sv;
		case Type::SingleIdset:
			return "SingleIdset"sv;
		case Type::SingleIdSetWithDeferedSort:
			return "SingleIdSetWithDeferedSort"sv;
		case Type::RevSingleRange:
			return "RevSingleRange"sv;
		case Type::RevSingleIdset:
			return "RevSingleIdset"sv;
		case Type::RevSingleIdSetWithDeferedSort:
			return "RevSingleIdSetWithDeferedSort"sv;
		case Type::Unsorted:
			return "Unsorted"sv;
		case Type::UnbuiltSortOrdersIndex:
			return "UnbuiltSortOrdersIndex"sv;
		default:
			return "<unknown>"sv;
	}
}

std::string SelectIterator::Dump() const {
	std::string ret = name + ' ' + std::string(TypeName()) + "(";

	for (auto& it : *this) {
		switch (it.collectionType_) {
			case SingleSelectKeyResult::Collection::NotSet:
				break;
			case SingleSelectKeyResult::Collection::FlatIdSet:
				break;
			case SingleSelectKeyResult::Collection::TreeIdSet:
				ret += "btree;";
				break;
			case SingleSelectKeyResult::Collection::Range:
				ret += "range;";
				break;
			case SingleSelectKeyResult::Collection::SingleIterator:
				ret += "unbuilt idx;";
				break;
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
