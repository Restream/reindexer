#include "types.h"

#include <iostream>
#include <vector>
#include "core/key_value_type.h"

namespace fuzzing {

std::ostream& operator<<(std::ostream& os, FieldType ft) {
	switch (ft) {
		case FieldType::Bool:
			return os << "bool";
		case FieldType::Int:
			return os << "int";
		case FieldType::Int64:
			return os << "int64";
		case FieldType::Double:
			return os << "double";
		case FieldType::String:
			return os << "string";
		case FieldType::Uuid:
			return os << "uuid";
		case FieldType::Point:
			return os << "point";
		case FieldType::Struct:
			return os << "struct";
		default:
			assertrx(0);
	}
	return os;
}

reindexer::KeyValueType ToKeyValueType(FieldType ft) {
	switch (ft) {
		case FieldType::Bool:
			return reindexer::KeyValueType::Bool{};
		case FieldType::Int:
			return reindexer::KeyValueType::Int{};
		case FieldType::Int64:
			return reindexer::KeyValueType::Int64{};
		case FieldType::Double:
			return reindexer::KeyValueType::Double{};
		case FieldType::String:
			return reindexer::KeyValueType::String{};
		case FieldType::Uuid:
			return reindexer::KeyValueType::Uuid{};
		case FieldType::Point:
			return reindexer::KeyValueType::Undefined{};  // TODO change to KeyValueType::Point #1352
		case FieldType::Struct:
		default:
			assertrx(0);
			std::abort();
	}
}

std::ostream& operator<<(std::ostream& os, const FieldPath& fp) {
	os << '[';
	for (size_t i = 0, s = fp.size(); i < s; ++i) {
		if (i != 0) {
			os << ' ';
		}
		os << fp[i];
	}
	return os << ']' << std::endl;
}

std::string_view ToText(IndexType it) {
	using namespace std::string_view_literals;
	switch (it) {
		case IndexType::Store:
			return "-"sv;
		case IndexType::Hash:
			return "hash"sv;
		case IndexType::Tree:
			return "tree"sv;
		case IndexType::Ttl:
			return "ttl"sv;
		case IndexType::FastFT:
			return "text"sv;
		case IndexType::FuzzyFT:
			return "fuzzytext"sv;
		case IndexType::RTree:
			return "rtree"sv;
		default:
			assertrx(0);
			std::abort();
	}
}

std::ostream& operator<<(std::ostream& os, IndexType it) { return os << ToText(it); }

}  // namespace fuzzing
