#pragma once

#include <iosfwd>
#include <string_view>
#include <vector>

namespace reindexer {

class KeyValueType;

}  // namespace reindexer

namespace fuzzing {

enum class FieldType { Bool, Int, Int64, Double, String, Uuid, Point, Struct, END = Struct };
reindexer::KeyValueType ToKeyValueType(FieldType);
std::ostream& operator<<(std::ostream&, FieldType);

using FieldPath = std::vector<size_t>;
std::ostream& operator<<(std::ostream&, const FieldPath&);

enum class IndexType { Store, Hash, Tree, Ttl, FastFT, FuzzyFT, RTree, END = RTree };
std::string_view ToText(IndexType);
std::ostream& operator<<(std::ostream&, IndexType);

enum class IsArray : bool { Yes = true, No = false };
enum class IsSparse : bool { Yes = true, No = false };

}  // namespace fuzzing
