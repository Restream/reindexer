#pragma once

#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"

namespace reindexer {
class FloatVectorIndex;

struct [[nodiscard]] FloatVectorIndexData {
	size_t ptField{0};
	const FloatVectorIndex* ptr{nullptr};
};

struct [[nodiscard]] FloatVectorsIndexes : h_vector<FloatVectorIndexData, 2> {};
struct [[nodiscard]] FloatVectorsIndexesValues : h_vector<std::pair<FloatVectorIndexData, h_vector<Variant, 1>>, 2> {};	 // per Item

}  // namespace reindexer
