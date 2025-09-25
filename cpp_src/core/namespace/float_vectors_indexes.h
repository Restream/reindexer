#pragma once

#include "estl/h_vector.h"

namespace reindexer {

class FloatVectorIndex;

struct [[nodiscard]] FloatVectorIndexData {
	size_t ptField;
	const FloatVectorIndex* ptr;
};

struct [[nodiscard]] FloatVectorsIndexes : h_vector<FloatVectorIndexData, 2> {};

}  // namespace reindexer
