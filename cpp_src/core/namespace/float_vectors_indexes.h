#pragma once

#include "estl/h_vector.h"

namespace reindexer {

class FloatVectorIndex;

struct [[nodiscard]] FloatVectorIndexData {
	size_t ptField{0};
	const FloatVectorIndex* ptr{nullptr};
};

struct [[nodiscard]] FloatVectorsIndexes : h_vector<FloatVectorIndexData, 2> {};

}  // namespace reindexer
