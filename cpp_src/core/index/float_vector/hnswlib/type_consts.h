#pragma once
#include <cstdint>

namespace hnswlib {

enum class [[nodiscard]] MetricType {
	NONE,
	L2,
	INNER_PRODUCT,
	COSINE,
};

using CorrectiveOffset = float;

using tableint = uint32_t;
using labeltype = uint32_t;
using linklistsizeint = uint32_t;

static constexpr uint8_t kQuantizeBits = 8;
static constexpr float kSq8Range = (1 << kQuantizeBits) - 1;
static constexpr int kSampleBatchSize = 20;

}  // namespace hnswlib
