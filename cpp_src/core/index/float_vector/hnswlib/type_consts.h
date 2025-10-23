#pragma once
#include <cstdint>

namespace hnswlib {

enum class [[nodiscard]] MetricType {
	NONE,
	L2,
	INNER_PRODUCT,
	COSINE,
};

static constexpr uint8_t kQuantizeBits = 8;
static constexpr float KDefaultSq8Range = (1 << kQuantizeBits) - 1;

static constexpr int kSampleBatchSize = 20;

enum class [[nodiscard]] Sq8NonLinearCorrection { Disabled, Enabled };

struct [[nodiscard]] CorrectiveOffsets {
	float precalc;
	float roundingErr;
	float negOutlierErr;
	float posOutlierErr;
};

#if 0
enum class [[nodiscard]] ScalarQuantizeType { Full, Partial, Component };
#endif
}  // namespace hnswlib
