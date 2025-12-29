#pragma once
#include <cstddef>
#include <cstdint>

namespace hnswlib {

enum class [[nodiscard]] MetricType {
	NONE,
	L2,
	INNER_PRODUCT,
	COSINE,
};

typedef uint32_t tableint;
typedef uint32_t labeltype;
typedef uint32_t linklistsizeint;

static constexpr uint8_t kQuantizeBits = 8;
static constexpr float KDefaultSq8Range = (1 << kQuantizeBits) - 1;
static constexpr size_t kDefaultSampleSize = 20'000;
static constexpr int kSampleBatchSize = 20;

enum class [[nodiscard]] Sq8NonLinearCorrection { Disabled, Enabled };

#if 0
enum class [[nodiscard]] ScalarQuantizeType { Full, Partial, Component };
#endif
}  // namespace hnswlib
