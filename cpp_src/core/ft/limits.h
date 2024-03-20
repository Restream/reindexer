#pragma once

#include <cstdint>

namespace reindexer {

constexpr uint32_t kWordIdEmptyIdVal = 0x3FFFFFF;
constexpr uint32_t kWordIdMaxIdVal = kWordIdEmptyIdVal - 1;
constexpr uint32_t kWordIdMaxStepVal = 0x3F;
constexpr uint32_t kMaxStepsCount = kWordIdMaxStepVal + 1;
constexpr int kMaxMergeLimitValue = 65000;
constexpr int kMinMergeLimitValue = 0;
constexpr int kMaxTyposInWord = 2;
constexpr int kMaxTypoLenLimit = 100;

}  // namespace reindexer
