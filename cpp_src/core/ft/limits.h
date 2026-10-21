#pragma once

#include <cstddef>
#include <cstdint>

namespace reindexer {

constexpr uint32_t kMaxMergeLimitValue = 0x1FFFFFF;
constexpr uint32_t kMinMergeLimitValue = 1;
constexpr int kMaxTyposInWord = 2;
constexpr int kMaxTypoLenLimit = 100;
constexpr int kMaxSplitLen = 100;
constexpr size_t kMaxFtWordLen = 256;

}  // namespace reindexer
