#pragma once

#include <thread>

namespace reindexer {

// Wrapper to handle situation, when std::thread::hardware_concurrency returns 0.
inline unsigned hardware_concurrency() noexcept { return std::max(std::thread::hardware_concurrency(), 1u); }

}  // namespace reindexer
