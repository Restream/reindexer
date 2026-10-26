#pragma once

#include <chrono>

namespace reindexer {

struct [[nodiscard]] EmbedderCircuitBreakerDefaults {
	static constexpr size_t kThreshold = 16;
	static constexpr size_t kThresholdTimeoutMs = 15'000;
	static constexpr size_t kCooldownMs = 7'500;
	static constexpr std::chrono::milliseconds kThresholdTimeout{kThresholdTimeoutMs};
	static constexpr std::chrono::milliseconds kCooldown{kCooldownMs};
};

}  // namespace reindexer
