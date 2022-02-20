#pragma once

#include <array>
#include <chrono>
#include <random>

namespace reindexer {
namespace tools {

class RandomGenerator {
public:
	static uint16_t getu16(uint16_t from = std::numeric_limits<uint16_t>::min(), uint16_t to = std::numeric_limits<uint16_t>::max()) {
		return get<uint16_t>(from, to);
	}
	static uint32_t getu32(uint32_t from = std::numeric_limits<uint32_t>::min(), uint32_t to = std::numeric_limits<uint32_t>::max()) {
		return get<uint32_t>(from, to);
	}
	static uint64_t getu64(uint64_t from = std::numeric_limits<uint64_t>::min(), uint64_t to = std::numeric_limits<uint64_t>::max()) {
		return get<uint64_t>(from, to);
	}
	static int16_t gets16(int16_t from = std::numeric_limits<int16_t>::min(), int16_t to = std::numeric_limits<int16_t>::max()) {
		return get<int16_t>(from, to);
	}
	static int32_t gets32(int32_t from = std::numeric_limits<int32_t>::min(), int32_t to = std::numeric_limits<int32_t>::max()) {
		return get<int32_t>(from, to);
	}
	static int64_t gets64(int64_t from = std::numeric_limits<int64_t>::min(), int64_t to = std::numeric_limits<int64_t>::max()) {
		return get<int64_t>(from, to);
	}

	template <typename T>
	static T get(T from, T to) {
		thread_local static std::default_random_engine e(std::chrono::steady_clock::now().time_since_epoch().count());
		thread_local static std::uniform_int_distribution<T> uniform_dist(from, to);
		return uniform_dist(e);
	}
};

}  // namespace tools
}  // namespace reindexer
