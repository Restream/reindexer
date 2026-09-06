#pragma once

#include <memory>
#include <random>
#include "tools/clock.h"

namespace reindexer {
namespace tools {

class [[nodiscard]] RandomGenerator {
public:
	static uint16_t getu16(uint16_t from = std::numeric_limits<uint16_t>::min(), uint16_t to = std::numeric_limits<uint16_t>::max()) {
		return get32<uint16_t>(from, to);
	}
	static uint32_t getu32(uint32_t from = std::numeric_limits<uint32_t>::min(), uint32_t to = std::numeric_limits<uint32_t>::max()) {
		return get32<uint32_t>(from, to);
	}
	static uint64_t getu64(uint64_t from = std::numeric_limits<uint64_t>::min(), uint64_t to = std::numeric_limits<uint64_t>::max()) {
		return get64<uint64_t>(from, to);
	}
	static int16_t gets16(int16_t from = std::numeric_limits<int16_t>::min(), int16_t to = std::numeric_limits<int16_t>::max()) {
		return get32<int16_t>(from, to);
	}
	static int32_t gets32(int32_t from = std::numeric_limits<int32_t>::min(), int32_t to = std::numeric_limits<int32_t>::max()) {
		return get32<int32_t>(from, to);
	}
	static int64_t gets64(int64_t from = std::numeric_limits<int64_t>::min(), int64_t to = std::numeric_limits<int64_t>::max()) {
		return get64<int64_t>(from, to);
	}

	template <typename T>
	static T get32(T from, T to) {
		return get<T, std::mt19937>(from, to);
	}
	template <typename T>
	static T get64(T from, T to) {
		return get<T, std::mt19937_64>(from, to);
	}

private:
	template <typename T, typename GenT>
	static T get(T from, T to) {
		thread_local static std::unique_ptr<GenT> e;
		if (!e) {
			// Using unique ptr here to avoid initialization on the thread creaion.
			// Slower first call is acceptable for tagsmatcher, ns names and RAFT randomization.
			e = std::make_unique<GenT>(steady_clock_w::now().time_since_epoch().count());
		}
		return std::uniform_int_distribution<T>(from, to)(*e);
	}
};

}  // namespace tools
}  // namespace reindexer
