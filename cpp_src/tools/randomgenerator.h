#include <array>
#include <chrono>
#include <random>

namespace reindexer {
namespace tools {

class RandomGenerator {
public:
	static uint16_t getu16(uint16_t from, uint16_t to) { return get<uint16_t>(from, to); }
	static uint32_t getu32(uint32_t from, uint32_t to) { return get<uint32_t>(from, to); }
	static uint64_t getu64(uint64_t from, uint64_t to) { return get<uint64_t>(from, to); }
	static int16_t gets16(int16_t from, int16_t to) { return get<int16_t>(from, to); }
	static int32_t gets32(int32_t from, int32_t to) { return get<int32_t>(from, to); }
	static int64_t gets64(int64_t from, int64_t to) { return get<int64_t>(from, to); }

	template <typename T>
	static T get(T from, T to) {
		thread_local static std::default_random_engine e(std::chrono::steady_clock::now().time_since_epoch().count());
		thread_local static std::uniform_int_distribution<T> uniform_dist(from, to);
		return uniform_dist(e);
	}
};

}  // namespace tools
}  // namespace reindexer
