#include <benchmark/benchmark.h>

int main() {
	[[maybe_unused]] auto p = reinterpret_cast<void*>(&benchmark::MaybeReenterWithoutASLR);
	return 0;
}
