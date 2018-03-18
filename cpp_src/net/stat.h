#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <chrono>

namespace reindexer {

class Stat {
public:
	Stat();
	~Stat();

	Stat operator-(const Stat &other) const;

	uint64_t GetTimeElapsed() { return time_us_; }
	size_t GetAllocsCnt() { return allocs_cnt_; }
	size_t GetAllocsBytes() { return allocs_bytes_; }

private:
	std::chrono::high_resolution_clock::time_point tmpoint_;

	uint64_t time_us_;
	size_t allocs_cnt_;
	size_t allocs_bytes_;
};
}  // namespace reindexer
