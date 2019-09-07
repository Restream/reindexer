#pragma once

#include <stdint.h>
#include <stdlib.h>
#include <atomic>
#include <chrono>

namespace reindexer {

class HandlerStat {
public:
	using ClockT = std::chrono::high_resolution_clock;

	HandlerStat();
	~HandlerStat();

	HandlerStat operator-(const HandlerStat& other) const;

	uint64_t GetTimeElapsed() const { return time_us_; }
	size_t GetAllocsCnt() const { return allocs_cnt_; }
	size_t GetAllocsBytes() const { return allocs_bytes_; }

private:
	ClockT::time_point tmpoint_;

	uint64_t time_us_;
	size_t allocs_cnt_;
	size_t allocs_bytes_;
};

struct SizeStat {
	size_t reqSizeBytes{0};
	size_t respSizeBytes{0};
};

struct Stat {
	HandlerStat allocStat;
	SizeStat sizeStat;
};

class TrafficStat {
public:
	uint64_t GetReadBytes() const { return read_bytes_; }
	uint64_t GetWrittenBytes() const { return written_bytes_; }
	void AppendReadBytes(uint64_t read_bytes) { read_bytes_ += read_bytes; }
	void AppendWrittenBytes(uint64_t written_bytes) { written_bytes_ += written_bytes; }
	void Reset() {
		read_bytes_ = 0;
		written_bytes_ = 0;
	}

private:
	uint64_t read_bytes_{0};
	uint64_t written_bytes_{0};
};

}  // namespace reindexer
