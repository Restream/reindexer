#pragma once

#include <stdint.h>
#include <stdlib.h>
#include "tools/clock.h"

namespace reindexer {

class [[nodiscard]] HandlerStat {
public:
	using ClockT = system_clock_w;

	HandlerStat() noexcept : tmpoint_(ClockT::now()), time_us_(0), allocs_cnt_(0), allocs_bytes_(0) {}

	HandlerStat operator-(const HandlerStat& other) const noexcept {
		HandlerStat res = *this;
		res.time_us_ = std::chrono::duration_cast<std::chrono::microseconds>(res.tmpoint_ - other.tmpoint_).count();
		res.allocs_cnt_ -= other.allocs_cnt_;
		res.allocs_bytes_ -= other.allocs_bytes_;
		return res;
	}

	uint64_t GetTimeElapsed() const noexcept { return time_us_; }
	size_t GetAllocsCnt() const noexcept { return allocs_cnt_; }
	size_t GetAllocsBytes() const noexcept { return allocs_bytes_; }

private:
	ClockT::time_point tmpoint_;

	uint64_t time_us_;
	size_t allocs_cnt_;
	size_t allocs_bytes_;
};

struct [[nodiscard]] SizeStat {
	size_t reqSizeBytes{0};
	size_t respSizeBytes{0};
};

struct [[nodiscard]] Stat {
	HandlerStat allocStat;
	SizeStat sizeStat;
};

class [[nodiscard]] TrafficStat {
public:
	uint64_t GetReadBytes() const noexcept { return read_bytes_; }
	uint64_t GetWrittenBytes() const noexcept { return written_bytes_; }
	void AppendReadBytes(uint64_t read_bytes) noexcept { read_bytes_ += read_bytes; }
	void AppendWrittenBytes(uint64_t written_bytes) noexcept { written_bytes_ += written_bytes; }
	void Reset() noexcept {
		read_bytes_ = 0;
		written_bytes_ = 0;
	}

private:
	uint64_t read_bytes_{0};
	uint64_t written_bytes_{0};
};

}  // namespace reindexer
