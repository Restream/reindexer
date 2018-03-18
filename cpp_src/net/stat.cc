#include "stat.h"
#include <chrono>
#include "debug/allocdebug.h"

namespace reindexer {

Stat::Stat() {
	tmpoint_ = std::chrono::high_resolution_clock::now();
	time_us_ = 0;
	allocs_cnt_ = get_alloc_cnt_total();
	allocs_bytes_ = get_alloc_size_total();
}

Stat::~Stat() {}

Stat Stat::operator-(const Stat &other) const {
	Stat res = *this;

	res.time_us_ = std::chrono::duration_cast<std::chrono::microseconds>(res.tmpoint_ - other.tmpoint_).count();
	res.allocs_cnt_ -= other.allocs_cnt_;
	res.allocs_bytes_ -= other.allocs_bytes_;

	return res;
}

}  // namespace reindexer
