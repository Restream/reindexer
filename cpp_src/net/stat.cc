#include "stat.h"
#include <chrono>
#include "debug/allocdebug.h"

namespace reindexer {

HandlerStat::HandlerStat() : tmpoint_(ClockT::now()), time_us_(0), allocs_cnt_(get_alloc_cnt_total()), allocs_bytes_(get_alloc_size_total()) {}

HandlerStat::~HandlerStat() {}

HandlerStat HandlerStat::operator-(const HandlerStat &other) const {
	HandlerStat res = *this;

	res.time_us_ = std::chrono::duration_cast<std::chrono::microseconds>(res.tmpoint_ - other.tmpoint_).count();
	res.allocs_cnt_ -= other.allocs_cnt_;
	res.allocs_bytes_ -= other.allocs_bytes_;

	return res;
}

}  // namespace reindexer
