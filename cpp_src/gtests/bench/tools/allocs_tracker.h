#pragma once

#include <benchmark/benchmark.h>

#include "debug/allocdebug.h"
#include "debug/backtrace.h"

namespace benchmark {

struct AllocsTracker {
	AllocsTracker(State& state) : total_sz(get_alloc_size_total()), total_cnt(get_alloc_cnt_total()), state(state) { init(); }
	~AllocsTracker() {
		state.counters.insert({{"Bytes/Op", (get_alloc_size_total() - total_sz) / state.iterations()},
							   {"Allocs/Op", (get_alloc_cnt_total() - total_cnt) / state.iterations()}});
	}

protected:
	size_t total_sz, total_cnt;
	State& state;

private:
	void init() {
		static bool inited = false;
		if (!inited) {
			allocdebug_init();
			backtrace_init();
			inited = true;
		}
	}
};
}  // namespace benchmark
