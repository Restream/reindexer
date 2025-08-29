#pragma once

#include <benchmark/benchmark.h>

#include "debug/allocdebug.h"
#include "debug/backtrace.h"

namespace benchmark {

struct [[nodiscard]] AllocsTracker {
	enum [[nodiscard]] PrintOpts { kNoPrint = 1 << 0, kPrintAllocs = 1 << 1, kPrintHold = 1 << 2 };

	AllocsTracker(State& state, uint8_t printFlags = kPrintAllocs)
		: total_sz(get_alloc_size_total()),
		  total_cnt(get_alloc_cnt_total()),
		  held_mem(get_alloc_size()),
		  held_allocs(get_alloc_cnt()),
		  state(state),
		  flags(printFlags) {
		init();
	}
	~AllocsTracker() {
		auto all_bytes = get_alloc_size_total() - total_sz;
		auto all_allocs = get_alloc_cnt_total() - total_cnt;

		if (flags & kPrintAllocs) {
			state.counters["Bytes/Op"] = all_bytes / state.iterations();
			state.counters["Allocs/Op"] = all_allocs / state.iterations();
		}

		if (flags & kPrintHold) {
			int64_t diff = get_alloc_cnt() - held_allocs;
			state.counters["HeldMem/Op"] = (get_alloc_size() - held_mem) / state.iterations();
			state.counters["HeldA/Op"] = (diff < 0 ? 0 : diff) / state.iterations();
		}
	}

	size_t GetCurrentMemoryConsumption() { return get_alloc_size_total() - total_sz; }
	size_t GetCurrentAllocCount() { return get_alloc_cnt_total() - total_cnt; }

protected:
	size_t total_sz, total_cnt;
	size_t held_mem, held_allocs;
	State& state;
	uint8_t flags;

private:
	void init() {
		static bool inited = false;
		if (!inited) {
			allocdebug_init_mt();
			reindexer::debug::backtrace_init();
			inited = true;
		}
	}
};

}  // namespace benchmark
