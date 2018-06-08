#include <stdio.h>
#include <stdlib.h>
#include <atomic>
#include "core/type_consts.h"
#include "tools/logger.h"

template <typename counter_t>
class AllocsTracer {
public:
	counter_t alloced_sz;
	counter_t alloced_cnt;
	counter_t alloced_cnt_total;
	counter_t alloced_sz_total;
	void traced_new(size_t size) {
		alloced_cnt++;
		alloced_cnt_total++;
		alloced_sz += size;
		alloced_sz_total += size;
	}
	void traced_delete(size_t size) {
		alloced_sz -= size;
		alloced_cnt--;
	}
};

static thread_local AllocsTracer<size_t> tracer;
static AllocsTracer<std::atomic_size_t> tracer_mt;
static bool ismt;

#ifdef REINDEX_WITH_GPERFTOOLS

#include "gperftools/malloc_hook_c.h"
#include "gperftools/tcmalloc.h"

static void traced_new_mt(const void *ptr, size_t size) {
	if (ptr && size) tracer_mt.traced_new(tc_malloc_size(const_cast<void *>(ptr)));
}

static void traced_delete_mt(const void *ptr) {
	if (ptr) tracer_mt.traced_delete(tc_malloc_size(const_cast<void *>(ptr)));
}

static void traced_new(const void *ptr, size_t size) {
	if (ptr && size) tracer.traced_new(tc_malloc_size(const_cast<void *>(ptr)));
}

static void traced_delete(const void *ptr) {
	if (ptr) tracer.traced_delete(tc_malloc_size(const_cast<void *>(ptr)));
}

void allocdebug_init() {
	MallocHook_AddNewHook(traced_new);
	MallocHook_AddDeleteHook(traced_delete);
	ismt = false;
}

void allocdebug_init_mt() {
	MallocHook_AddNewHook(traced_new_mt);
	MallocHook_AddDeleteHook(traced_delete_mt);
	ismt = true;
}

#else
void allocdebug_init() {}
void allocdebug_init_mt() {}
#endif

size_t get_alloc_size() { return ismt ? tracer_mt.alloced_sz.load() : tracer.alloced_sz; }
size_t get_alloc_cnt() { return ismt ? tracer_mt.alloced_cnt.load() : tracer.alloced_cnt; }
size_t get_alloc_size_total() { return ismt ? tracer_mt.alloced_sz_total.load() : tracer.alloced_sz_total; }
size_t get_alloc_cnt_total() { return ismt ? tracer_mt.alloced_cnt_total.load() : tracer.alloced_cnt_total; }

void allocdebug_show() {
	reindexer::logPrintf(LogInfo, "meminfo (alloced %dM, %d total allocs, %d remain)", int(get_alloc_size() / (1024 * 1024)),
						 int(get_alloc_cnt_total()), int(get_alloc_cnt()));
}
