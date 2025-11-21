#include "allocdebug.h"
#include <stdio.h>
#include <stdlib.h>
#include <atomic>
#include "core/type_consts.h"
#include "tools/logger.h"

template <typename counter_t>
class [[nodiscard]] AllocsTracer {
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

#include "tools/alloc_ext/tc_malloc_extension.h"

static void traced_new_mt(const void* ptr, size_t size) {
	if (ptr && size) {
		tracer_mt.traced_new(reindexer::alloc_ext::instance()->GetAllocatedSize(const_cast<void*>(ptr)));
	}
}

static void traced_delete_mt(const void* ptr) {
	if (ptr) {
		tracer_mt.traced_delete(reindexer::alloc_ext::instance()->GetAllocatedSize(const_cast<void*>(ptr)));
	}
}

static void traced_new(const void* ptr, size_t size) {
	if (ptr && size) {
		tracer.traced_new(reindexer::alloc_ext::instance()->GetAllocatedSize(const_cast<void*>(ptr)));
	}
}

static void traced_delete(const void* ptr) {
	if (ptr) {
		tracer.traced_delete(reindexer::alloc_ext::instance()->GetAllocatedSize(const_cast<void*>(ptr)));
	}
}

void allocdebug_init() {
	if (reindexer::alloc_ext::TCMallocIsAvailable() && reindexer::alloc_ext::TCMallocHooksAreAvailable()) {
		std::ignore = reindexer::alloc_ext::MallocHook_AddNewHook(traced_new);
		std::ignore = reindexer::alloc_ext::MallocHook_AddDeleteHook(traced_delete);
		ismt = false;
	} else {
		logFmt(LogWarning,
			   "Reindexer was compiled with GPerf tools, but tcmalloc was not successfully linked. Malloc new hook is unavailable");
	}
}

void allocdebug_init_mt() {
	if (reindexer::alloc_ext::TCMallocIsAvailable() && reindexer::alloc_ext::TCMallocHooksAreAvailable()) {
		std::ignore = reindexer::alloc_ext::MallocHook_AddNewHook(traced_new_mt);
		std::ignore = reindexer::alloc_ext::MallocHook_AddDeleteHook(traced_delete_mt);
		ismt = true;
	} else {
		logFmt(LogWarning,
			   "Reindexer was compiled with GPerf tools, but tcmalloc was not successfully linked. Malloc delete hook is unavailable");
	}
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
	logFmt(LogInfo, "meminfo (alloced {}M, {} total allocs, {} remain)", get_alloc_size() / (1024 * 1024), get_alloc_cnt_total(),
		   get_alloc_cnt());
}
