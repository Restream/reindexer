#include <stdio.h>
#include <stdlib.h>
#include <atomic>
#include "core/type_consts.h"
#include "tools/logger.h"

#ifdef REINDEX_WITH_GPERFTOOLS

#include "gperftools/malloc_hook_c.h"
#include "gperftools/tcmalloc.h"

using std::atomic_size_t;
using reindexer::logPrintf;

atomic_size_t alloced_sz;
atomic_size_t alloced_cnt;
atomic_size_t alloced_cnt_total;
atomic_size_t alloced_sz_total;

size_t (*ptc_malloc_size)(void *) = nullptr;

static void traced_new(const void *ptr, size_t size) {
	if (ptr && size) {
		alloced_cnt.fetch_add(1);
		alloced_cnt_total.fetch_add(1);
		alloced_sz.fetch_add(tc_malloc_size(const_cast<void *>(ptr)));
		alloced_sz_total.fetch_add(tc_malloc_size(const_cast<void *>(ptr)));
	}
}

static void traced_delete(const void *ptr) {
	if (ptr) {
		alloced_sz.fetch_sub(tc_malloc_size(const_cast<void *>(ptr)));
		alloced_cnt.fetch_sub(1);
	}
}
void allocdebug_show() {
	logPrintf(LogInfo, "meminfo (alloced %luM, %lu total allocs, %lu remain)", alloced_sz.load() / (1024 * 1024), alloced_cnt_total.load(),
			  alloced_cnt.load());
}

size_t get_alloc_size() { return alloced_sz.load(); }
size_t get_alloc_cnt() { return alloced_cnt.load(); }
size_t get_alloc_size_total() { return alloced_sz_total.load(); }
size_t get_alloc_cnt_total() { return alloced_cnt_total.load(); }

void allocdebug_init() {
	MallocHook_AddNewHook(traced_new);
	MallocHook_AddDeleteHook(traced_delete);
}
#else
void allocdebug_init() {}
void allocdebug_show() {}
size_t get_alloc_size() { return -1; }
size_t get_alloc_size_total() { return -1; }
size_t get_alloc_cnt() { return -1; }
size_t get_alloc_cnt_total() { return -1; }

#endif
