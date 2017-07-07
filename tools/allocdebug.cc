#include <stdlib.h>
#include <atomic>
#include "core/type_consts.h"
#include "tools/logger.h"

namespace reindexer {
std::atomic<int32_t> payload_cnt;
}

#if REINDEX_ALLOC_DEBUG

using std::atomic_size_t;
using reindexer::logPrintf;

const size_t heapObjOverhead = 4 * sizeof(void *);

atomic_size_t alloced_sz;
atomic_size_t alloced_cnt;
atomic_size_t alloced_cnt_total;

void *__traced_malloc(size_t count) {
	alloced_cnt.fetch_add(1);
	alloced_cnt_total.fetch_add(1);
	alloced_sz.fetch_add(count + heapObjOverhead);
	size_t *ptr = (size_t *)malloc(count + sizeof(size_t));
	*ptr++ = count;
	return ptr;
}

void __traced_free(void *_ptr) {
	size_t *ptr = (size_t *)_ptr;
	--ptr;
	alloced_cnt.fetch_sub(1);
	alloced_sz.fetch_sub(*ptr + 24);
	free(ptr);
}

void *operator new(std::size_t count) { return __traced_malloc(count); }
void *operator new[](std::size_t count) { return __traced_malloc(count); }
void operator delete(void *ptr) throw() { __traced_free(ptr); }
void operator delete(void *ptr, size_t /*count*/) throw() { __traced_free(ptr); }
void operator delete[](void *ptr) throw() { __traced_free(ptr); }
void operator delete[](void *ptr, std::size_t /*count*/) throw() { __traced_free(ptr); }

void allocdebug_show() {
	logPrintf(LogInfo, "meminfo (alloced %luM, %lu total allocs, %lu remain,ploads=%lu)", alloced_sz.load() / (1024 * 1024),
			  alloced_cnt_total.load(), alloced_cnt.load(), reindexer::payload_cnt.load());
}
size_t get_alloc_size() { return alloced_sz.load(); }
size_t get_alloc_cnt() { return alloced_cnt.load(); }
size_t get_alloc_cnt_total() { return alloced_cnt_total.load(); }

#else
void allocdebug_show() {}
size_t get_alloc_size() { return -1; }
size_t get_alloc_cnt() { return -1; }
size_t get_alloc_cnt_total() { return -1; }

#endif
