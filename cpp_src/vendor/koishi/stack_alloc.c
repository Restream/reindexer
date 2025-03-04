
#include <koishi.h>
#include "stack_alloc.h"

#include <stdlib.h>
#include <assert.h>

#if defined KOISHI_HAVE_WIN32API
	#define WIN32_LEAN_AND_MEAN
	#include <windows.h>
	#include <memoryapi.h>
	#include <sysinfoapi.h>
#else
	#if defined KOISHI_HAVE_MMAP
		#include <sys/mman.h>
	#endif

	#if defined KOISHI_HAVE_SYSCONF || defined KOISHI_HAVE_GETPAGESIZE
		#include <unistd.h>
#include <assert.h>
#endif
#endif

static inline size_t get_page_size(void) {
#if defined KOISHI_STATIC_PAGE_SIZE
	return KOISHI_STATIC_PAGE_SIZE;
#elif defined KOISHI_HAVE_WIN32API
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	return si.dwPageSize;
#elif defined KOISHI_HAVE_SYSCONF
	return sysconf(KOISHI_SC_PAGE_SIZE);
#elif defined KOISHI_HAVE_GETPAGESIZE
	return getpagesize();
#else
	#error No way to detect page size
#endif
}

static inline void *alloc_stack_mem(size_t size) {
#if defined KOISHI_HAVE_WIN32API
	return VirtualAlloc(NULL, size, MEM_COMMIT, PAGE_READWRITE);
#elif defined KOISHI_HAVE_MMAP
	return mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | KOISHI_MAP_ANONYMOUS, -1, 0);
#elif defined KOISHI_HAVE_ALIGNED_ALLOC
	return aligned_alloc(koishi_util_page_size(), size);
#elif defined KOISHI_HAVE_POSIX_MEMALIGN
	void *p = NULL;
	int res = posix_memalign(&p, koishi_util_page_size(), size);
	(void)res;
	return p;
#else
	//#pragma GCC warning "Stack will not be aligned to page size"
	return malloc(size);
#endif
}

static inline void free_stack_mem(void *stack, size_t size) {
#if defined KOISHI_HAVE_WIN32API
	(void)size;
	VirtualFree(stack, 0, MEM_RELEASE);
#elif defined KOISHI_HAVE_MMAP
	munmap(stack, size);
#else
	(void)size;
	free(stack);
#endif
}

void *alloc_stack(size_t minsize, size_t *realsize) {
	size_t sz = koishi_util_real_stack_size(minsize);
	*realsize = sz;
	return alloc_stack_mem(sz);
}

void free_stack(void *stack, size_t size) {
	free_stack_mem(stack, size);
}

KOISHI_API size_t koishi_util_page_size(void) {
	static size_t page_size = 0;

	if(!page_size) {
		page_size = get_page_size();
	}

	return page_size;
}

KOISHI_API size_t koishi_util_real_stack_size(size_t size) {
	size_t page_size = koishi_util_page_size();

	assert(page_size) ;

	if(size == 0) {
		size = 64 * 1024;
	}

	size_t num_pages = (size - 1) / page_size + 1;

	if(num_pages < 2) {
		num_pages = 2;
	}

	return num_pages * page_size;
}
