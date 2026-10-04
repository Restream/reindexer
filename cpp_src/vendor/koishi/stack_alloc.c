
#include <koishi.h>
#include "stack_alloc.h"

#include <stdlib.h>
#include <assert.h>

#if defined KOISHI_HAVE_WIN32API
	#define WIN32_LEAN_AND_MEAN
	#include <windows.h>
	#include <memoryapi.h>
	#include <sysinfoapi.h>
#else // !defined KOISHI_HAVE_WIN32API
	#if defined KOISHI_HAVE_MMAP || defined REINDEX_KOISHI_STACK_GUARD
		#include <sys/mman.h>
	#endif // defined KOISHI_HAVE_MMAP || defined REINDEX_KOISHI_STACK_GUARD

	#if defined KOISHI_HAVE_SYSCONF || defined KOISHI_HAVE_GETPAGESIZE
		#include <unistd.h>
	#endif // defined KOISHI_HAVE_SYSCONF || defined KOISHI_HAVE_GETPAGESIZE
#endif // !defined KOISHI_HAVE_WIN32API

#ifndef KOISHI_MAP_ANONYMOUS
	#if defined MAP_ANONYMOUS
		#define KOISHI_MAP_ANONYMOUS MAP_ANONYMOUS
	#elif defined(MAP_ANON)
		#define KOISHI_MAP_ANONYMOUS MAP_ANON
	#else // !defined(MAP_ANON)
		#define KOISHI_MAP_ANONYMOUS 0x20
	#endif // !defined(MAP_ANON)
#endif // KOISHI_MAP_ANONYMOUS

#if defined(REINDEX_KOISHI_STACK_GUARD) && !defined(KOISHI_HAVE_WIN32API)
#if defined(KOISHI_HAVE_MMAP) || defined(KOISHI_HAVE_POSIX_MEMALIGN)
#define KOISHI_STACK_GUARD_AVAILABLE 1

static int koishi_stack_guard_protect(void *guard_page, size_t page_size) {
	return mprotect(guard_page, page_size, PROT_NONE);
}

static int koishi_stack_guard_unprotect(void *guard_page, size_t page_size) {
	return mprotect(guard_page, page_size, PROT_READ | PROT_WRITE);
}

#endif // defined(KOISHI_HAVE_MMAP) || defined(KOISHI_HAVE_POSIX_MEMALIGN)
#endif // defined(REINDEX_KOISHI_STACK_GUARD) && !defined(KOISHI_HAVE_WIN32API)

static inline size_t get_page_size(void) {
#if defined(KOISHI_STATIC_PAGE_SIZE)
	return KOISHI_STATIC_PAGE_SIZE;
#elif defined(KOISHI_HAVE_WIN32API)
	SYSTEM_INFO si;
	GetSystemInfo(&si);
	return si.dwPageSize;
#elif defined(KOISHI_HAVE_SYSCONF)
	return sysconf(KOISHI_SC_PAGE_SIZE);
#elif defined(KOISHI_HAVE_GETPAGESIZE)
	return getpagesize();
#else // !defined(KOISHI_HAVE_GETPAGESIZE)
	#error No way to detect page size
#endif // !defined(KOISHI_HAVE_GETPAGESIZE)
}

#if KOISHI_STACK_GUARD_AVAILABLE

static void *alloc_stack_guarded(size_t usable, size_t *realsize) {
	const size_t page_size = koishi_util_page_size();
	const size_t total = usable + page_size;
	void *base = NULL;

#if defined(KOISHI_HAVE_MMAP)
	base = mmap(NULL, total, PROT_READ | PROT_WRITE, MAP_PRIVATE | KOISHI_MAP_ANONYMOUS, -1, 0);
	if (base == MAP_FAILED) {
		*realsize = usable;
		return NULL;
	}
#elif defined(KOISHI_HAVE_POSIX_MEMALIGN)
	if (posix_memalign(&base, page_size, total) != 0) {
		*realsize = usable;
		return NULL;
	}
#endif // defined(KOISHI_HAVE_POSIX_MEMALIGN)

	if (koishi_stack_guard_protect(base, page_size) != 0) {
#if defined(KOISHI_HAVE_MMAP)
		munmap(base, total);
#else // !defined(KOISHI_HAVE_MMAP)
		free(base);
#endif // !defined(KOISHI_HAVE_MMAP)
		*realsize = usable;
		return NULL;
	}

	*realsize = usable;
	return (char *)base + page_size;
}

static void free_stack_guarded(void *stack, size_t size) {
	if (!stack) {
		return;
	}

	const size_t page_size = koishi_util_page_size();
	void *const base = (char *)stack - page_size;

	koishi_stack_guard_unprotect(base, page_size);

#if defined(KOISHI_HAVE_MMAP)
	munmap(base, size + page_size);
#else // !defined(KOISHI_HAVE_MMAP)
	(void)size;
	free(base);
#endif // !defined(KOISHI_HAVE_MMAP)
}

#else // !KOISHI_STACK_GUARD_AVAILABLE

static void *alloc_stack_mem(size_t size) {
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
#else // !defined KOISHI_HAVE_POSIX_MEMALIGN
	return malloc(size);
#endif // !defined KOISHI_HAVE_POSIX_MEMALIGN
}

static void free_stack_mem(void *stack, size_t size) {
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

#endif // !KOISHI_STACK_GUARD_AVAILABLE

void *alloc_stack(size_t minsize, size_t *realsize) {
	const size_t usable = koishi_util_real_stack_size(minsize);

#if defined(KOISHI_STACK_GUARD_AVAILABLE)
	return alloc_stack_guarded(usable, realsize);
#else // !defined(KOISHI_STACK_GUARD_AVAILABLE)
	*realsize = usable;
	return alloc_stack_mem(usable);
#endif // !defined(KOISHI_STACK_GUARD_AVAILABLE)
}

void free_stack(void *stack, size_t size) {
#if defined(KOISHI_STACK_GUARD_AVAILABLE)
	free_stack_guarded(stack, size);
#else // !defined(KOISHI_STACK_GUARD_AVAILABLE)
	free_stack_mem(stack, size);
#endif // !defined(KOISHI_STACK_GUARD_AVAILABLE)
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
