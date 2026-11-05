#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <assert.h>
#include <koishi.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "../stack_alloc.h"
#include "fcontext.hpp"

#if defined(_WIN32)
#include <windows.h>
#else
#include <pthread.h>
#endif

#ifdef REINDEX_WITH_TSAN
#if defined(__GNUC__) && !defined(__clang__) && !defined(__INTEL_COMPILER) && __GNUC__ >= 10
// Enable tsan fiber annotation for GCC >= 10.0
#include <sanitizer/tsan_interface.h>
#define IF_KOISHI_TSAN(a) a
#define KOISHI_TSAN
#elif defined(__clang__) && defined(__clang_major__) && __clang_major__ >= 10
// Enable tsan fiber annotation for Clang >= 10.0
#include <sanitizer/tsan_interface.h>
#define IF_KOISHI_TSAN(a) a
#define KOISHI_TSAN
#endif
#else  // REINDEX_WITH_TSAN
#define IF_KOISHI_TSAN(a)
#endif	// REINDEX_WITH_TSAN

#ifdef REINDEX_WITH_ASAN
#include <sanitizer/common_interface_defs.h>
#define IF_KOISHI_ASAN(a) a
#define KOISHI_ASAN
#else  // REINDEX_WITH_ASAN
#define IF_KOISHI_ASAN(a)
#endif	// REINDEX_WITH_ASAN

typedef struct fcontext_fiber {
	fcontext_t fctx;
	char* stack;
	size_t stack_size;
	KOISHI_VALGRIND_STACK_ID(valgrind_stack_id)
#ifdef KOISHI_TSAN
	void* tsan_fiber;
#endif
#ifdef KOISHI_ASAN
	void* asan_fake_stack; /* saved while suspended; NULL when running/fresh/dead-cleaned */
#endif
} koishi_fiber_t;

#include "../fiber.h"

#ifdef KOISHI_ASAN
static void koishi_asan_fail(const char* msg) {
	fprintf(stderr, "reindexer/koishi ASAN fiber error: %s\n", msg);
	abort();
}
#endif

/* Fill fiber->stack (low address) + fiber->stack_size for the OS thread stack. */
static int koishi_query_os_thread_stack(char** stack_out, size_t* size_out) {
#if defined(_WIN32)
	ULONG_PTR low = 0, high = 0;
	GetCurrentThreadStackLimits(&low, &high);
	if (!low || high <= low) {
		return -1;
	}
	*stack_out = (char*)(uintptr_t)low;
	*size_out = (size_t)(high - low);
	return 0;
#elif defined(__APPLE__)
	/* Darwin: pthread_get_stackaddr_np returns the TOP (high address). */
	void* top = pthread_get_stackaddr_np(pthread_self());
	size_t sz = pthread_get_stacksize_np(pthread_self());
	if (!top || !sz) {
		return -1;
	}
	*stack_out = (char*)top - sz;
	*size_out = sz;
	return 0;
#else	// !defined(_WIN32) && !defined(__APPLE__)
	/* Linux and other glibc platforms with pthread_getattr_np. */
	pthread_attr_t attr;
	int rc = pthread_getattr_np(pthread_self(), &attr);
	if (rc != 0) {
		return -1;
	}
	void* stack_addr = NULL;
	size_t stack_sz = 0;
	rc = pthread_attr_getstack(&attr, &stack_addr, &stack_sz);
	pthread_attr_destroy(&attr);
	if (rc != 0 || !stack_addr || !stack_sz) {
		return -1;
	}
	*stack_out = (char*)stack_addr;
	*size_out = stack_sz;
	return 0;
#endif	// !defined(_WIN32) && !defined(__APPLE__)
}

#ifdef KOISHI_ASAN
__attribute__((no_sanitize("address")))
#endif
static void koishi_fiber_swap(koishi_fiber_t* from, koishi_fiber_t* to) {
#ifdef KOISHI_ASAN
	/* state is set BEFORE swap in fiber.h koishi_swap_coroutine — safe to read */
	const int from_dead = ((KOISHI_FIBER_TO_COROUTINE(from))->state == KOISHI_DEAD);
	if (!to->stack || !to->stack_size) {
		koishi_asan_fail("destination fiber has no stack bounds (incl. main)");
	}
	/*
	 * Leaving `from`: if it is dead we will never resume it — pass NULL so ASAN
	 * destroys its FakeStack. Otherwise save FakeStack into from->asan_fake_stack.
	 */
	__sanitizer_start_switch_fiber(from_dead ? NULL : &from->asan_fake_stack, to->stack, to->stack_size);
	if (from_dead) {
		from->asan_fake_stack = NULL;
	}
#endif
	IF_KOISHI_TSAN(__tsan_switch_to_fiber(to->tsan_fiber, 0);)
	transfer_t tf = jump_fcontext(to->fctx, from);
#ifdef KOISHI_ASAN
	/*
	 * Back on `from` (this jump site): install the FakeStack saved when we last
	 * left this fiber. Clear the slot afterward — AsanThread owns it until the
	 * next start_switch(&from->asan_fake_stack).
	 */
	__sanitizer_finish_switch_fiber(from->asan_fake_stack, NULL, NULL);
	from->asan_fake_stack = NULL;
#endif
	from = tf.data;
	from->fctx = tf.fctx;
}

#ifdef KOISHI_ASAN
__attribute__((no_sanitize("address")))
#endif
static KOISHI_NORETURN void co_entry(transfer_t tf) {
#ifdef KOISHI_ASAN
	/* co_current is already the new fiber (set in koishi_swap_coroutine). FakeStack is NULL on first entry. */
	__sanitizer_finish_switch_fiber(co_current->fiber.asan_fake_stack, NULL, NULL);
	co_current->fiber.asan_fake_stack = NULL;
#endif
	koishi_coroutine_t* co = co_current;
	assert(tf.data == &co->caller->fiber);
	((koishi_fiber_t*)tf.data)->fctx = tf.fctx;
	koishi_entry(co);
}

static inline void init_fiber_fcontext(koishi_fiber_t* fiber) {
	fiber->fctx = make_fcontext(fiber->stack + fiber->stack_size, fiber->stack_size, co_entry);
}

static void koishi_fiber_init(koishi_fiber_t* fiber, size_t min_stack_size) {
	fiber->stack = alloc_stack(min_stack_size, &fiber->stack_size);
	IF_KOISHI_ASAN(fiber->asan_fake_stack = NULL;)
	IF_KOISHI_TSAN(fiber->tsan_fiber = __tsan_create_fiber(0);)
	KOISHI_VALGRIND_STACK_REGISTER(fiber->valgrind_stack_id, fiber->stack, fiber->stack + fiber->stack_size);
	init_fiber_fcontext(fiber);
}

static void koishi_fiber_recycle(koishi_fiber_t* fiber) {
#ifdef KOISHI_ASAN
	/*
	 * Recycle is only safe after a natural DEAD path that already called
	 * start_switch(NULL). Suspended kill without that path is unsupported under ASAN.
	 */
	if (fiber->asan_fake_stack != NULL) {
		koishi_asan_fail("recycle with non-NULL asan_fake_stack (fiber must die via DEAD swap first)");
	}
#endif
#ifdef KOISHI_TSAN
	if (fiber->tsan_fiber != NULL) {
		__tsan_destroy_fiber(fiber->tsan_fiber);
	}
	fiber->tsan_fiber = __tsan_create_fiber(0);
#endif
	init_fiber_fcontext(fiber);
}

static void koishi_fiber_init_main(koishi_fiber_t* fiber) {
	/* Real OS thread stack bounds — required so ASAN switches TO main get valid stack/size. */
	char* stack = NULL;
	size_t stack_sz = 0;
	if (koishi_query_os_thread_stack(&stack, &stack_sz) != 0) {
#ifdef KOISHI_ASAN
		koishi_asan_fail("failed to query OS thread stack bounds for main fiber");
#else
		fprintf(stderr, "reindexer/koishi warning: failed to query OS thread stack bounds for main fiber\n");
#endif
	} else {
		fiber->stack = stack;
		fiber->stack_size = stack_sz;
	}
	IF_KOISHI_ASAN(fiber->asan_fake_stack = NULL;)
#ifdef KOISHI_TSAN
	if (!co_main.fiber.tsan_fiber) {
		fiber->tsan_fiber = __tsan_get_current_fiber();
	}
#endif
}

static void koishi_fiber_deinit(koishi_fiber_t* fiber) {
	const int is_main = (fiber == &co_main.fiber);
	/* Main fiber.stack points at the OS thread stack — never free it. */
	if (fiber->stack && !is_main) {
		KOISHI_VALGRIND_STACK_DEREGISTER(fiber->valgrind_stack_id);
		free_stack(fiber->stack, fiber->stack_size);
		fiber->stack = NULL;
	}
#ifdef KOISHI_ASAN
	/*
	 * Natural death path already destroyed FakeStack via start(NULL).
	 * koishi_kill() on a suspended fiber skips that and is unsupported under ASAN.
	 */
	if (fiber->asan_fake_stack != NULL) {
		koishi_asan_fail("deinit with non-NULL asan_fake_stack (suspended koishi_kill under ASAN?)");
	}
#endif
#ifdef KOISHI_TSAN
	/* Do not destroy the OS-thread fiber identity used by main. */
	if (!is_main && fiber->tsan_fiber != NULL) {
		__tsan_destroy_fiber(fiber->tsan_fiber);
		fiber->tsan_fiber = NULL;
	}
#endif
}

KOISHI_API void* koishi_get_stack(koishi_coroutine_t* co, size_t* stack_size) {
	if (stack_size) {
		*stack_size = co->fiber.stack_size;
	}
	return co->fiber.stack;
}
