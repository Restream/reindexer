
#ifndef KOISHI_FIBER_H
#define KOISHI_FIBER_H

/*
 * Common template used by "fiber-like" backends.
 *
 * The implementation must first define the koishi_fiber_t struct, then include
 * this header, then implement the koishi_fiber_* internal interface functions.
 */

#include <assert.h>
#include <koishi.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct koishi_coroutine {
	koishi_fiber_t fiber;
	koishi_coroutine_t *caller;
	void *userdata;
	koishi_entrypoint_t entry;
	int state;
} koishi_coroutine;

static_assert(sizeof(struct koishi_coroutine) <= sizeof(struct koishi_coroutine_pub), "struct koishi_coroutine is too large");

#define KOISHI_FIBER_TO_COROUTINE(fib) (koishi_coroutine *)(((char *)(fib)) - offsetof(koishi_coroutine, fiber))

static void koishi_fiber_deinit(koishi_fiber_t *fiber);
static void koishi_fiber_init(koishi_fiber_t *fiber, size_t min_stack_size);
static void koishi_fiber_init_main(koishi_fiber_t *fiber);
static void koishi_fiber_recycle(koishi_fiber_t *fiber);
static void koishi_fiber_swap(koishi_fiber_t *from, koishi_fiber_t *to);

static KOISHI_THREAD_LOCAL koishi_coroutine co_main;
static KOISHI_THREAD_LOCAL koishi_coroutine *co_current;

static void koishi_swap_coroutine(koishi_coroutine *from, koishi_coroutine *to, int state) {
	//#if !defined NDEBUG
	//    koishi_coroutine *prev = koishi_active();
	//    assert(from->state == KOISHI_RUNNING);
	//    assert(to->state == KOISHI_SUSPENDED);
	//    assert(prev == from);
	//#endif

	from->state = state;
	co_current = to;
	to->state = KOISHI_RUNNING;
	koishi_fiber_swap(&from->fiber, &to->fiber);

	//#if !defined NDEBUG
	//    assert(to->state == KOISHI_SUSPENDED || to->state == KOISHI_DEAD);
	//    assert(from->state == KOISHI_RUNNING);
	//#endif
}

static void koishi_return_to_caller(koishi_coroutine *from, int state) {
	while (from->caller->state == KOISHI_DEAD) {
		from->caller = from->caller->caller;
	}

	koishi_swap_coroutine(from, from->caller, state);
}

static inline KOISHI_NORETURN void koishi_entry(koishi_coroutine *co) {
	co->userdata = co->entry(co->userdata);
	koishi_return_to_caller(co, KOISHI_DEAD);
	KOISHI_UNREACHABLE;
}

KOISHI_API koishi_coroutine *koishi_create() { return malloc(sizeof(struct koishi_coroutine)); }

KOISHI_API void koishi_destroy(koishi_coroutine *pointer) { free(pointer); }

KOISHI_API void koishi_init(koishi_coroutine *co, size_t min_stack_size, koishi_entrypoint_t entry_point) {
	co->state = KOISHI_SUSPENDED;
	co->entry = entry_point;
	koishi_fiber_init(&co->fiber, min_stack_size);
}

KOISHI_API void koishi_recycle(koishi_coroutine *co, koishi_entrypoint_t entry_point) {
	co->state = KOISHI_SUSPENDED;
	co->entry = entry_point;
	koishi_fiber_recycle(&co->fiber);
}

KOISHI_API void *koishi_resume(koishi_coroutine *co, void *arg) {
	koishi_coroutine *prev = koishi_active();
	co->userdata = arg;
	co->caller = prev;
	koishi_swap_coroutine(prev, co, KOISHI_SUSPENDED);
	return co->userdata;
}

KOISHI_API void *koishi_yield(void *arg) {
	koishi_coroutine *co = koishi_active();
	co->userdata = arg;
	koishi_return_to_caller(co, KOISHI_SUSPENDED);
	return co->userdata;
}

KOISHI_API KOISHI_NORETURN void koishi_die(void *arg) {
	koishi_coroutine *co = koishi_active();
	co->userdata = arg;
	koishi_return_to_caller(co, KOISHI_DEAD);
	KOISHI_UNREACHABLE;
}

KOISHI_API void koishi_kill(koishi_coroutine *co, void *arg) {
	if (co == koishi_active()) {
		koishi_die(arg);
	} else {
		assert(co->state == KOISHI_SUSPENDED);
		co->state = KOISHI_DEAD;
		co->userdata = arg;
	}
}

KOISHI_API void koishi_deinit(koishi_coroutine *co) { koishi_fiber_deinit(&co->fiber); }

KOISHI_API koishi_coroutine *koishi_active(void) {
	if (!co_current) {
		co_main.state = KOISHI_RUNNING;
		co_current = &co_main;
		koishi_fiber_init_main(&co_main.fiber);
	}

	return co_current;
}

KOISHI_API int koishi_state(koishi_coroutine *co) { return co->state; }

#endif	// KOISHI_FIBER_H
