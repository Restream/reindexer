#include <koishi.h>
#include <ucontext.h>
#include <assert.h>
#include <stdlib.h>
#include "../stack_alloc.h"

typedef struct uctx_fiber {
	ucontext_t *uctx;
} koishi_fiber_t;

#include "../fiber.h"

static KOISHI_THREAD_LOCAL ucontext_t co_main_uctx;

static KOISHI_NORETURN void co_entry(void) {
	koishi_entry(co_current);
}

static void koishi_fiber_init(koishi_fiber_t *fiber, size_t min_stack_size) {
	ucontext_t *uctx = calloc(1, sizeof(*uctx));
	getcontext(uctx);
	uctx->uc_stack.ss_sp = alloc_stack(min_stack_size, &uctx->uc_stack.ss_size);
	// TODO: makecontext_e2k() could fail, but for now we don't expect that
	makecontext_e2k(uctx, co_entry, 0);
	fiber->uctx = uctx;
}

static void koishi_fiber_recycle(koishi_fiber_t *fiber) {
	freecontext_e2k(fiber->uctx);
	// TODO: makecontext_e2k() could fail, but for now we don't expect that
	makecontext_e2k(fiber->uctx, co_entry, 0);
}

static void koishi_fiber_swap(koishi_fiber_t *from, koishi_fiber_t *to) {
	swapcontext(from->uctx, to->uctx);
}

static void koishi_fiber_init_main(koishi_fiber_t *fiber) {
	fiber->uctx = &co_main_uctx;
	getcontext(&co_main_uctx);
}

static void koishi_fiber_deinit(koishi_fiber_t *fiber) {
	if(fiber->uctx) {
		if(fiber->uctx->uc_stack.ss_sp) {
			free_stack(fiber->uctx->uc_stack.ss_sp, fiber->uctx->uc_stack.ss_size);
		}

		freecontext_e2k(fiber->uctx);
		free(fiber->uctx);
		fiber->uctx = NULL;
	}
}

KOISHI_API void *koishi_get_stack(koishi_coroutine_t *co, size_t *stack_size) {
	if(stack_size) *stack_size = co->fiber.uctx->uc_stack.ss_size;
	return co->fiber.uctx->uc_stack.ss_sp;
}
