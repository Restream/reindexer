#include <assert.h>
#include <koishi.h>
#include "fcontext.hpp"
#include "../stack_alloc.h"


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
#else // REINDEX_WITH_TSAN
	#define IF_KOISHI_TSAN(a)
#endif	// REINDEX_WITH_TSAN

#ifdef REINDEX_WITH_ASAN
#include <sanitizer/common_interface_defs.h>
#define IF_KOISHI_ASAN(a) a
#else // REINDEX_WITH_ASAN
#define IF_KOISHI_ASAN(a)
#endif	// REINDEX_WITH_ASAN

typedef struct fcontext_fiber
{
    fcontext_t fctx;
    char *stack;
    size_t stack_size;
    KOISHI_VALGRIND_STACK_ID(valgrind_stack_id)
#ifdef KOISHI_TSAN
	void *tsan_fiber;
#endif
}koishi_fiber_t;

#include "../fiber.h"


static void koishi_fiber_swap(koishi_fiber_t *from, koishi_fiber_t *to)
{
	IF_KOISHI_ASAN(void *fake_stack = NULL; void* stack = to->stack; __sanitizer_start_switch_fiber(&fake_stack, stack, to->stack_size);)
	IF_KOISHI_TSAN(__tsan_switch_to_fiber(to->tsan_fiber,0);)
    transfer_t tf = jump_fcontext(to->fctx, from);
	//if (co_current != &co_main)
	IF_KOISHI_ASAN(__sanitizer_finish_switch_fiber(NULL, NULL, NULL);)
    from = tf.data;
    from->fctx = tf.fctx;
}

static KOISHI_NORETURN void co_entry(transfer_t tf)
{
	IF_KOISHI_ASAN(__sanitizer_finish_switch_fiber(NULL, NULL, NULL);)
    koishi_coroutine_t *co = co_current;
    assert(tf.data == &co->caller->fiber);
    ((koishi_fiber_t *) tf.data)->fctx = tf.fctx;
    koishi_entry(co);
}

static inline void init_fiber_fcontext(koishi_fiber_t *fiber)
{
    fiber->fctx = make_fcontext(fiber->stack + fiber->stack_size, fiber->stack_size, co_entry);
}

static void  koishi_fiber_init(koishi_fiber_t *fiber, size_t min_stack_size)
{
    fiber->stack = alloc_stack(min_stack_size, &fiber->stack_size);
	IF_KOISHI_TSAN(fiber->tsan_fiber=__tsan_create_fiber(0);)
    KOISHI_VALGRIND_STACK_REGISTER(fiber->valgrind_stack_id, fiber->stack, fiber->stack + fiber->stack_size);
    init_fiber_fcontext(fiber);
}

static void koishi_fiber_recycle(koishi_fiber_t *fiber)
{
	//IF_KOISHI_TSAN(if(fiber->tsan_fiber != NULL) __tsan_destroy_fiber(fiber->tsan_fiber);fiber->tsan_fiber=NULL;)
	init_fiber_fcontext(fiber);
}

static void koishi_fiber_init_main(koishi_fiber_t *fiber)
{
#ifdef REINDEX_WITH_TSAN
	if (!co_main.fiber.tsan_fiber)
		fiber->tsan_fiber = __tsan_get_current_fiber();
#else // REINDEX_WITH_TSAN
	(void) fiber;
#endif // REINDEX_WITH_TSAN
}

static void koishi_fiber_deinit(koishi_fiber_t *fiber)
{
    if (fiber->stack)
    {
        KOISHI_VALGRIND_STACK_DEREGISTER(fiber->valgrind_stack_id);
        free_stack(fiber->stack, fiber->stack_size);
        fiber->stack = NULL;
    }
	IF_KOISHI_TSAN(if(fiber->tsan_fiber != NULL )__tsan_destroy_fiber(fiber->tsan_fiber);fiber->tsan_fiber=NULL;)
}

KOISHI_API void *koishi_get_stack(koishi_coroutine_t *co, size_t *stack_size)
{
    if (stack_size) *stack_size = co->fiber.stack_size;
    return co->fiber.stack;
}
