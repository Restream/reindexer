
//          Copyright Oliver Kowalke 2009.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#pragma once

#include <cstddef>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* fcontext_t;

/*! the context-from type
 *
 * @fctx    the from-context
 * @data    the passed user private data
 */
struct transfer_t {
	fcontext_t fctx;
	void* data;
};

/*! the context entry function type
 *
 * @param from        the from-context
 */
typedef void (*context_func_t)(transfer_t from);

/*! jump to the given context
 *
 * @param to          the to-context
 * @param vp          the passed user private data
 *
 * @return            the from-context
 */
transfer_t jump_fcontext(fcontext_t const to, void* vp);

/*! make context from the given the stack space and the callback function
 *
 * @param sp     the stack data
 * @param size   the stack size
 * @param fn     the entry function
 *
 * @return              the context pointer
 */
fcontext_t make_fcontext(void* sp, std::size_t size, context_func_t fn);

#ifdef __cplusplus
}
#endif
