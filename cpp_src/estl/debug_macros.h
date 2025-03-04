#pragma once

#include <iostream>
#include "debug/backtrace.h"

namespace reindexer {

#if defined(RX_WITH_STDLIB_DEBUG) && defined(__GNUC__) && !defined(__INTEL_COMPILER) && !defined(__clang__)

// Verify that the subscript _N is less than the container's size.
#define rx_debug_check_subscript(_N)                             \
	if (_N >= this->size()) debug::print_crash_query(std::cerr); \
	if (int64_t(_N) < 0) debug::print_crash_query(std::cerr);    \
	_GLIBCXX_DEBUG_VERIFY(                                       \
		_N < this->size() && int64_t(_N) >= 0,                   \
		_M_message(__gnu_debug::__msg_subscript_oob)._M_sequence(*this, "this")._M_integer(_N, #_N)._M_integer(this->size(), "size"))

// Verify that the subscript _N is less than or equal to the container's size.
#define rx_debug_check_subscript_le(_N)                         \
	if (_N > this->size()) debug::print_crash_query(std::cerr); \
	if (int64_t(_N) < 0) debug::print_crash_query(std::cerr);   \
	_GLIBCXX_DEBUG_VERIFY(                                      \
		_N <= this->size() && int64_t(_N) >= 0,                 \
		_M_message(__gnu_debug::__msg_subscript_oob)._M_sequence(*this, "this")._M_integer(_N, #_N)._M_integer(this->size(), "size"))

// Verify that the container is nonempty
#define rx_debug_check_nonempty()                           \
	if (this->empty()) debug::print_crash_query(std::cerr); \
	_GLIBCXX_DEBUG_VERIFY(!this->empty(), _M_message(__gnu_debug::__msg_empty)._M_sequence(*this, "this"))

// Vierify range bounds
#define rx_debug_check_valid_range(_left, _right)            \
	if (_left > _right) debug::print_crash_query(std::cerr); \
	_GLIBCXX_DEBUG_VERIFY(_left <= _right,                   \
						  _M_message(__gnu_debug::__msg_valid_range)._M_iterator(_left, #_left)._M_iterator(_right, #_right))

#else  // #if defined(RX_WITH_STDLIB_DEBUG) && defined (__GNUC__) && !defined(__clang__)

#define rx_debug_check_subscript(_N) ((void)0)
#define rx_debug_check_subscript_le(_N) ((void)0)
#define rx_debug_check_nonempty() ((void)0)
#define rx_debug_check_valid_range(_left, _right) ((void)0)

#endif	// #if defined(RX_WITH_STDLIB_DEBUG) && defined (__GNUC__) && !defined(__clang__)

}  // namespace reindexer
