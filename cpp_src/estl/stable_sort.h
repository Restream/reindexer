#pragma once

#include <algorithm>
#include <iterator>
#include "tools/assertrx.h"

namespace reindexer {

namespace sort_impl {

constexpr size_t kInsertionSortThreshold = 32;

template <std::random_access_iterator Iter, typename Compare>
void insertion_sort(Iter begin, Iter end,
					Compare&& comp) noexcept(std::is_nothrow_invocable_v<Compare, typename std::iterator_traits<Iter>::value_type,
																		 typename std::iterator_traits<Iter>::value_type>) {
	typedef typename std::iterator_traits<Iter>::value_type T;
	assertrx_dbg(begin != end);

	for (Iter cur = begin + 1; cur != end; ++cur) {
		Iter sift = cur;
		Iter sift_1 = cur - 1;

		if (comp(*sift, *sift_1)) {
			T tmp = std::move(*sift);

			do {
				*sift-- = std::move(*sift_1);
			} while (sift != begin && comp(tmp, *--sift_1));

			*sift = std::move(tmp);
		}
	}
}

}  // namespace sort_impl

// 1. GCC's std::stable_sort performs allocations even in the simplest scenarios, so handling some of them explicitly.
// 2. Some of the std::stable_sort implementations do not use insertion_sort (even if it's more effective for small containers and also
// allows to avoid allocation).
template <std::random_access_iterator Iter, typename Compare>
RX_ALWAYS_INLINE void stable_sort(Iter begin, Iter end, Compare&& comp) {
	assertrx_dbg(begin <= end);
	const size_t dist = std::distance(begin, end);
	switch (dist) {
		case 0:
		case 1:
			break;
		case 2: {
			auto it = begin;
			auto& a = *(it++);
			auto& b = *(it);
			if (comp(b, a)) {
				std::swap(a, b);
			}
			break;
		}
		case 3: {
			auto it = begin;
			auto& a = *(it++);
			auto& b = *(it++);
			auto& c = *(it);
			if (comp(b, a)) {
				std::swap(a, b);
			}
			if (comp(c, b)) {
				std::swap(b, c);
			}
			if (comp(b, a)) {
				std::swap(a, b);
			}
			break;
		}
		default:
			if (dist <= sort_impl::kInsertionSortThreshold) {
				sort_impl::insertion_sort(std::move(begin), std::move(end), std::move(comp));
			} else {
				// clang-tidy reports that std::get_temporary_buffer is deprecated
				// NOLINTNEXTLINE (clang-diagnostic-deprecated-declarations)
				std::stable_sort(std::move(begin), std::move(end), std::move(comp));
			}
	}
}

}  // namespace reindexer
