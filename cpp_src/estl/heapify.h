#pragma once

#include <span>
#include "defines.h"
#include "tools/assertrx.h"

namespace reindexer {

template <typename T, typename CompareT>
RX_ALWAYS_INLINE void heapifyRoot(std::span<T> vec) noexcept {
	static_assert(std::is_trivially_move_assignable_v<T> && sizeof(T) <= 16, "Expecting T being a type with fast swaps");
	T* target = vec.data();
	T* end = target + vec.size();
	CompareT c;
	for (size_t i = 0;;) {
		T* cur = target;
		const auto lIdx = (i << 1) + 1;
		T* left = vec.data() + lIdx;
		T* right = left + 1;

		if (left < end && c(*target, *left)) {
			target = left;
			i = lIdx;
		}
		if (right < end && c(*target, *right)) {
			target = right;
			i = lIdx + 1;
		}
		if (cur == target) {
			return;
		}
		std::swap(*cur, *target);
	}
}

template <typename CompareT, typename T>
RX_ALWAYS_INLINE void removeHeapRoot(T& heap) noexcept {
	assertrx_dbg(!heap.empty());
	std::swap(heap.front(), heap.back());
	heap.pop_back();
	if (!heap.empty()) {
		heapifyRoot<typename T::value_type, CompareT>(heap);
	}
}

}  // namespace reindexer