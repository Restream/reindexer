#pragma once

#include <utility>

namespace reindexer {

template <typename BidirectionalIt, typename UnaryPred>
BidirectionalIt unstable_remove_if(BidirectionalIt begin, BidirectionalIt end,
								   UnaryPred pred) noexcept(noexcept(*begin = std::move(*end)) && noexcept(pred(*begin))) {
	for (; begin != end; ++begin) {
		if (pred(*begin)) {
			do {
				if (--end == begin) {
					return end;
				}
			} while (pred(*end));
			*begin = std::move(*end);
		}
	}
	return end;
}

// Room for one more element; doubles capacity when full (avoids reserve(size+1) pin).
template <typename Cont>
void ensure_capacity_for_one_more(Cont& cont) {
	if (cont.capacity() == cont.size()) {
		cont.reserve(cont.size() ? cont.size() * 2 : 1);
	}
}

}  // namespace reindexer
