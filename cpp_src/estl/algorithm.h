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

}  // namespace reindexer
