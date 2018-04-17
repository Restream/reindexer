#include "sortingprioritiestable.h"
#include <algorithm>
#include <cassert>
#include <unordered_map>
#include "tools/errors.h"
#include "tools/slice.h"
#include "tools/stringstools.h"

using namespace reindexer;

SortingPrioritiesTable::SortingPrioritiesTable(const std::string& sortOrderUTF8)
	: sortOrder_(std::make_shared<SortOrderTable>()), sortOrderCharacters_(sortOrderUTF8) {
	using Range = std::pair<uint16_t, uint16_t>;
	std::vector<Range> ranges;

	size_t items = 0;
	wchar_t lastItem = 0;
	uint16_t priority = 0;
	uint16_t maxPriority = 0;

	wstring orderUtf16 = reindexer::utf8_to_utf16(sortOrderUTF8);
	for (auto it = orderUtf16.begin(); it != orderUtf16.end(); ++it) {
		if (*it != '-') {
			if (++items == 2) {
				if (*it <= lastItem) throw Error(errLogic, "Incorrect format of sort order string: range should be ascending");
				for (auto ch = lastItem; ch <= *it; ++ch) {
					sortOrder_->operator[](ch) = priority++;
				}
				ranges.push_back({lastItem, *it - lastItem + 1});
				maxPriority = priority;
				items = 0;
			}
			if (*it == orderUtf16.back() && items != 0)
				throw Error(errLogic, "Incorrect format of sort order string: last item should close the range");
			lastItem = *it;
		} else {
			if (*it == orderUtf16.back()) throw Error(errLogic, "Incorrect format of sort order string: '-' cannot be the last character");
		}
	}

	if (!ranges.empty()) {
		std::sort(ranges.begin(), ranges.end(), [](const Range& lhs, const Range& rhs) { return lhs.first < rhs.first; });
		size_t rangeIdx = 0;
		uint16_t outOfRangePriority = maxPriority;
		for (size_t i = 0; i < tableSize;) {
			if ((rangeIdx < ranges.size()) && (ranges[rangeIdx].first == i)) {
				i += ranges[rangeIdx].second;
				++rangeIdx;
			} else {
				sortOrder_->operator[](i++) = outOfRangePriority++;
			}
		}
	}
}

int SortingPrioritiesTable::GetPriority(wchar_t c) const {
	assert(sortOrder_.get() != nullptr);
	assert(static_cast<uint32_t>(c) < tableSize);
	uint16_t ch(static_cast<uint16_t>(c));
	return sortOrder_->operator[](ch);
}

const std::string& SortingPrioritiesTable::GetSortOrderCharacters() const { return sortOrderCharacters_; }
