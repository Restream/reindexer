#include "sortingprioritiestable.h"
#include <algorithm>
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {

SortingPrioritiesTable::SortingPrioritiesTable(const std::string& sortOrderUTF8)
	: sortOrder_(make_intrusive<SortOrderTable>()), sortOrderCharacters_(sortOrderUTF8) {
	if (sortOrderCharacters_.empty()) {
		throw Error(errLogic, "Custom sort format string cannot be empty!");
	}

	wchar_t prevCh = 0;
	uint16_t priority = 0;
	uint16_t maxPriority = 0;
	std::map<uint16_t, uint16_t> ranges;

	std::wstring orderUtf16 = reindexer::utf8_to_utf16(sortOrderUTF8);
	const int lastCharIdx = static_cast<int>(orderUtf16.size() - 1);

	for (int i = 0; i <= lastCharIdx; ++i) {
		auto ch(orderUtf16[i]);
		if (ch == '-') {
			if ((i == 0) || (i == lastCharIdx)) {
				throw Error(errLogic, "Incorrect format of sort order string: '-' cannot be the first or the last character");
			}
		} else {
			if ((i != 0) && (orderUtf16[i - 1] == '-')) {
				if (ch <= prevCh) {
					throw Error(errLogic, "Incorrect format of sort order string: range should be ascending");
				}
				for (auto it = prevCh; it <= ch; ++it) {
					if (checkForRangeIntersection(ranges, it)) {
						throw Error(errLogic, "There can't be 2 same formating characters in format string!");
					}
					sortOrder_->operator[](it) = priority++;
				}
				ranges.insert({prevCh, ch - prevCh + 1});
				maxPriority = priority;
			} else if (((i + 1 <= lastCharIdx) && (orderUtf16[i + 1] != '-')) || (i == lastCharIdx)) {
				if (checkForRangeIntersection(ranges, ch)) {
					throw Error(errLogic, "There can't be 2 same formating characters in format string!");
				}
				sortOrder_->operator[](ch) = priority++;
				ranges.insert({ch, 1});
				maxPriority = priority;
			}
			prevCh = ch;
		}
	}

	if (!ranges.empty()) {
		auto rangeIt = ranges.begin();
		uint16_t outOfRangePriority = maxPriority;
		for (size_t i = 0; i < kTableSize;) {
			if ((rangeIt != ranges.end()) && (rangeIt->first == i)) {
				i += rangeIt->second;
				++rangeIt;
			} else {
				sortOrder_->operator[](i++) = outOfRangePriority++;
			}
		}
	}
}

bool SortingPrioritiesTable::checkForRangeIntersection(std::map<uint16_t, uint16_t>& ranges, wchar_t ch) {
	if (ranges.empty()) {
		return false;
	}
	auto itLow = ranges.lower_bound(ch);
	if (itLow == ranges.end()) {
		itLow = ranges.begin();
	}
	auto itUp = ranges.upper_bound(ch);
	for (auto it = itLow; it != itUp; ++it) {
		if ((ch >= it->first) && (ch < it->first + it->second)) {
			return true;
		}
	}
	return false;
}

}  // namespace reindexer
