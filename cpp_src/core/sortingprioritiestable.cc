#include "sortingprioritiestable.h"
#include <algorithm>
#include <cassert>
#include "tools/errors.h"
#include "tools/slice.h"
#include "tools/stringstools.h"

using namespace reindexer;

SortingPrioritiesTable::SortingPrioritiesTable(const std::string& sortOrderUTF8)
	: sortOrder_(std::make_shared<SortOrderTable>()), sortOrderCharacters_(sortOrderUTF8) {
	if (sortOrderCharacters_.empty()) throw Error(errLogic, "Custom sort format string cannot be empty!");

	wchar_t prevCh = 0;
	uint16_t priority = 0;
	uint16_t maxPriority = 0;

	wstring orderUtf16 = reindexer::utf8_to_utf16(sortOrderUTF8);
	const int lastCharIdx = static_cast<int>(orderUtf16.size() - 1);

	for (int i = 0; i <= lastCharIdx; ++i) {
		auto ch(orderUtf16[i]);
		if (ch == '-') {
			if ((i == 0) || (i == lastCharIdx))
				throw Error(errLogic, "Incorrect format of sort order string: '-' cannot be the first or the last character");
		} else {
			if ((i != 0) && (orderUtf16[i - 1] == '-')) {
				if (ch <= prevCh) throw Error(errLogic, "Incorrect format of sort order string: range should be ascending");
				for (auto it = prevCh; it <= ch; ++it) {
					if (checkForRangeIntersection(it))
						throw Error(errLogic, "There can't be 2 same formating characters in format string!");
					sortOrder_->operator[](it) = priority++;
				}
				ranges_.insert({prevCh, ch - prevCh + 1});
				maxPriority = priority;
			} else if (((i + 1 <= lastCharIdx) && (orderUtf16[i + 1] != '-')) || (i == lastCharIdx)) {
				if (checkForRangeIntersection(ch)) throw Error(errLogic, "There can't be 2 same formating characters in format string!");
				sortOrder_->operator[](ch) = priority++;
				ranges_.insert({ch, 1});
				maxPriority = priority;
			}
			prevCh = ch;
		}
	}

	if (!ranges_.empty()) {
		auto rangeIt = ranges_.begin();
		uint16_t outOfRangePriority = maxPriority;
		for (size_t i = 0; i < tableSize;) {
			if ((rangeIt != ranges_.end()) && (rangeIt->first == i)) {
				i += rangeIt->second;
				++rangeIt;
			} else {
				sortOrder_->operator[](i++) = outOfRangePriority++;
			}
		}
	}
}

bool SortingPrioritiesTable::checkForRangeIntersection(wchar_t ch) {
	if (ranges_.empty()) return false;
	auto itLow = ranges_.lower_bound(ch);
	if (itLow == ranges_.end()) itLow = ranges_.begin();
	auto itUp = ranges_.upper_bound(ch);
	for (auto it = itLow; it != itUp; ++it) {
		if ((ch >= it->first) && (ch < it->first + it->second)) return true;
	}
	return false;
}

int SortingPrioritiesTable::GetPriority(wchar_t c) const {
	assert(sortOrder_.get() != nullptr);
	// assert(static_cast<uint32_t>(c) < tableSize);
	uint16_t ch(static_cast<uint16_t>(c));
	return sortOrder_->operator[](ch);
}

const std::string& SortingPrioritiesTable::GetSortOrderCharacters() const { return sortOrderCharacters_; }
