#pragma once

#include <array>
#include <map>
#include <string>
#include "estl/intrusive_ptr.h"
#include "tools/assertrx.h"

namespace reindexer {

/// Sorting priorities table for CollateCustom
/// sorting mode. Input string looks like: "А-ЯA-Z0-9..."
/// and sets sorting priority for charachers in an output.
class [[nodiscard]] SortingPrioritiesTable {
public:
	/// Default constructor.
	SortingPrioritiesTable() = default;
	/// Constructor.
	/// Builds priorities table from UTF-8 string.
	explicit SortingPrioritiesTable(const std::string& sortOrderUTF8);

	/// Returns priority of a character.
	/// @param c - character
	/// @returns int priority value
	int GetPriority(wchar_t c) const noexcept {
		assertrx(sortOrder_.get() != nullptr);
		// assertrx(static_cast<uint32_t>(c) < tableSize);
		uint16_t ch(static_cast<uint16_t>(c));
		return sortOrder_->operator[](ch);
	}

	/// @returns string of sort order characters
	const std::string& GetSortOrderCharacters() const noexcept { return sortOrderCharacters_; }

private:
	/// Checks whether ch is in existing ranges ir not.
	/// @param ch - character to check.
	/// @param ranges - map with character's ranges
	/// @returns true, if character is in one of existing ranges already.
	bool checkForRangeIntersection(std::map<uint16_t, uint16_t>& ranges, wchar_t ch);

	constexpr static uint32_t kTableSize = 0x10000;
	using SortOrderTable = intrusive_atomic_rc_wrapper<std::array<uint16_t, kTableSize>>;
	using SortOrderTablePtr = intrusive_ptr<SortOrderTable>;
	SortOrderTablePtr sortOrder_;
	std::string sortOrderCharacters_;
};

}  // namespace reindexer
