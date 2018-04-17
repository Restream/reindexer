#pragma once

#include <array>
#include <memory>
#include <string>
#include "type_consts.h"

namespace reindexer {
struct Slice;
}

/// Sorting priorities table for CollateCustom
/// sorting mode. Input string looks like: "А-ЯA-Z0-9..."
/// and sets sorting priority for charachers in an output.
class SortingPrioritiesTable {
public:
	/// Default constructor.
	SortingPrioritiesTable() = default;
	/// Constructor.
	/// Builds priorities table from UTF-8 string.
	explicit SortingPrioritiesTable(const std::string& sortOrderUTF8);

	/// Returns priority of a character.
	/// @param ch - character.
	/// @return int priority value
	int GetPriority(wchar_t ch) const;

	/// @returns string of sort order characters
	const std::string& GetSortOrderCharacters() const;

private:
	static const uint32_t tableSize = 0x10000;
	using SortOrderTable = std::array<uint16_t, tableSize>;
	using SortOrderTablePtr = std::shared_ptr<SortOrderTable>;
	SortOrderTablePtr sortOrder_;
	std::string sortOrderCharacters_;
};
