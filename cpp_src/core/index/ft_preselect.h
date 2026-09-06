#pragma once

#include <vector>
#include "estl/dynamic_bitset.h"

namespace reindexer {

using index_t = uint32_t;

struct [[nodiscard]] FtMergeStatuses {
	using Statuses = DynamicBitset<>;

	static constexpr size_t kEmpty = std::numeric_limits<uint32_t>::max();
	Statuses docsExcluded;
	std::vector<bool> rowIds;
	const std::vector<uint32_t>* rowId2Vdoc;
};

using FtPreselectT = FtMergeStatuses;

}  // namespace reindexer
