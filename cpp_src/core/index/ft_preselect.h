#pragma once

#include <vector>

namespace reindexer {

using index_t = uint32_t;

struct [[nodiscard]] FtMergeStatuses {
	using Statuses = std::vector<index_t>;

	static constexpr size_t kEmpty = std::numeric_limits<size_t>::max();
	// 0: means not added,
	// kExcluded: means should not be added
	// others: 1 + index of rawResult which added
	enum [[nodiscard]] : index_t { kExcluded = std::numeric_limits<index_t>::max() };
	Statuses statuses;
	std::vector<bool> rowIds;
	const std::vector<size_t>* rowId2Vdoc;
};

using FtPreselectT = FtMergeStatuses;

}  // namespace reindexer
