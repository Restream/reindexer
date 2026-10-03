#pragma once

#include <vector>
#include "core/ft/idrelset.h"
#include "estl/dynamic_bitset.h"
#include "tools/assertrx.h"

namespace reindexer {

using index_t = uint32_t;

struct [[nodiscard]] FtMergeStatuses {
	using Statuses = DynamicBitset<>;

	static constexpr size_t kEmpty = kEmptyVDocId;
	// Empty bitset means no extern exclusions were applied.
	Statuses docsExcluded;
	// Empty on the regular SelectKey path; sized by PreparePreselectBuffers() before preselect writes.
	std::vector<bool> rowIds;
	const std::vector<uint32_t>* rowId2VdocId = nullptr;
	size_t vdocsCount = 0;

	void PreparePreselectBuffers() {
		assertrx_throw(rowId2VdocId);
		if (docsExcluded.size() == 0) {
			docsExcluded = Statuses(vdocsCount, true);
		}
		if (rowIds.empty()) {
			rowIds.assign(rowId2VdocId->size(), false);
		}
	}
};

using FtPreselectT = FtMergeStatuses;

}  // namespace reindexer
