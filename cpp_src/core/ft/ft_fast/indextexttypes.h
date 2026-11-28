#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include "estl/fast_hash_map.h"
#include "tools/assertrx.h"

namespace reindexer {

using WordIdType = uint32_t;

template <typename IdCont>
struct [[nodiscard]] WordOccurences {
	std::shared_ptr<IdCont> ids;
	uint64_t deletedDocsAtCleanup = 0;

	double EstimateNumLiveDocs(size_t liveDocs, uint64_t deletedDocs) const noexcept {
		assertrx_dbg(deletedDocs >= deletedDocsAtCleanup);
		if (liveDocs == 0 || ids->empty()) {
			return 0.0;
		}
		// Estimate df assuming deletions are distributed uniformly across words.
		// N = LiveDocs + D, so N - D1 = LiveDocs + (D - D1).
		const double liveDocsCount = static_cast<double>(liveDocs);
		const double totalDocs = liveDocsCount + static_cast<double>(deletedDocs - deletedDocsAtCleanup);
		const double estimatedDocs = static_cast<double>(ids->size()) * (liveDocsCount / totalDocs);
		return std::clamp(estimatedDocs, 1.0, liveDocsCount);
	}
};

using FoundWordsProcsType = fast_hash_map<WordIdType, float>;
using FoundWordsType = fast_hash_map<WordIdType, size_t>;

enum class [[nodiscard]] FtUseExternStatuses : bool { Yes, No };

}  // namespace reindexer
