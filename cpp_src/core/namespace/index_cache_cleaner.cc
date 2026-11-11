#include "index_cache_cleaner.h"
#include "core/index/index.h"

namespace reindexer {

void IndexesCacheCleaner::Add(Index& idx) noexcept {
	// Each ordered index may affect SortOrders of the other indexes
	requiresCleanup_ = requiresCleanup_ || idx.IsOrdered();
}

IndexesCacheCleaner::~IndexesCacheCleaner() {
	if (requiresCleanup_) {
		for (auto& idx : indexes_) {
			if (idx->IsSupportSortedIdsBuild()) {
				idx->ClearCache();
			}
		}
	}
}

}  // namespace reindexer
