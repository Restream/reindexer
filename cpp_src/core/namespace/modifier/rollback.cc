#include "rollback.h"
#include <algorithm>
#include <tuple>
#include "core/formatters/namespacesname_fmt.h"
#include "core/index/index.h"
#include "core/index/indexing_condition.h"
#include "core/item_tree/intermediate_indexes_data.h"
#include "core/namespace/index_cache_cleaner.h"
#include "core/namespace/indexes/indexes_storage.h"
#include "core/namespace/stringsholder.h"
#include "tools/logger.h"

namespace reindexer::ns_item_modify {

void RollbackState::Reset(const PayloadValue& pv) {
	pvSave_ = pv;
	pvSave_.Clone();
	std::fill(data_.begin(), data_.end(), IndexUpdateState::Unchanged);
	for (VariantArray& keys : insertedKeys_) {
		keys.clear();
	}
	pkModified_ = false;
}

// NOLINTNEXTLINE(bugprone-exception-escape) Termination here is better, than inconsistent state of the user's data
void RollbackGuard::rollback() noexcept {
	item_.Clone();
	IndexesCacheCleaner indexesCacheCleaner{indexes_};
	removeInsertedComposites(indexesCacheCleaner);
	restoreIndexes(indexesCacheCleaner);
	restoreTuple();
	restoreComposites(indexesCacheCleaner);
}

void RollbackGuard::removeInsertedComposites(IndexesCacheCleaner& indexesCacheCleaner) {
	const auto& data = rollbackState_.IndexStatus();
	const auto& indexes = indexes_;
	for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()) && i < data.size(); ++i) {
		if (data[i] == IndexUpdateState::NewValueInserted) {
			bool needClearCache{false};
			indexes[i]->Delete(Variant(item_), itemId_, MustExist_False, strHolder_, needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(*indexes[i]);
			}
		}
	}
}

void RollbackGuard::restoreIndexes(IndexesCacheCleaner& indexesCacheCleaner) {
	const auto& data = rollbackState_.IndexStatus();
	const auto& indexes = indexes_;

	for (size_t i = 1; i < data.size() && i < size_t(indexes.firstCompositePos()); i++) {
		if (data[i] != IndexUpdateState::Unchanged) {
			bool needClearCache{false};
			if (data[i] == IndexUpdateState::NewValueInserted) {
				const VariantArray& vals = rollbackState_.InsertedKeys(i);
				indexes[i]->Delete(vals, itemId_, MustExist_False, strHolder_, needClearCache);
				if (needClearCache) {
					indexesCacheCleaner.Add(*indexes[i]);
				}
			}

			const VariantArray& oldData = indexData_.IndexDataValue(i);
			VariantArray result;
			indexes[i]->Upsert(result, oldData, itemId_, verdicts_[static_cast<int>(i)], needClearCache);
			if (!indexes[i]->Opts().IsSparse()) {
				Payload pl{payloadType_, item_};
				try {
					pl.Set(i, result);
				} catch (const Error& err) {
					assertf(false, "[{}]: Failed to set index {} value: {}", nsName_, i, err.what());
					std::abort();
				}
			}
			if (needClearCache) {
				indexesCacheCleaner.Add(*indexes[i]);
			}
		}
	}
}

void RollbackGuard::restoreTuple() {
	const auto& indexes = indexes_;
	Payload plCur(payloadType_, item_);

	const IndexUpdateState tupleState = rollbackState_.IndexStatus()[0];
	if (tupleState != IndexUpdateState::Unchanged) {
		bool needClearCache{false};
		VariantArray res;
		if (tupleState == IndexUpdateState::NewValueInserted) {
			indexes[0]->Delete(indexData_.IndexDataValueNew(0), itemId_, MustExist_True, strHolder_, needClearCache);
		}
		indexes[0]->Upsert(res, indexData_.IndexDataValue(0), itemId_, needClearCache);
		try {
			plCur.Set(0, res);
		} catch (const Error& err) {
			assertf(false, "[{}]: Failed to set cjson tuple: {}", nsName_, err.what());
			std::abort();
		}
	}
}

void RollbackGuard::restoreComposites(IndexesCacheCleaner& indexesCacheCleaner) {
	const auto& data = rollbackState_.IndexStatus();
	const auto& indexes = indexes_;

	// Insert current payload value into affected composite indexes or refresh value in unaffected indexes
	const Variant pvCurVariant{item_};
	for (size_t i = indexes.firstCompositePos(); i < size_t(indexes.totalSize()); ++i) {
		if (data[i] != IndexUpdateState::Unchanged) {
			bool needClearCache{false};
			std::ignore = indexes[i]->Upsert(pvCurVariant, itemId_, verdicts_[static_cast<int>(i)], needClearCache);
			if (needClearCache) {
				indexesCacheCleaner.Add(*indexes[i]);
			}
		} else {
			bool refreshed = indexes[i]->RefreshCompositeKey(pvCurVariant, itemId_);
			assertrx_dbg(refreshed);
			if (!refreshed) [[unlikely]] {
				logFmt(LogError, "[{}]: Unable to refresh key for {} during update query", nsName_, indexes[i]->Name());
			}
		}
	}
}

}  // namespace reindexer::ns_item_modify
