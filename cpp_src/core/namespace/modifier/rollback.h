#pragma once

#include <utility>
#include <vector>
#include "core/enums.h"
#include "core/id_type.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadvalue.h"

namespace reindexer {

class IndexesCacheCleaner;
class IndexingVerdicts;
class IntermediateIndexesData;
class NamespaceName;
class PayloadType;
class StringsHolder;

namespace ns_indexes {
class IndexesStorage;
}

namespace ns_item_modify {

enum class [[nodiscard]] IndexUpdateState { Unchanged, OldValueDeleted, NewValueInserted };

class [[nodiscard]] RollbackState {
public:
	explicit RollbackState(int indexCount) : data_(indexCount), insertedKeys_(indexCount) {}
	void Reset(const PayloadValue& pv);
	void SetOldValueDeleted(size_t index, IsPk isPk) noexcept {
		assertrx_dbg(index < data_.size());
		assertrx_dbg(data_[index] == IndexUpdateState::Unchanged || data_[index] == IndexUpdateState::NewValueInserted);
		data_[index] = IndexUpdateState::OldValueDeleted;
		pkModified_ = pkModified_ || isPk;
	}
	void SetNewValueInserted(size_t index, IsPk isPk) noexcept {
		assertrx_dbg(index < data_.size());
		assertrx_dbg(data_[index] == IndexUpdateState::OldValueDeleted);
		data_[index] = IndexUpdateState::NewValueInserted;
		pkModified_ = pkModified_ || isPk;
	}
	void SetNewValueInserted(size_t index, IsPk isPk, VariantArray&& insertedKeys) noexcept {
		assertrx_dbg(index < insertedKeys_.size());
		insertedKeys_[index] = std::move(insertedKeys);
		SetNewValueInserted(index, isPk);
	}
	const PayloadValue& GetPayloadValueBackup() const noexcept { return pvSave_; }
	const std::vector<IndexUpdateState>& IndexStatus() const noexcept { return data_; }
	const VariantArray& InsertedKeys(size_t index) const noexcept {
		assertrx_dbg(index < insertedKeys_.size());
		return insertedKeys_[index];
	}
	bool IsPkModified() const noexcept { return pkModified_; }

private:
	std::vector<IndexUpdateState> data_;
	std::vector<VariantArray> insertedKeys_;
	PayloadValue pvSave_;
	bool pkModified_ = false;
};

// The item, index storage, saved data and verdicts must outlive the guard and keep their identities until it is destroyed.
class [[nodiscard]] RollbackGuard {
public:
	RollbackGuard(const RollbackState& rollbackState, IntermediateIndexesData& indexData, const ns_indexes::IndexesStorage& indexes,
				  PayloadValue& item, const PayloadType& payloadType, const NamespaceName& nsName, StringsHolder& strHolder, IdType id,
				  const IndexingVerdicts& verdicts) noexcept
		: rollbackState_{rollbackState},
		  indexData_{indexData},
		  indexes_{indexes},
		  item_{item},
		  payloadType_{payloadType},
		  nsName_{nsName},
		  strHolder_{strHolder},
		  itemId_{id},
		  verdicts_{verdicts} {}
	~RollbackGuard() {
		if (!disabled_) {
			rollback();
		}
	}

	void Disable() noexcept { disabled_ = true; }

	RollbackGuard(const RollbackGuard&) = delete;
	RollbackGuard(RollbackGuard&&) = delete;
	RollbackGuard& operator=(const RollbackGuard&) = delete;
	RollbackGuard& operator=(RollbackGuard&&) = delete;

private:
	void rollback() noexcept;
	void removeInsertedComposites(IndexesCacheCleaner&);
	void restoreIndexes(IndexesCacheCleaner&);
	void restoreTuple();
	void restoreComposites(IndexesCacheCleaner&);

	const RollbackState& rollbackState_;
	IntermediateIndexesData& indexData_;
	const ns_indexes::IndexesStorage& indexes_;
	PayloadValue& item_;
	const PayloadType& payloadType_;
	const NamespaceName& nsName_;
	StringsHolder& strHolder_;
	IdType itemId_;
	const IndexingVerdicts& verdicts_;
	bool disabled_ = false;
};

}  // namespace ns_item_modify
}  // namespace reindexer
