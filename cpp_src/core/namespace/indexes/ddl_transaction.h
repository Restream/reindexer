#pragma once

#include "core/id_type.h"
#include "core/namespace/namespacestat.h"
#include "core/payload/payloadvalue.h"
#include "target_state.h"

namespace reindexer {

class IndexDef;
class NamespaceImpl;
class Recoder;

}  // namespace reindexer

namespace reindexer::ns_indexes {

/**
 * @brief Single entry point for indexes-registry modifications.
 *
 * Add/drop/update also touches names, sparse count, float-vector positions, composites mapping, payload type, tags
 * matcher and items - all must stay consistent.
 *
 * Short-lived, one op under the namespace write lock. Two phases:
 *  - prepare builds TargetState aside and recodes items. Checks and throws live here, so a failure leaves registry,
 *    metadata and items as they were. Not rolled back on a failed prepare (pre-existing, not introduced here):
 *    embedding-config verify (verifyUpdateIndex()/prepareAdd()) may register a JSON-path in EmbeddersCache; recoding
 *    the tuple (prepareItems()) releases old string refs from StringsHolder per item before success is known;
 *  - commit swaps the prepared state into the registry (noexcept).
 *
 * Covers registry, metadata and items - not PK migration (migrations::PKMigrationService), which writes storage after
 * metadata is already fixed. It does have its own (imperfect) recovery mechanism - see migrations::PKMigrationService
 * for the guarantees.
 */
class [[nodiscard]] TransactionDDL {
public:
	explicit TransactionDDL(NamespaceImpl& ns) noexcept;

	TransactionDDL(const TransactionDDL&) = delete;
	TransactionDDL(TransactionDDL&&) = delete;
	TransactionDDL& operator=(const TransactionDDL&) = delete;
	TransactionDDL& operator=(TransactionDDL&&) = delete;

	void AddIndex(const IndexDef& indexDef, bool disableTmVersionInc, bool skipEqualityCheck);
	void DropIndex(const IndexDef& indexDef, bool disableTmVersionInc);
	/** @brief Returns false if the index is already in the requested state. */
	bool UpdateIndex(const IndexDef& indexDef, bool disableTmVersionInc);

private:
	struct [[nodiscard]] Operation {
		const IndexDef* toDrop{nullptr};
		const IndexDef* toAdd{nullptr};
		bool disableTmVersionInc{false};
	};

	struct [[nodiscard]] AddedIndex {
		int pos{-1};
		bool sparse{false};
		std::string_view jsonPath;
	};

	/** @brief What exactly happened to the payload fields:
	 *  this defines whether the stored items
	 *  have to be recoded and which recoder does the job
	 */
	struct [[nodiscard]] PayloadFieldsChange {
		/// Position of the dropped field in the source payload type
		int droppedPos{-1};
		/// Position of the added field in the target payload type
		int addedPos{-1};

		bool Any() const noexcept { return droppedPos >= 0 || addedPos >= 0; }
	};

	void apply(const Operation& op);
	int prepareDrop(TargetState& st, const IndexDef& indexDef, PayloadFieldsChange& change);
	AddedIndex prepareAdd(TargetState& st, const IndexDef& indexDef, PayloadFieldsChange& change);
	void prepareComposites(TargetState& st, const PayloadFieldsChange& change);
	void prepareItems(TargetState& st, const PayloadFieldsChange& change, const PayloadType& afterDropPlType, const AddedIndex& added);
	void fillAddedIndex(TargetState& st, const AddedIndex& added);
	std::unique_ptr<Recoder> makeRecoder(const TargetState& st, const PayloadFieldsChange& change) const;
	uint64_t itemChecksum(const TargetState& st, const PayloadType& pt, const PayloadValue& pv, IdType rowId) const noexcept;
	void rewriteStorage() const;

	void commit(TargetState& st, int droppedSrcPos) noexcept;

	NamespaceImpl& ns_;
	Registry& registry_;

	std::vector<std::pair<IdType, PayloadValue>> newItems_;
	std::vector<std::unique_ptr<Index>> newIndexes_;
	uint64_t newChecksum_;
	size_t newItemsDataSize_{0};
	bool itemsRecoded_{false};
	bool rewriteStorage_{false};
	std::unique_ptr<Index> droppedIndex_;
	std::vector<std::unique_ptr<Index>> replacedIndexes_;
};

}  // namespace reindexer::ns_indexes
