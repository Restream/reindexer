#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>
#include "core/cjson/sparse_index_data.h"
#include "core/payload/fieldsset.h"
#include "registry.h"

namespace reindexer {
class Index;
class PayloadFieldType;
}  // namespace reindexer

namespace reindexer::ns_indexes {

/**
 * @brief The target content of the indexes registry, built by the prepare phase of a TransactionDDL.
 *
 * Every check and every throwing operation happens while this object is being built, so a failure simply discards
 * it: the registry, the metadata and the stored items stay untouched. The tags matcher here is a copy, so a failed
 * operation neither bumps its version nor leaks new tags into it.
 *
 * Reuses the source registry's indexes as is (only newly created ones are owned here), so it must not outlive that
 * registry.
 */
class [[nodiscard]] TargetState {
public:
	explicit TargetState(const Registry& src);

	TargetState(const TargetState&) = delete;
	TargetState(TargetState&&) = delete;
	TargetState& operator=(const TargetState&) = delete;
	TargetState& operator=(TargetState&&) = delete;

	const PayloadType& GetPayloadType() const& noexcept { return plType_; }
	const TagsMatcher& GetTagsMatcher() const& noexcept { return tm_; }
	/**
	 * @brief The tags matcher copy being built. Every JSON-path registration goes here, so an aborted operation
	 * leaves the namespace tags matcher untouched.
	 */
	TagsMatcher& GetTagsMatcher() & noexcept { return tm_; }
	int TotalSize() const noexcept { return int(slots_.size()); }
	int FirstSparsePos() const noexcept { return plType_.NumFields(); }
	int FirstCompositePos() const noexcept { return plType_.NumFields() + sparseIndexesCount_; }
	/**
	 * @brief The index taking the given position. Reflects the target fields set only for newly created indexes -
	 * use FieldsAt() for the target fields set at any position.
	 */
	const Index& At(int pos) const& noexcept;
	const FieldsSet& FieldsAt(int pos) const& noexcept;
	/** @brief The target position of a non-composite index with the given name. */
	bool TryGetScalarIndexByName(std::string_view name, int& pos) const noexcept;

	auto GetPayloadType() const&& = delete;
	auto GetTagsMatcher() const&& = delete;
	auto At(int) const&& = delete;
	auto FieldsAt(int) const&& = delete;

	/**
	 * @brief Removes the index at pos, renumbering the names, the float vector positions and the fields sets of the
	 * remaining indexes. Drops the corresponding payload field if the removed index is a regular one.
	 */
	void Erase(int pos);
	/**
	 * @brief Inserts a newly created index at pos, renumbering the names and the float vector positions.
	 * @param payloadField - must be set exactly for regular indexes, i.e. when pos is the first sparse position.
	 */
	void Insert(int pos, std::unique_ptr<Index>&& index, const std::string& name, std::optional<PayloadFieldType>&& payloadField);
	/** @brief Replaces the index at pos with a newly created one describing exactly the same field. */
	void Replace(int pos, std::unique_ptr<Index>&& index);

	/**
	 * @brief Propagates the target payload type and the target sparse indexes into the tags matcher copy. Throws if
	 * the tags matcher has no room for the new tags - which is exactly the point of using a copy.
	 */
	void UpdateTagsMatcherPayloadType(NeedChangeTmVersion changeVersion);

private:
	friend class TransactionDDL;

	/** @brief A single position of the target indexes layout. */
	struct [[nodiscard]] Slot {
		/// Position of the index in the source registry, if the index is reused as is
		int reusedFrom{-1};
		/// The newly created index, which takes this position
		std::unique_ptr<Index> created;
		/// The target fields set of the reused index, if it differs from the current one
		std::optional<FieldsSet> fields;
	};

	// The indexes, which are being created by the current operation and are not filled with the items data yet
	Index& created(int pos) & noexcept;
	void renumberFieldsRefs(int erasedPos);
	static bool isSparse(const Index& index) noexcept;

	const Registry& src_;
	PayloadType plType_;
	TagsMatcher tm_;
	std::vector<Slot> slots_;
	Registry::NamesMap names_;
	std::vector<size_t> floatVectorsPositions_;
	int sparseIndexesCount_{0};
};

}  // namespace reindexer::ns_indexes
