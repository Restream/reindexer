#pragma once

#include <memory>
#include <span>
#include <vector>
#include "metadata.h"

namespace reindexer {
class Index;
}  // namespace reindexer

namespace reindexer::ns_indexes {

/**
 * @brief Ordered indexes container: the payload fields (starting with '-tuple' at position 0), then the sparse
 * indexes, then the composite ones.
 *
 * The regular/sparse border is derived from the payload type and the sparse indexes count, so the container content
 * and the metadata must always change together - otherwise the accessors below start lying to their callers. Only
 * the Registry holds a mutable reference to this container.
 */
class [[nodiscard]] IndexesStorage final : public std::vector<std::unique_ptr<Index>> {
public:
	using Base = std::vector<std::unique_ptr<Index>>;

	explicit IndexesStorage(const Metadata& md) noexcept : md_(md) {}

	IndexesStorage(const IndexesStorage& src) = delete;
	IndexesStorage& operator=(const IndexesStorage& src) = delete;
	IndexesStorage(IndexesStorage&& src) = delete;
	IndexesStorage& operator=(IndexesStorage&& src) noexcept = delete;

	int regularIndexesSize() const noexcept { return md_.GetPayloadType().NumFields(); }
	int sparseIndexesSize() const noexcept { return sparseIndexesCount_; }
	int compositeIndexesSize() const noexcept { return totalSize() - regularIndexesSize() - sparseIndexesSize(); }
	int firstSparsePos() const noexcept { return md_.GetPayloadType().NumFields(); }
	int firstCompositePos() const noexcept { return md_.GetPayloadType().NumFields() + sparseIndexesCount_; }
	int firstCompositePos(const PayloadType& pt, int sparseIndexes) const noexcept { return pt.NumFields() + sparseIndexes; }
	int totalSize() const noexcept { return size(); }
	std::span<std::unique_ptr<Index>> SparseIndexes() & noexcept {
		return sparseIndexesCount_ ? std::span(&(*this)[firstSparsePos()], sparseIndexesCount_) : std::span<std::unique_ptr<Index>>{};
	}

private:
	friend class Registry;
	friend class TransactionDDL;

	const Metadata& md_;
	int sparseIndexesCount_{0};
};

}  // namespace reindexer::ns_indexes
