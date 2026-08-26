#include "registry.h"

#include <algorithm>
#include "core/index/index.h"
#include "core/payload/payloadfieldtype.h"

namespace reindexer::ns_indexes {

Registry::Registry(const std::string& nsName, int32_t tmStateToken) : md_{nsName, tmStateToken}, indexes_{md_} {}

Registry::Registry(const Registry& src, size_t newItemsCapacity)
	: md_{src.md_}, indexes_{md_}, names_{src.names_}, composites_{src.composites_}, floatVectorsPositions_{src.floatVectorsPositions_} {
	indexes_.sparseIndexesCount_ = src.indexes_.sparseIndexesCount_;
	indexes_.reserve(src.indexes_.size());
	for (const auto& idx : src.indexes_) {
		indexes_.emplace_back(idx->Clone(newItemsCapacity, IndexCloneKind::Logical));
	}
}

Registry::~Registry() = default;

std::unique_ptr<Index> Registry::ReplaceIndex(int pos, std::unique_ptr<Index>&& newIndex) {
	assertrx_throw(pos >= 0 && pos < indexes_.totalSize());
	auto& oldIndex = *indexes_[pos];
	assertrx_throw(newIndex);
	assertf(oldIndex.Name() == newIndex->Name(), "[{}] Attempt to replace index '{}' at position {} with the index '{}'", nsName(),
			oldIndex.Name(), pos, newIndex->Name());
	assertf(IsComposite(oldIndex.Type()) == IsComposite(newIndex->Type()) &&
				bool(oldIndex.Opts().IsSparse()) == bool(newIndex->Opts().IsSparse()) &&
				oldIndex.IsFloatVector() == newIndex->IsFloatVector(),
			"[{}] Attempt to replace index '{}' at position {} with the index of the incompatible kind", nsName(), oldIndex.Name(), pos);

	std::swap(indexes_[pos], newIndex);
	return std::move(newIndex);
}

void Registry::ReplaceFieldType(int pos, PayloadFieldType&& fieldType) {
	assertrx_throw(pos >= 0 && pos < indexes_.firstSparsePos());
	assertf(md_.plType_.Field(pos).Name() == fieldType.Name() && md_.plType_.Field(pos).Offset() == fieldType.Offset(),
			"[{}] Attempt to replace the payload field '{}' at position {} with the field '{}'", nsName(), md_.plType_.Field(pos).Name(),
			pos, fieldType.Name());

	md_.plType_.Replace(pos, std::move(fieldType));
	PropagatePayloadType();
	md_.tagsMatcher_.updatePayloadType(md_.plType_, indexes_.SparseIndexes(), NeedChangeTmVersion::No);
}

void Registry::PropagatePayloadType() {
	for (auto& idx : indexes_) {
		idx->UpdatePayloadType(PayloadType{md_.plType_});
	}
}

void Registry::RenamePayloadType(std::string_view name) {
	md_.plType_.SetName(name);
	md_.tagsMatcher_.updatePayloadType(md_.plType_, indexes_.SparseIndexes(), NeedChangeTmVersion::No);
}

void Registry::ReplaceTagsMatcher(TagsMatcher&& tm) {
	md_.tagsMatcher_ = std::move(tm);
	md_.tagsMatcher_.updatePayloadType(md_.plType_, indexes_.SparseIndexes(), NeedChangeTmVersion::No);
	md_.tagsMatcher_.setUpdated();
}

void Registry::RebuildCompositesMapping() noexcept {
	// The only possible exception here is bad_alloc, but required memory footprint is tiny
	const auto beg = indexes_.firstCompositePos();
	const auto end = beg + indexes_.compositeIndexesSize();
	std::optional<CompositesMap> composites;
	for (auto i = beg; i < end; ++i) {
		const auto& index = indexes_[i];
		assertrx(IsComposite(index->Type()));
		const auto& fields = index->Fields();
		for (auto field : fields) {
			try {
				if (!composites.has_value()) {
					composites.emplace();
				}
				composites.value()[field].emplace_back(i);
			} catch (...) {
				// Termination here is better than inconsistent state of the indexes
				std::terminate();
			}
		}
	}
	if (composites.has_value()) {
		composites_ = std::move(*composites);
	}
}

void Registry::LeakIndexes() noexcept {
	for (auto& idx : indexes_) {
		idx.release();	// NOLINT(bugprone-unused-return-value)
	}
}

void Registry::DestroyIndex(size_t pos) noexcept { indexes_[pos].reset(); }

template <Registry::ConsistencyState consistencyState>
void Registry::CheckConsistency() const noexcept(consistencyState == ConsistencyState::AfterModification) {
#define assertf_or_assertrx(condition, ...)                                       \
	{                                                                             \
		if constexpr (consistencyState == ConsistencyState::BeforeModification) { \
			assertrx_throw(condition);                                            \
		} else {                                                                  \
			assertf(condition, __VA_ARGS__);                                      \
		}                                                                         \
	}

	std::string_view step;
	switch (consistencyState) {
		case ConsistencyState::BeforeModification:
			step = "before the index modification";
			break;
		case ConsistencyState::AfterModification:
			step = "after the index modification";
			break;
		default:
			assertrx(false);
	}

	const auto& name = nsName();
	const int total = indexes_.totalSize();
	if constexpr (consistencyState == ConsistencyState::BeforeModification) {
		if (total == 0) {
			// Only true right before the very first AddIndex() call from the NamespaceImpl constructor - trivially
			// consistent, there is nothing to cross-check yet. Once the tuple index exists, it can never legitimately
			// disappear again (see NamespaceImpl::verifyDropIndex), so this is not given the same pass below
			return;
		}
	}
	const int firstSparse = md_.GetPayloadType().NumFields();
	const int firstComposite = firstSparse + indexes_.sparseIndexesSize();

	assertf_or_assertrx(total > 0, "[{}] {}: namespace has no indexes at all", name, step);
	assertf_or_assertrx(indexes_[0]->Name() == kTupleName, "[{}] {}: expecting tuple index at position 0, but got '{}'", name, step,
						indexes_[0]->Name());
	assertf_or_assertrx(firstComposite <= total, "[{}] {}: {} regular and {} sparse indexes do not fit into {} indexes", name, step,
						firstSparse, indexes_.sparseIndexesSize(), total);

	int sparseCount = 0;
	int pkCount = 0;
	size_t floatVectorsCount = 0;
	for (int i = 0; i < total; ++i) {
		const auto& idx = *indexes_[i];
		const bool isComposite = IsComposite(idx.Type());
		assertf_or_assertrx(isComposite == (i >= firstComposite),
							"[{}] {}: index '{}' at position {} is {}composite, but positions [{}, {}) are reserved for the composites",
							name, step, idx.Name(), i, isComposite ? "" : "not ", firstComposite, total);
		if (isComposite) {
			assertf_or_assertrx(!bool(idx.Opts().IsSparse()), "[{}] {}: composite index '{}' at position {} is marked sparse", name, step,
								idx.Name(), i);
			for (int field : idx.Fields()) {
				assertf_or_assertrx(field == IndexValueType::SetByJsonPath || (field >= 0 && field < firstComposite),
									"[{}] {}: composite index '{}' refers to the unexpected field {}", name, step, idx.Name(), field);
				if (field != IndexValueType::SetByJsonPath) {
					const auto compIt = composites_.find(field);
					assertf_or_assertrx(
						compIt != composites_.end() && std::find(compIt->second.begin(), compIt->second.end(), i) != compIt->second.end(),
						"[{}] {}: composite index '{}' at position {} refers to the field {}, but composites_ does not map it back", name,
						step, idx.Name(), i, field);
				}
			}
		} else {
			const bool isSparse = bool(idx.Opts().IsSparse());
			assertf_or_assertrx(
				isSparse == (i >= firstSparse),
				"[{}] {}: index '{}' at position {} is {}sparse, but positions [{}, {}) are reserved for the sparse indexes", name, step,
				idx.Name(), i, isSparse ? "" : "not ", firstSparse, firstComposite);
			if (isSparse) {
				++sparseCount;
			} else {
				assertf_or_assertrx(md_.GetPayloadType().Field(i).Name() == idx.Name(),
									"[{}] {}: payload field {} is '{}', but the corresponding index is '{}'", name, step, i,
									md_.GetPayloadType().Field(i).Name(), idx.Name());
				assertf_or_assertrx(idx.Fields().contains(i), "[{}] {}: index '{}' at position {} does not refer to its own payload field",
									name, step, idx.Name(), i);
			}
		}
		if (idx.IsFloatVector()) {
			assertf_or_assertrx(floatVectorsCount < floatVectorsPositions_.size() && floatVectorsPositions_[floatVectorsCount] == size_t(i),
								"[{}] {}: float vector index '{}' at position {} is missing in the float vector positions", name, step,
								idx.Name(), i);
			++floatVectorsCount;
		}
		if (idx.Opts().IsPK()) {
			++pkCount;
		}
		const auto nameIt = names_.find(idx.Name());
		assertf_or_assertrx(nameIt != names_.end(), "[{}] {}: index '{}' at position {} is missing in the names map", name, step,
							idx.Name(), i);
		assertf_or_assertrx(nameIt->second == i, "[{}] {}: index '{}' is stored at position {}, but the names map points to {}", name, step,
							idx.Name(), i, nameIt->second);
	}

	assertf_or_assertrx(sparseCount == indexes_.sparseIndexesSize(),
						"[{}] {}: sparse indexes count is {}, but {} sparse indexes were found", name, step, indexes_.sparseIndexesSize(),
						sparseCount);
	assertf_or_assertrx(floatVectorsCount == floatVectorsPositions_.size(),
						"[{}] {}: float vector positions hold {} entries, but {} float vector indexes were found", name, step,
						floatVectorsPositions_.size(), floatVectorsCount);
	assertf_or_assertrx(pkCount <= 1, "[{}] {}: {} PK indexes were found", name, step, pkCount);

	const auto pkIt = names_.find(kPKIndexName);
	assertf_or_assertrx((pkIt != names_.end()) == (pkCount == 1),
						"[{}] {}: '{}' alias is {}present in the names map, but {} PK indexes were found", name, step, kPKIndexName,
						pkIt != names_.end() ? "" : "not ", pkCount);

	for (const auto& [idxName, no] : names_) {
		assertf_or_assertrx(no >= 0 && no < total, "[{}] {}: the names map maps '{}' to the out of range position {} ({} indexes total)",
							name, step, idxName, no, total);
		if (idxName == kPKIndexName) {
			assertf_or_assertrx(bool(indexes_[no]->Opts().IsPK()), "[{}] {}: '{}' alias points to the non-PK index '{}'", name, step,
								kPKIndexName, indexes_[no]->Name());
		} else {
			assertf_or_assertrx(indexes_[no]->Name() == idxName, "[{}] {}: the names map maps '{}' to the index '{}'", name, step, idxName,
								indexes_[no]->Name());
		}
	}

	for (const auto& [field, compositePositions] : composites_) {
		if (field == IndexValueType::SetByJsonPath) {
			continue;
		}
		assertf_or_assertrx(field >= 0 && field < firstComposite,
							"[{}] {}: composites_ maps the out of range field {} to some composite indexes", name, step, field);
		for (int compositePos : compositePositions) {
			const bool posInRange = compositePos >= firstComposite && compositePos < total;
			assertf_or_assertrx(posInRange, "[{}] {}: composites_[{}] points to the out of range position {}", name, step, field,
								compositePos);
			if (posInRange) {
				assertf_or_assertrx(
					indexes_[compositePos]->Fields().contains(field),
					"[{}] {}: composites_[{}] points to the composite index '{}' at position {}, which does not refer to that field", name,
					step, field, indexes_[compositePos]->Name(), compositePos);
			}
		}
	}

	{
		const auto& sparseIndexes = md_.GetTagsMatcher().SparseIndexes();
		assertf_or_assertrx(sparseIndexes.size() == size_t(sparseCount),
							"[{}] {}: tags matcher holds {} sparse indexes, but the registry has {}", name, step, sparseIndexes.size(),
							sparseCount);
		for (int i = firstSparse; i < firstComposite; ++i) {
			const auto& idxName = indexes_[i]->Name();
			const bool found = std::find_if(sparseIndexes.begin(), sparseIndexes.end(),
											[&idxName](const auto& sd) { return sd.name == idxName; }) != sparseIndexes.end();
			assertf_or_assertrx(found, "[{}] {}: sparse index '{}' at position {} is missing from the tags matcher", name, step, idxName,
								i);
		}
	}
#undef assertf_or_assertrx
}

template void Registry::CheckConsistency<Registry::ConsistencyState::BeforeModification>() const;
template void Registry::CheckConsistency<Registry::ConsistencyState::AfterModification>() const noexcept;
}  // namespace reindexer::ns_indexes
