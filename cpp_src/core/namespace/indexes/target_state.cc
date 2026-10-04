#include "target_state.h"

#include "core/index/index.h"
#include "core/payload/payloadfieldtype.h"

namespace reindexer::ns_indexes {

TargetState::TargetState(const Registry& src)
	: src_{src},
	  plType_{src.GetPayloadType()},
	  tm_{src.GetTagsMatcher()},
	  names_{src.names_},
	  floatVectorsPositions_{src.FloatVectorsPositions()},
	  sparseIndexesCount_{src.Indexes().sparseIndexesSize()} {
	slots_.resize(src.Indexes().totalSize());
	for (int i = 0, total = TotalSize(); i < total; ++i) {
		slots_[i].reusedFrom = i;
	}
}

const Index& TargetState::At(int pos) const& noexcept {
	const auto& slot = slots_[pos];
	return slot.created ? *slot.created : *src_.Indexes()[slot.reusedFrom];
}

Index& TargetState::created(int pos) & noexcept {
	assertrx_dbg(slots_[pos].created);
	return *slots_[pos].created;
}

const FieldsSet& TargetState::FieldsAt(int pos) const& noexcept {
	const auto& slot = slots_[pos];
	return slot.fields ? *slot.fields : At(pos).Fields();
}

bool TargetState::TryGetScalarIndexByName(std::string_view name, int& pos) const noexcept {
	const auto it = names_.find(name);
	if (it == names_.end() || it->second >= FirstCompositePos()) {
		return false;
	}
	pos = it->second;
	return true;
}

bool TargetState::isSparse(const Index& index) noexcept { return !IsComposite(index.Type()) && index.Opts().IsSparse(); }

void TargetState::renumberFieldsRefs(int erasedPos) {
	for (int i = 0, total = TotalSize(); i < total; ++i) {
		if (i == erasedPos) {
			continue;
		}
		const auto& cur = FieldsAt(i);
		FieldsSet renumbered;
		int jsonPathIdx = 0;
		bool changed = false;
		for (int field : cur) {
			if (field == IndexValueType::SetByJsonPath) {
				renumbered.push_back(cur.getJsonPath(jsonPathIdx));
				renumbered.push_back(cur.getTagsPath(jsonPathIdx));
				++jsonPathIdx;
			} else {
				renumbered.push_back(field < erasedPos ? field : field - 1);
				changed = changed || field >= erasedPos;
			}
		}
		if (!changed) {
			continue;
		}
		if (slots_[i].created) {
			slots_[i].created->SetFields(std::move(renumbered));
		} else {
			slots_[i].fields.emplace(std::move(renumbered));
		}
	}
}

void TargetState::Erase(int pos) {
	assertrx_throw(pos >= 0 && pos < TotalSize());
	const bool wasSparse = isSparse(At(pos));
	const bool wasFloatVector = At(pos).IsFloatVector();
	const bool wasRegular = pos < FirstSparsePos();
	const std::string erasedName{At(pos).Name()};

	renumberFieldsRefs(pos);

	for (auto it = names_.begin(); it != names_.end();) {
		if (it->second == pos) {
			it = names_.erase(it);
		} else {
			if (it->second > pos) {
				--it->second;
			}
			++it;
		}
	}

	slots_.erase(slots_.begin() + pos);
	if (wasSparse) {
		--sparseIndexesCount_;
	}
	if (wasRegular) {
		plType_.Drop(erasedName);
	}

	auto it = floatVectorsPositions_.begin();
	const auto end = floatVectorsPositions_.end();
	for (; it != end && *it < size_t(pos); ++it) {
	}
	if (wasFloatVector) {
		assertrx_dbg(it != end && *it == size_t(pos));
		it = floatVectorsPositions_.erase(it);
	}
	for (; it != floatVectorsPositions_.end(); ++it) {
		--*it;
	}
}

void TargetState::Insert(int pos, std::unique_ptr<Index>&& index, const std::string& name, std::optional<PayloadFieldType>&& payloadField) {
	assertrx_throw(pos >= 0 && pos <= TotalSize());
	assertrx_throw(index);
	assertrx_throw(!payloadField || pos == FirstSparsePos());
	const bool newIsPK = bool(index->Opts().IsPK());
	const bool newIsSparse = isSparse(*index);
	const bool newIsFloatVector = index->IsFloatVector();

	for (auto& [_, no] : names_) {
		if (no >= pos) {
			++no;
		}
	}

	slots_.insert(slots_.begin() + pos, Slot{.reusedFrom = -1, .created = std::move(index), .fields = std::nullopt});
	if (newIsSparse) {
		++sparseIndexesCount_;
	}
	if (payloadField) {
		plType_.Add(std::move(*payloadField));
	}
	const auto [_, inserted] = names_.emplace(name, pos);
	(void)inserted;
	assertf(inserted, "[{}] Index '{}' is already registered", plType_.Name(), name);
	if (newIsPK) {
		names_.emplace(kPKIndexName, pos);
	}

	auto it = floatVectorsPositions_.begin();
	auto end = floatVectorsPositions_.end();
	for (; it != end && *it < size_t(pos); ++it) {
	}
	if (newIsFloatVector) {
		it = floatVectorsPositions_.insert(it, pos);
		++it;
		end = floatVectorsPositions_.end();
	}
	for (; it != end; ++it) {
		++*it;
	}
}

void TargetState::Replace(int pos, std::unique_ptr<Index>&& index) {
	assertrx_throw(pos >= 0 && pos < TotalSize());
	assertrx_throw(index);
	assertf(isSparse(At(pos)) == isSparse(*index) && IsComposite(At(pos).Type()) == IsComposite(index->Type()) &&
				At(pos).IsFloatVector() == index->IsFloatVector(),
			"[{}] Attempt to replace index '{}' at position {} with the index of the incompatible kind", plType_.Name(), At(pos).Name(),
			pos);
	slots_[pos].created = std::move(index);
	slots_[pos].fields.reset();
}

void TargetState::UpdateTagsMatcherPayloadType(NeedChangeTmVersion changeVersion) {
	std::vector<SparseIndexData> sparseIndexes;
	sparseIndexes.reserve(sparseIndexesCount_);
	for (int pos = FirstSparsePos(), end = FirstCompositePos(); pos < end; ++pos) {
		const auto& index = At(pos);
		sparseIndexes.emplace_back(index.Name(), index.Type(), index.KeyType(), index.Opts().IsArray(), FieldsAt(pos));
	}
	tm_.updatePayloadType(plType_, sparseIndexes, changeVersion);
}

}  // namespace reindexer::ns_indexes
