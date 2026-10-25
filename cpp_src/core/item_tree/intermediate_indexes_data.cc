#include "intermediate_indexes_data.h"

#include "core/index/index.h"
#include "core/payload/payloadfieldvalue.h"
#include "item_tree.h"

namespace reindexer {

void IntermediateIndexesData::SaveOldIndexData(size_t index, ConstPayload& plSave, TreeNode& root) {
	assertrx_throw(index < data_.size());
	if (index == 0) {
		return;
	}
	if (data_[index].isChanged) {
		return;
	}
	auto& data = data_[index];
	if (!data.oldDataMaterialized) {
		data.oldData = GetIndexData<VariantArray>(root, *indexes_[index], tm_, plSave);
		const auto& idx = *indexes_[index];
		if (data.oldData.empty() && !idx.Opts().IsArray() && !idx.Opts().IsSparse()) {
			data.oldData.emplace_back(PayloadFieldValue::GetDefaultValue(idx.KeyType()));
		}
	}
	data.isChanged = true;
}

void IntermediateIndexesData::SetNewIndexData(size_t i, IndexNodeData&& val, Index* index) {
	assertrx_throw(i < data_.size());
	if (i == 0) {
		return;
	}
	data_[i].newData = std::move(val);
	for (auto& v : data_[i].newData.Vals) {
		std::ignore = v.convert(index->KeyType());
	}
	if (data_[i].newData.Vals.empty() && !index->Opts().IsArray() && !index->Opts().IsSparse()) {
		data_[i].newData.Vals.emplace_back(PayloadFieldValue::GetDefaultValue(index->KeyType()));
		data_[i].isNewDataNode = false;
	}
	data_[i].isChanged = true;
}

void IntermediateIndexesData::SaveEmptyOldIndexData(size_t index) {
	assertrx_throw(index < data_.size());
	if (index == 0 || data_[index].isChanged) {
		return;
	}

	auto& data = data_[index];
	const auto& idx = *indexes_[index];
	if (!data.oldDataMaterialized && !idx.Opts().IsArray() && !idx.Opts().IsSparse()) {
		data.oldData.emplace_back(PayloadFieldValue::GetDefaultValue(idx.KeyType()));
	}
	data.isChanged = true;
}

std::string_view IntermediateIndexesData::IndexName(size_t index) const {
	assertrx_throw(index < indexes_.size());
	return indexes_[index]->Name();
}

}  // namespace reindexer
