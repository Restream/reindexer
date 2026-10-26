#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <span>
#include <string_view>
#include <utility>
#include <vector>
#include "core/keyvalue/variant.h"
#include "core/payload/payloadiface.h"
#include "estl/h_vector.h"
#include "tools/assertrx.h"

namespace reindexer {

class Index;
class TreeNode;

class [[nodiscard]] IntermediateIndexesData {
public:
	struct [[nodiscard]] IndexNodeData {
		VariantArray Vals;
		h_vector<TreeNode*, 4> Nodes;
	};

	struct [[nodiscard]] IndexInfo {
		bool isChanged = false;
		bool isNewDataNode = true;
		bool oldDataMaterialized = false;
		VariantArray oldData;
		IndexNodeData newData;
	};
	IntermediateIndexesData(std::span<const std::unique_ptr<Index>> indexes, const TagsMatcher& t) : indexes_(indexes), tm_(t) {
		data_.resize(indexes.size(),
					 {.isChanged = false, .isNewDataNode = true, .oldDataMaterialized = false, .oldData = {}, .newData = {}});
	}
	void Reset() { std::fill(data_.begin(), data_.end(), IndexInfo{}); }
	void SetTupleData(Variant oldValue, Variant newValue) {
		data_[0].oldData.emplace_back(std::move(oldValue));
		data_[0].newData.Vals.emplace_back(std::move(newValue));
		data_[0].isChanged = true;
	}
	bool IsIndexChanged(size_t index) const {
		assertrx_throw(index < data_.size());
		return data_[index].isChanged;
	}
	bool IsOldDataMaterialized(size_t index) const {
		assertrx_throw(index < data_.size());
		return data_[index].oldDataMaterialized;
	}
	void SetMaterializedOldData(size_t index, VariantArray&& values) {
		assertrx_throw(index > 0 && index < data_.size());
		auto& data = data_[index];
		data.oldData = std::move(values);
		data.oldDataMaterialized = true;
	}
	void MarkIndexChanged(size_t index) {
		assertrx_throw(index > 0 && index < data_.size());
		assertrx_throw(data_[index].oldDataMaterialized);
		data_[index].isChanged = true;
	}
	void SaveOldIndexData(size_t index, ConstPayload& plSave, TreeNode& root);

	void SaveOldIndexData(size_t index, VariantArray&& val) {
		assertrx_throw(index < data_.size());
		if (index == 0) {
			return;
		}
		if (data_[index].isChanged) {
			return;
		}
		auto& data = data_[index];
		if (!data.oldDataMaterialized) {
			data.oldData = std::move(val);
		}
		data.isChanged = true;
	}

	void SetNewIndexData(size_t i, IndexNodeData&& val, Index* index);
	std::string_view IndexName(size_t index) const;

	bool IsNewDataNode(size_t index) { return data_[index].isNewDataNode; }

	void SaveEmptyOldIndexData(size_t index);

	VariantArray& IndexDataValue(size_t index) noexcept {
		assertrx(index < data_.size());
		return data_[index].oldData;
	}
	const VariantArray& IndexDataValueNew(size_t index) const noexcept {
		assertrx(index < data_.size());
		return data_[index].newData.Vals;
	}
	void SetUpsertedIndexData(size_t index, VariantArray&& values) {
		assertrx_throw(index < data_.size());
		assertrx(data_[index].isChanged);
		data_[index].newData.Vals = std::move(values);
	}
	const h_vector<TreeNode*, 4>& IndexDataNodeNew(size_t index) const noexcept {
		assertrx(index < data_.size());
		return data_[index].newData.Nodes;
	}

private:
	std::vector<IndexInfo> data_;
	std::span<const std::unique_ptr<Index>> indexes_;
	const TagsMatcher& tm_;
};

}  // namespace reindexer
