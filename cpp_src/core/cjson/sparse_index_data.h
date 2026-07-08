#pragma once

#include <string>
#include "core/cjson/tagspath.h"
#include "core/enums.h"
#include "core/key_value_type.h"

namespace reindexer {

class FieldsSet;

struct [[nodiscard]] SparseIndexData {
	SparseIndexData(std::string n, IndexType idxType, KeyValueType t, IsArray a, const FieldsSet& fs);

	bool operator==(const SparseIndexData& other) const& noexcept {
		return name == other.name && dataType.IsSame(other.dataType) && indexType == other.indexType && isArray == other.isArray &&
			   paths == other.paths;
	}

	size_t ArrayDim() const noexcept { return indexType == IndexRTree ? 2 : 0; }

	std::string name;
	KeyValueType dataType;
	IndexType indexType;
	IsArray isArray;
	h_vector<TagsPath, 1> paths;
};

}  // namespace reindexer
