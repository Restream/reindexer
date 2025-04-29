#include "sparse_index_data.h"
#include "core/payload/fieldsset.h"

namespace reindexer {

SparseIndexData::SparseIndexData(std::string n, IndexType idxType, KeyValueType t, IsArray a, const FieldsSet& fs)
	: name{std::move(n)}, dataType{t}, indexType{idxType}, isArray{a} {
	assertrx(fs.size() > 0);
	paths.reserve(fs.size());
	for (size_t i = 0, s = fs.size(); i < s; ++i) {
		assertrx(fs[i] == SetByJsonPath);
		paths.push_back(fs.getTagsPath(i));
	}
}

}  // namespace reindexer
