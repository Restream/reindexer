#include "ns_ft_func_interface.h"
#include "core/index/index.h"
#include "indexes/registry.h"

namespace reindexer {

bool NsFtFuncInterface::getIndexByName(std::string_view name, int& index) const noexcept { return registry_.TryGetIndexPos(name, index); }
int NsFtFuncInterface::getIndexesCount() const noexcept { return registry_.Indexes().size(); }

const std::string& NsFtFuncInterface::getIndexName(int id) const noexcept { return registry_.Indexes()[id]->Name(); }
IndexType NsFtFuncInterface::getIndexType(int id) const noexcept { return registry_.Indexes()[id]->Type(); }
const FieldsSet& NsFtFuncInterface::getIndexFields(int id) const noexcept { return registry_.Indexes()[id]->Fields(); }
TagsPath NsFtFuncInterface::getTagsPathForField(std::string_view jsonPath) const noexcept {
	return registry_.GetTagsMatcher().path2tag(jsonPath);
}

}  // namespace reindexer
