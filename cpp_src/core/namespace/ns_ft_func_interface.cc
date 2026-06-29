#include "ns_ft_func_interface.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"

namespace reindexer {

bool NsFtFuncInterface::getIndexByName(std::string_view name, int& index) const noexcept { return nm_.tryGetIndexByName(name, index); }
int NsFtFuncInterface::getIndexesCount() const noexcept { return nm_.indexes_.size(); }

const std::string& NsFtFuncInterface::getIndexName(int id) const noexcept { return nm_.indexes_[id]->Name(); }
IndexType NsFtFuncInterface::getIndexType(int id) const noexcept { return nm_.indexes_[id]->Type(); }
const FieldsSet& NsFtFuncInterface::getIndexFields(int id) const noexcept { return nm_.indexes_[id]->Fields(); }
TagsPath NsFtFuncInterface::getTagsPathForField(std::string_view jsonPath) const noexcept { return nm_.tagsMatcher_.path2tag(jsonPath); }

}  // namespace reindexer
