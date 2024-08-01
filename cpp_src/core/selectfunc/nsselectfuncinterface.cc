#include "nsselectfuncinterface.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
namespace reindexer {

const std::string& NsSelectFuncInterface::GetName() const noexcept { return nm_.name_; }
int NsSelectFuncInterface::getIndexByName(std::string_view index) const noexcept { return nm_.getIndexByName(index); }
bool NsSelectFuncInterface::getIndexByName(std::string_view name, int& index) const noexcept { return nm_.tryGetIndexByName(name, index); }
int NsSelectFuncInterface::getIndexesCount() const noexcept { return nm_.indexes_.size(); }

const std::string& NsSelectFuncInterface::getIndexName(int id) const noexcept { return nm_.indexes_[id]->Name(); }
IndexType NsSelectFuncInterface::getIndexType(int id) const noexcept { return nm_.indexes_[id]->Type(); }
const FieldsSet& NsSelectFuncInterface::getIndexFields(int id) const noexcept { return nm_.indexes_[id]->Fields(); }
TagsPath NsSelectFuncInterface::getTagsPathForField(std::string_view jsonPath) const noexcept {
	return nm_.tagsMatcher_.path2tag(jsonPath);
}

}  // namespace reindexer
