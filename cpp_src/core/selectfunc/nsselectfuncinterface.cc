#include "nsselectfuncinterface.h"
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
namespace reindexer {

const std::string& NsSelectFuncInterface::GetName() const { return nm_.name_; };
int NsSelectFuncInterface::getIndexByName(const std::string& index) const { return nm_.getIndexByName(index); }
bool NsSelectFuncInterface::getIndexByName(const std::string& name, int& index) const { return nm_.getIndexByName(name, index); }
int NsSelectFuncInterface::getIndexesCount() const { return nm_.indexes_.size(); }

const std::string& NsSelectFuncInterface::getIndexName(int id) const { return nm_.indexes_[id]->Name(); }
IndexType NsSelectFuncInterface::getIndexType(int id) const { return nm_.indexes_[id]->Type(); }
const FieldsSet& NsSelectFuncInterface::getIndexFields(int id) const { return nm_.indexes_[id]->Fields(); }
TagsPath NsSelectFuncInterface::getTagsPathForField(const std::string& jsonPath) const { return nm_.tagsMatcher_.path2tag(jsonPath); }

}  // namespace reindexer
