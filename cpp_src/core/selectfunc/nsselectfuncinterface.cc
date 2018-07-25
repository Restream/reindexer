#include "nsselectfuncinterface.h"
#include "core/index/index.h"
#include "core/namespace.h"
namespace reindexer {

const string& NsSelectFuncInterface::GetName() const { return nm_.name_; };
int NsSelectFuncInterface::getIndexByName(const string& index) const { return nm_.getIndexByName(index); }
bool NsSelectFuncInterface::getIndexByName(const string& name, int& index) const { return nm_.getIndexByName(name, index); }
int NsSelectFuncInterface::getIndexesCount() const { return nm_.indexes_.size(); }

const string& NsSelectFuncInterface::getIndexName(int id) const { return nm_.indexes_[id]->Name(); }
IndexType NsSelectFuncInterface::getIndexType(int id) const { return nm_.indexes_[id]->Type(); }
const FieldsSet& NsSelectFuncInterface::getIndexFields(int id) const { return nm_.indexes_[id]->Fields(); }
TagsPath NsSelectFuncInterface::getTagsPathForField(const string& jsonPath) const { return nm_.tagsMatcher_.path2tag(jsonPath); }

}  // namespace reindexer
