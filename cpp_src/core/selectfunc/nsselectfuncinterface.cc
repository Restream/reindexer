#include "nsselectfuncinterface.h"
#include "core/index/index.h"
#include "core/namespace.h"
namespace reindexer {

const string& NsSelectFuncInterface::GetName() const { return nm_.name_; };
int NsSelectFuncInterface::getIndexByName(const string& index) const { return nm_.getIndexByName(index); }

const string& NsSelectFuncInterface::getIndexName(int id) const { return nm_.indexes_[id]->Name(); }
IndexType NsSelectFuncInterface::getIndexType(int id) const { return nm_.indexes_[id]->Type(); }
const FieldsSet& NsSelectFuncInterface::getIndexFields(int id) const { return nm_.indexes_[id]->Fields(); }

}  // namespace reindexer
