#include "functionexecutor.h"
#include "core/namespace/namespaceimpl.h"
#include "core/selectfunc/selectfuncparser.h"
#include "tools/timetools.h"

namespace reindexer {

FunctionExecutor::FunctionExecutor(NamespaceImpl& ns) : ns_(ns) {}

Variant FunctionExecutor::Execute(SelectFuncStruct& funcData) {
	if (funcData.funcName == "now") {
		string mode = "sec";
		if (!funcData.funcArgs.empty() && !funcData.funcArgs.front().empty()) {
			mode = funcData.funcArgs.front();
		}
		return Variant(getTimeNow(mode));
	} else if (funcData.funcName == "serial") {
		return Variant(ns_.GetSerial(funcData.field));
	} else {
		throw Error(errParams, "Unknown function %s", funcData.field);
	}
}

}  // namespace reindexer
