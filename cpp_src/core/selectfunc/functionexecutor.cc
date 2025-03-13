#include "functionexecutor.h"
#include "core/namespace/namespaceimpl.h"
#include "core/selectfunc/selectfuncparser.h"
#include "tools/timetools.h"

namespace reindexer {

Variant FunctionExecutor::Execute(SelectFuncStruct& funcData, const NsContext& ctx) {
	if (funcData.funcName == "now") {
		std::string_view mode = "sec";
		if (!funcData.funcArgs.empty() && !funcData.funcArgs.front().empty()) {
			mode = funcData.funcArgs.front();
		}
		return Variant(getTimeNow(mode));
	} else if (funcData.funcName == "serial") {
		return Variant(ns_.GetSerial(funcData.field, replUpdates_, ctx));
	}
	throw Error(errParams, "Unknown function '{}'. Field: '{}'", funcData.funcName, funcData.field);
}

}  // namespace reindexer
