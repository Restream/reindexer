#pragma once
#include "core/item.h"
#include "core/queryresults/queryresults.h"
#include "core/selectfunc/selectfuncparser.h"

namespace reindexer {

class Highlight {
public:
	static bool process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func);
};
}  // namespace reindexer
