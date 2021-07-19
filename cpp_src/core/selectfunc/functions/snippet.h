#pragma once
#include "core/item.h"
#include "core/queryresults/queryresults.h"
#include "core/selectfunc/selectfuncparser.h"

namespace reindexer {

class Snippet {
public:
	static bool process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func, std::vector<key_string> &stringsHolder);
};
}  // namespace reindexer
