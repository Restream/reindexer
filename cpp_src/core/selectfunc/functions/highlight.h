#pragma once
#include "core/item.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

struct SelectFuncStruct;

class Highlight {
public:
	bool Process(ItemRef &res, PayloadType &pl_type, const SelectFuncStruct &func, std::vector<key_string> &stringsHolder);
};
}  // namespace reindexer
