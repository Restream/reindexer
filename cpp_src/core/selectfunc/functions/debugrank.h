#pragma once
#include "core/item.h"
#include "core/queryresults/queryresults.h"

namespace reindexer {

struct SelectFuncStruct;

class DebugRank {
public:
	bool Process(ItemRef& res, PayloadType& plType, const SelectFuncStruct& func, std::vector<key_string>& stringsHolder);
};
}  // namespace reindexer
