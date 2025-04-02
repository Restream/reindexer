#pragma once

#include <vector>

namespace reindexer {

struct SelectFuncStruct;
class PayloadType;
class key_string;
class ItemRef;

class DebugRank {
public:
	bool Process(ItemRef& res, PayloadType& plType, const SelectFuncStruct& func, std::vector<key_string>& stringsHolder);
};
}  // namespace reindexer
