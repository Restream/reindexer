#pragma once

#include <vector>

namespace reindexer {

struct SelectFuncStruct;
class PayloadType;
class ItemRef;
class key_string;

class Highlight {
public:
	bool Process(ItemRef& res, PayloadType& pl_type, const SelectFuncStruct& func, std::vector<key_string>& stringsHolder);
};

}  // namespace reindexer
