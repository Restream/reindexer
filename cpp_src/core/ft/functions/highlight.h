#pragma once

#include <vector>

namespace reindexer {

struct FtFuncStruct;
class PayloadType;
class ItemRef;
class key_string;

class [[nodiscard]] Highlight {
public:
	bool Process(ItemRef&, PayloadType&, const FtFuncStruct&, std::vector<key_string>& stringsHolder) const;
};

}  // namespace reindexer
