#pragma once

#include <vector>

namespace reindexer {

struct FtFuncStruct;
class PayloadType;
class key_string;
class ItemRef;

class [[nodiscard]] DebugRank {
public:
	[[nodiscard]] bool Process(ItemRef&, PayloadType&, const FtFuncStruct&, std::vector<key_string>& stringsHolder) const;
};

}  // namespace reindexer
