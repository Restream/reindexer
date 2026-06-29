#pragma once

#include <vector>
#include "core/keyvalue/variant.h"

namespace reindexer {

struct FtFuncStruct;
class PayloadType;
class key_string;
class ItemRef;

class [[nodiscard]] DebugRank {
public:
	bool Process(ItemRef&, PayloadType&, const FtFuncStruct&, std::vector<key_string>& stringsHolder);

private:
	VariantArray plArr_;
};

}  // namespace reindexer
