#pragma once

#include <vector>
#include "areas_sorter.h"
#include "core/keyvalue/variant.h"

namespace reindexer {

struct FtFuncStruct;
class PayloadType;
class ItemRef;
class key_string;

class [[nodiscard]] Highlight : private AreasSorter {
public:
	bool Process(ItemRef&, PayloadType&, const FtFuncStruct&, std::vector<key_string>& stringsHolder);

private:
	VariantArray plArr_;
};

}  // namespace reindexer
