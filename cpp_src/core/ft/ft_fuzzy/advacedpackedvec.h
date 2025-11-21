#pragma once
#include <limits.h>
#include "core/ft/idrelset.h"
namespace reindexer {
class IdRelSet;
class IdRelType;

class [[nodiscard]] AdvacedPackedVec : public PackedIdRelVec {
public:
	AdvacedPackedVec(IdRelSet&& data);

	int max_id_;
	int min_id_;
};
}  // namespace reindexer
