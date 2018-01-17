#pragma once
#include <limits.h>
#include "estl/packed_vector.h"
namespace reindexer {
class IdRelSet;
struct IdRelType;

class AdvacedPackedVec : public packed_vector<IdRelType> {
public:
	AdvacedPackedVec(IdRelSet &&data);

	int max_id_;
	int min_id_;
};
}  // namespace reindexer
