#include "advacedpackedvec.h"
#include "core/ft/idrelset.h"
namespace reindexer {

AdvacedPackedVec::AdvacedPackedVec(IdRelSet&& data) {
	data.SimpleCommit();

	insert_back(data.begin(), data.end());

	max_id_ = data.max_id_;
	min_id_ = data.min_id_;
	data.clear();
}
}  // namespace reindexer
