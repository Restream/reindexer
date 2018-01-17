#include "core/idset.h"
#include <algorithm>

namespace reindexer {
void IdSetPlain::Commit(const CommitContext& ctx) {
	// reserve for sorted ids, to avoid allocation and race on UpdateSortOrders
	reserve(size() * (ctx.getSortedIdxCount() + 1));
}

void IdSet::Commit(const CommitContext& ctx) {
	if (!size() && set_) {
		resize(0);
		reserve(set_->size() * (ctx.getSortedIdxCount() + 1));
		for (auto id : *set_) push_back(id);
		//	set_ = nullptr;
	} else {
		auto sz = size();
		auto expCap = sz * (ctx.getSortedIdxCount() + 1);
		if (capacity() != expCap) {
			resize(expCap);
			shrink_to_fit();
			resize(sz);
		}
	}
}

string IdSetPlain::Dump() {
	string buf = "[";

	for (int i = 0; i < static_cast<int>(size()); i++) buf += std::to_string((*this)[i]) + " ";

	buf += "]";

	return buf;
}

}  // namespace reindexer
