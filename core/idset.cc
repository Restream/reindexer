#include "core/idset.h"
#include <algorithm>

using std::to_string;

namespace reindexer {

void IdSet::commit(const CommitContext &ctx) {
	int removed = 0;
	auto last = end() - inserted_;

	if (removed_) {
		// Remove all updated items. Updated items will be in inserted_ block of array
		for (auto it = begin(); it != last;) {
			if (ctx.updated(*it)) {
				*it = back();
				pop_back();

				if (removed < inserted_)
					it++;
				else
					last--;
				removed++;
			} else
				it++;
		}

		// 2-n phase of remove: remove all non exists items
		for (auto it = begin(); it != end();) {
			if (!ctx.exists(*it)) {
				*it = back();
				pop_back();
				removed++;
			} else
				it++;
		}
	}

	if (inserted_ || removed) std::sort(begin(), end());
	if (inserted_) base_idset::erase(std::unique(begin(), end()), end());

	removed_ = inserted_ = 0;
	shrink_to_fit();
}

string IdSet::dump() {
	string buf = "[";

	for (int i = 0; i < static_cast<int>(size()) - inserted_; i++) buf += to_string((*this)[i]) + " ";

	buf += "]";
	if (inserted_) {
		buf += " inserted [";

		for (int i = static_cast<int>(size()) - inserted_; i < static_cast<int>(size()); i++) buf += to_string((*this)[i]) + " ";

		buf += "]";
	}

	return buf;
}

}  // namespace reindexer
