#include "core/idset.h"
#include <algorithm>
#include "tools/errors.h"

namespace reindexer {
void IdSetPlain::Commit() {}

void IdSet::Commit() {
	if (!size() && set_) {
		resize(0);
		for (auto id : *set_) push_back(id);
	}

	usingBtree_ = false;
}

string IdSetPlain::Dump() {
	string buf = "[";

	for (int i = 0; i < static_cast<int>(size()); i++) buf += std::to_string((*this)[i]) + " ";

	buf += "]";

	return buf;
}

}  // namespace reindexer
