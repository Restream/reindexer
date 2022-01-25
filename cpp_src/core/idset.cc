#include "core/idset.h"
#include <algorithm>
#include "tools/errors.h"

namespace reindexer {

void IdSet::Commit() {
	if (!size() && set_) {
		resize(0);
		for (auto id : *set_) push_back(id);
	}

	usingBtree_ = false;
}

string IdSetPlain::Dump() const {
	string buf = "[";

	for (int i = 0; i < static_cast<int>(size()); i++) buf += std::to_string((*this)[i]) + " ";

	buf += "]";

	return buf;
}

std::ostream& operator<<(std::ostream& os, const IdSetPlain& idset) {
	os << '[';
	for (auto b = idset.begin(), it = b, e = idset.end(); it != e; ++it) {
		if (it != b) os << ", ";
		os << *it;
	}
	return os << ']';
}

}  // namespace reindexer
