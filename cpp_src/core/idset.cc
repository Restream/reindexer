#include "core/idset.h"
#include "tools/errors.h"

namespace reindexer {

std::string IdSetPlain::Dump() const {
	std::string buf = "[";

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
