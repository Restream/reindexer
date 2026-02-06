#include "core/idset/idset.h"

namespace reindexer {

std::string IdSetUnique::Dump() const {
	std::string buf = "[";

	if (!empty()) {
		buf += std::to_string(id_.ToNumber());
	}

	buf += ']';
	return buf;
}

std::string IdSetPlain::Dump() const {
	std::string buf = "[";

	for (auto id : *this) {
		buf += std::to_string(id.ToNumber()) + ' ';
	}

	buf += ']';
	return buf;
}

std::ostream& operator<<(std::ostream& os, const IdSetUnique& idset) {
	if (idset.empty()) {
		return os << "[]";
	}
	return os << '[' << idset.begin()->ToNumber() << ']';
}

std::ostream& operator<<(std::ostream& os, const IdSetPlain& idset) {
	os << '[';
	for (auto b = idset.begin(), it = b, e = idset.end(); it != e; ++it) {
		if (it != b) {
			os << ", ";
		}
		os << it->ToNumber();
	}
	return os << ']';
}

void IdSet::Dump(auto& os) const {
	auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
	os << "<IdSetPlain>: " << static_cast<const IdSetPlain&>(*this) << "\nusingBtree_: " << std::boolalpha << isUsingBtree;
	if (set) {
		os << "\nset_: ";
		set->dump(os);
	}
	os << std::endl;
}

template void IdSet::Dump(std::ostream&) const;

}  // namespace reindexer
