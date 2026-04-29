#include "core/idset/idset.h"
#include "sort/pdqsort.hpp"

namespace reindexer {

void IdSetUnique::Dump(std::ostream& os) const {
	os << "[";

	if (!IsEmpty()) {
		os << id_.ToNumber();
	}

	os << ']';
}

IdSetPlain::Ptr IdSetPlain::BuildFromUnsorted(base_idset&& ids) {
	boost::sort::pdqsort_branchless(ids.begin(), ids.end());
	ids.erase(std::unique(ids.begin(), ids.end()), ids.cend());	 // TODO: It would be better to integrate unique into sort
	return make_intrusive<intrusive_atomic_rc_wrapper<IdSetPlain>>(std::move(ids));
}

void IdSetPlain::Dump(std::ostream& os) const {
	os << "[";

	for (auto id : *this) {
		os << id.ToNumber() << ' ';
	}

	os << ']';
}

void IdSet::Commit() {
	if (!size()) {
		auto set = set_.Get(std::memory_order_relaxed).first;
		if (set) {
			reserve(set->size());
			for (auto id : *set) {
				push_back(id);
			}
		}
	}

	setUsingBtree(false);
}

void IdSet::Dump(std::ostream& os) const {
	auto [set, isUsingBtree] = set_.Get(std::memory_order_acquire);
	os << "<IdSetPlain>: ";
	static_cast<const IdSetPlain&>(*this).Dump(os);
	os << "\nusingBtree_: " << std::boolalpha << isUsingBtree;
	if (set) {
		os << "\nset_: ";
		set->dump(os);
	}
	os << std::endl;
}

}  // namespace reindexer
