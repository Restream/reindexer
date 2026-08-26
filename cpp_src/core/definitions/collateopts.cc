#include "collateopts.h"

#include <ostream>

namespace reindexer {

CollateOpts::CollateOpts(const std::string& sortOrderUTF8) : mode(CollateCustom), sortOrderTable(sortOrderUTF8) {}

template <typename T>
void CollateOpts::Dump(T& os) const {
	using namespace reindexer;
	os << mode;
	if (mode == CollateCustom) {
		os << ": [" << sortOrderTable.GetSortOrderCharacters() << ']';
	}
}
template void CollateOpts::Dump<std::ostream>(std::ostream&) const;

}  // namespace reindexer
