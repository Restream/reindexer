#include "tag_name_index.h"

namespace reindexer {

void TagIndex::throwOverflow(auto v) {
	using namespace std::string_view_literals;
	throw Error{errLogic, "TagIndex overflow - max value is {}, got {}"sv, all_v - 1, v};
}
template void TagIndex::throwOverflow(uint64_t);

}  // namespace reindexer
