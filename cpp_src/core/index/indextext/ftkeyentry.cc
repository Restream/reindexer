#include "ftkeyentry.h"

namespace reindexer {

void FtKeyEntryData::Dump(std::ostream& os, std::string_view step, std::string_view offset) const {
	std::string newOffset{offset};
	newOffset += step;
	os << "{\n" << newOffset << "vdoc_id: " << vdoc_id_ << ",\n" << newOffset;
	Base::Dump(os, step, newOffset);
	os << '\n' << offset << '}';
}

}  // namespace reindexer
