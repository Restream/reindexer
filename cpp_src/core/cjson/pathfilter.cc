#include "pathfilter.h"

namespace reindexer {

template <typename Os>
void PathFilter::Dump(Os& os) const {
	std::visit(overloaded{[&os](const TagsPath* path) { reindexer::Dump(os, *path); },
						  [&os](const IndexedTagsPath* path) { path->Dump(os, nullptr); }},
			   path_);
	os << "; pos: " << position_ << "; match: " << match_ << std::endl;
}

template void PathFilter::Dump(std::ostream&) const;

}  // namespace reindexer
