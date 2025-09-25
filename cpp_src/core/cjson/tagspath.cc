#include "tagspath.h"
#include <ostream>
#include "core/cjson/tagsmatcher.h"

namespace reindexer {

void Dump(auto& os, const TagsPath& path, TagsMatcher* tm) {
	for (size_t i = 0; i < path.size(); ++i) {
		if (i != 0) {
			os << '.';
		}
		if (tm) {
			os << tm->tag2name(path[i]);
		}
		os << '<' << path[i].AsNumber() << '>';
	}
}
template void Dump(std::ostream&, const TagsPath&, TagsMatcher*);

template <unsigned hvSize>
void IndexedTagsPathImpl<hvSize>::Dump(auto& os, TagsMatcher* tm) const {
	using namespace std::string_view_literals;
	for (size_t i = 0; i < this->size(); ++i) {
		const auto& node = (*this)[i];
		if (node.IsTagName()) {
			const auto name = node.GetTagName();
			if (i != 0) {
				os << '.';
			}
			if (tm) {
				os << tm->tag2name(name);
			}
			os << '<' << name.AsNumber() << '>';
		} else {
			const auto index = node.GetTagIndex();
			if (index.IsAll()) {
				os << "[*]"sv;
			} else {
				os << '[' << index.AsNumber() << ']';
			}
		}
	}
}
template void IndexedTagsPathImpl<6>::Dump(std::ostream&, TagsMatcher*) const;
template void IndexedTagsPathImpl<16>::Dump(std::ostream&, TagsMatcher*) const;

}  // namespace reindexer
