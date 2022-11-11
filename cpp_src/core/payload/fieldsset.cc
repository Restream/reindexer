#include "fieldsset.h"
#include "core/cjson/tagsmatcher.h"

namespace reindexer {

FieldsSet::FieldsSet(const TagsMatcher &tagsMatcher, const h_vector<std::string, 1> &fields) : mask_(0) {
	static_assert(std::numeric_limits<decltype(mask_)>::digits >= maxIndexes, "mask_ needs to provide 'maxIndexes' bits or more");
	for (const std::string &str : fields) {
		tagsPaths_.emplace_back(tagsMatcher.path2tag(str));
	}
}

}  // namespace reindexer
