#include "fieldsset.h"
#include <sstream>
#include "core/cjson/tagsmatcher.h"

namespace reindexer {

[[noreturn]] void IndexesFieldsSet::throwMaxValueError(int f) {
	throw Error(errLogic, "Can not push_back({}) to IndexesFieldsSet. Value must be in scope [-1,{}]", f, kMaxIndexes - 1);
}

FieldsSet::FieldsSet(const TagsMatcher& tagsMatcher, const h_vector<std::string, 1>& fields) : mask_(0) {
	for (const std::string& str : fields) {
		if (auto tagsPath = tagsMatcher.path2tag(str); !tagsPath.empty()) {
			tagsPaths_.emplace_back(std::move(tagsPath));
		}
	}
}

std::string FieldsSet::ToString(DumpWithMask withMask) const {
	std::stringstream ret;
	Dump(ret, withMask);
	return ret.str();
}

void FieldsSet::throwMaxValueError(int f) {
	throw Error(errLogic, "Can not push_back({}) to FieldsSet. Value must be in scope [-1,{}]", f, kMaxIndexes - 1);
}

}  // namespace reindexer
