#include "fieldsset.h"
#include "core/cjson/tagsmatcher.h"

namespace reindexer {

[[noreturn]] void IndexesFieldsSet::throwMaxValueError(int f) {
	throw Error(errLogic, "Can not push_back(%d) to IndexesFieldsSet. Value must be in scope [-1,%d]", f, kMaxIndexes - 1);
}

FieldsSet::FieldsSet(const TagsMatcher &tagsMatcher, const h_vector<std::string, 1> &fields) : mask_(0) {
	for (const std::string &str : fields) {
		tagsPaths_.emplace_back(tagsMatcher.path2tag(str));
	}
}

void FieldsSet::throwMaxValueError(int f) {
	throw Error(errLogic, "Can not push_back(%d) to FieldsSet. Value must be in scope [-1,%d]", f, kMaxIndexes - 1);
}

}  // namespace reindexer
