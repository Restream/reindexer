#include "fieldsset.h"
#include <sstream>
#include "core/cjson/tagsmatcher.h"
#include "core/enums.h"

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

template <typename Os>
class [[nodiscard]] FieldsSet::DumpFieldsPath {
public:
	DumpFieldsPath(Os& os) noexcept : os_{os} {}
	void operator()(const TagsPath& tp) const {
		os_ << '[';
		for (auto b = tp.cbegin(), it = b, e = tp.cend(); it != e; ++it) {
			if (it != b) {
				os_ << ", ";
			}
			os_ << it->AsNumber();
		}
		os_ << ']';
	}
	void operator()(const IndexedTagsPath& tp) const {
		os_ << '[';
		for (auto b = tp.cbegin(), it = b, e = tp.cend(); it != e; ++it) {
			if (it != b) {
				os_ << ", ";
			}
			os_ << '?';
		}
		os_ << ']';
	}

private:
	Os& os_;
};

template <typename Os>
void FieldsSet::Dump(Os& os, DumpWithMask withMask) const {
	const DumpFieldsPath fieldsPathDumper{os};
	os << "{[";
	for (auto b = begin(), it = b, e = end(); it != e; ++it) {
		if (it != b) {
			os << ", ";
		}
		os << *it;
	}
	os << "], ";
	if (withMask == DumpWithMask_True) {
		os << "mask: " << mask_ << ", ";
	}
	os << "tagsPaths: [";
	for (auto b = tagsPaths_.cbegin(), it = b, e = tagsPaths_.cend(); it != e; ++it) {
		if (it != b) {
			os << ", ";
		}
		std::visit(fieldsPathDumper, *it);
	}
	os << "]}";
	os << "], jsonPaths: [";
	for (auto b = jsonPaths_.cbegin(), it = b, e = jsonPaths_.cend(); it != e; ++it) {
		if (it != b) {
			os << ", ";
		}
		os << *it;
	}
	os << "]}";
}

template void FieldsSet::Dump(std::ostream&, DumpWithMask) const;

}  // namespace reindexer
