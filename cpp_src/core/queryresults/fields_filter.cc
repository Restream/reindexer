#include "fields_filter.h"
#include <sstream>
#include "core/index/index.h"
#include "core/namespace/namespaceimpl.h"
#include "core/query/fields_names_filter.h"

namespace reindexer {

FieldsFilter::FieldsFilter(const FieldsNamesFilter& filter, const NamespaceImpl& ns)
	: FieldsFilter{filter.AllRegularFields(), filter.AllVectorFields()} {
	if (!filter.AllRegularFields() || !filter.AllVectorFields()) {
		fill(filter.Fields(), ns);
	}
}

template <concepts::ConvertibleToString Str>
FieldsFilter::FieldsFilter(const Str& field, const NamespaceImpl& ns) : FieldsFilter{false, false} {
	add(field, ns);
}
template FieldsFilter::FieldsFilter(const std::string_view&, const NamespaceImpl&);

FieldsFilter::FieldsFilter(std::span<const std::string> fields, const NamespaceImpl& ns) : FieldsFilter{false, false} { fill(fields, ns); }

template <concepts::ConvertibleToString Str>
void FieldsFilter::add(const Str& field, const NamespaceImpl& ns) {
	int idx = IndexValueType::NotSet;
	const auto foundIdx = ns.tryGetIndexByJsonPath(field, idx);
	if (foundIdx) {
		if (ns.indexes_[idx]->IsFloatVector()) {
			if (!allVectorFields_) {
				vectorFields_.push_back(idx);
				const auto& jsonPaths = ns.payloadType_->Field(idx).JsonPaths();
				assertrx(jsonPaths.size() == 1);
				vectorFields_.push_back(ns.tagsMatcher_.path2tag(jsonPaths[0]));
			}
		} else if (!allRegularFields_) {
			if (idx < ns.payloadType_->NumFields()) {
				regularFields_.push_back(idx);
				for (const auto& jsonPath : ns.payloadType_->Field(idx).JsonPaths()) {
					regularFields_.push_back(ns.tagsMatcher_.path2tag(jsonPath));
				}
			} else {
				const auto& idxFields = ns.indexes_[idx]->Fields();
				for (int fldIdx : idxFields) {
					if (fldIdx >= 0) {
						regularFields_.push_back(fldIdx);
					}
				}
				for (int i = 0, s = idxFields.getTagsPathsLength(); i < s; ++i) {
					regularFields_.push_back(idxFields.getTagsPath(i));
				}
			}
		}
	} else if (!allRegularFields_) {
		auto tagsPath = ns.tagsMatcher_.path2tag(field);
		if (!tagsPath.empty()) {
			regularFields_.push_back(std::move(tagsPath));
		}
	}
}

void FieldsFilter::fill(std::span<const std::string> fields, const NamespaceImpl& ns) {
	for (const auto& fld : fields) {
		add(fld, ns);
	}
}

std::string FieldsFilter::Dump() const {
	std::stringstream ss;
	ss << "regular: ";
	if (allRegularFields_) {
		ss << "all";
	} else {
		regularFields_.Dump(ss, DumpWithMask_False);
	}
	ss << "\nvector: ";
	if (allVectorFields_) {
		ss << "all";
	} else {
		vectorFields_.Dump(ss, DumpWithMask_False);
	}
	ss << '\n';
	return ss.str();
}

}  // namespace reindexer
