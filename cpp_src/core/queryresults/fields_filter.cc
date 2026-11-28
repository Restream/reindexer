#include "fields_filter.h"
#include <sstream>
#include "core/namespace/namespace_metadata_view.h"
#include "core/payload/payloadtype.h"
#include "core/query/fields_names_filter.h"
#include "estl/h_vector.h"

namespace reindexer {

const FieldsFilter& FieldsFilter::AllFields() {
	static const FieldsFilter filter{true, true};
	return filter;
}

std::shared_ptr<const FieldsFilter> FieldsFilter::AllFieldsPtr() {
	return std::shared_ptr<const FieldsFilter>{std::shared_ptr<const FieldsFilter>{}, &AllFields()};
}

const FieldsFilter& FieldsFilter::Empty() noexcept {
	static const FieldsFilter filter;
	return filter;
}

const std::shared_ptr<const FieldsFilter>& FieldsFilter::EmptyPtr() noexcept {
	static const std::shared_ptr<const FieldsFilter> empty{std::shared_ptr<const FieldsFilter>{}, &FieldsFilter::Empty()};
	return empty;
}

std::shared_ptr<const FieldsFilter> FieldsFilter::Create(const FieldsNamesFilter& filter, const NamespaceMetadataView& metadata) {
	if (filter.OnlyAllRegularFields()) {
		return EmptyPtr();
	} else if (filter.AllRegularFields() && filter.AllVectorFields()) {
		return AllFieldsPtr();
	} else {
		return std::make_shared<FieldsFilter>(filter, metadata);
	}
}

FieldsFilter::FieldsFilter(const FieldsNamesFilter& filter, const NamespaceMetadataView& metadata)
	: FieldsFilter{filter.AllRegularFields(), filter.AllVectorFields()} {
	if (!filter.AllRegularFields() || !filter.AllVectorFields()) {
		fill(filter.Fields(), metadata);
	}
}

template <concepts::ConvertibleToString Str>
FieldsFilter::FieldsFilter(const Str& field, const NamespaceMetadataView& metadata) : FieldsFilter{false, false} {
	add(field, metadata);
}
template FieldsFilter::FieldsFilter(const std::string_view&, const NamespaceMetadataView&);

FieldsFilter::FieldsFilter(std::span<const std::string> fields, const NamespaceMetadataView& metadata) : FieldsFilter{false, false} {
	fill(fields, metadata);
}

FieldsFilter FieldsFilter::FromFieldsSet(const FieldsSet& fields, const PayloadType& pt, const NamespaceMetadataView& metadata) {
	h_vector<std::string, 4> jsonPaths;
	for (int f : fields) {
		if (f >= 0 && f < pt.NumFields()) {
			const auto& paths = pt.Field(f).JsonPaths();
			jsonPaths.insert(jsonPaths.end(), paths.begin(), paths.end());
		}
	}
	for (size_t i = 0, s = fields.getJsonPathsLength(); i < s; ++i) {
		jsonPaths.emplace_back(fields.getJsonPath(i));
	}
	assertrx_throw(!jsonPaths.empty());
	return FieldsFilter{jsonPaths, metadata};
}

template <concepts::ConvertibleToString Str>
void FieldsFilter::add(const Str& field, const NamespaceMetadataView& metadata) {
	int idx = IndexValueType::NotSet;
	const auto foundIdx = metadata.TryGetIndexPosByJsonPath(field, idx);
	if (foundIdx) {
		if (metadata.IsFloatVectorIndex(idx)) {
			if (!allVectorFields_) {
				vectorFields_.push_back(idx);
				const auto& jsonPaths = metadata.GetPayloadType().Field(idx).JsonPaths();
				assertrx(jsonPaths.size() == 1);
				vectorFields_.push_back(metadata.GetTagsPath(jsonPaths[0]));
			}
		} else if (!allRegularFields_) {
			if (idx < metadata.GetPayloadType().NumFields()) {
				regularFields_.push_back(idx);
				for (const auto& jsonPath : metadata.GetPayloadType().Field(idx).JsonPaths()) {
					regularFields_.push_back(metadata.GetTagsPath(jsonPath));
				}
			} else {
				const auto& idxFields = metadata.GetIndexFields(idx);
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
		auto tagsPath = metadata.GetTagsPath(field);
		if (!tagsPath.empty()) {
			regularFields_.push_back(std::move(tagsPath));
		}
	}
}

void FieldsFilter::fill(std::span<const std::string> fields, const NamespaceMetadataView& metadata) {
	for (const auto& fld : fields) {
		add(fld, metadata);
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
