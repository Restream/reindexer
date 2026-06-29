#pragma once

#include "core/key_value_type.h"
#include "core/keyvalue/variant.h"
#include "field_array_analizer.h"
#include "fields_explorer.h"
#include "fields_value_extractor.h"

namespace reindexer::cjson {

class [[nodiscard]] FieldsArrayValueExtractorStrategy : private FieldArrayAnalizerStrategy, private FieldsValueExtractorStrategy {
public:
	using FieldArrayAnalizerStrategy::FieldParams;
	using FieldArrayAnalizerStrategy::OnScopeEnd;
	using FieldArrayAnalizerStrategy::TargetField;
	using FieldArrayAnalizerStrategy::IsHavingOffset;

protected:
	FieldsArrayValueExtractorStrategy(VariantArray& values, KeyValueType expectedType, FieldParams& params) noexcept
		: FieldArrayAnalizerStrategy(params), FieldsValueExtractorStrategy(values, expectedType) {}
	FieldsArrayValueExtractorStrategy(FieldsArrayValueExtractorStrategy& other) noexcept
		: FieldArrayAnalizerStrategy(other), FieldsValueExtractorStrategy(other) {}
	void ExactMatchObject() noexcept {
		FieldArrayAnalizerStrategy::ExactMatchObject();
		FieldsValueExtractorStrategy::ExactMatchObject();
	}
	void StartArray() noexcept {
		FieldArrayAnalizerStrategy::StartArray();
		FieldsValueExtractorStrategy::StartArray();
	}
	template <typename T>
	void Array(const PathFilter& filter, TagIndex tagIndex, std::span<const T> data, unsigned offset,
			   TreatAsSingleElement treatAsSingleElement = TreatAsSingleElement_False) {
		FieldArrayAnalizerStrategy::Array(filter, tagIndex, data.size(), offset, treatAsSingleElement);
		FieldsValueExtractorStrategy::Array(filter, tagIndex, data, offset, treatAsSingleElement);
	}
	void Array(const PathFilter& filter, TagIndex tagIndex, Serializer& ser, TagType tagType, int count) {
		FieldArrayAnalizerStrategy::Array(filter, tagIndex, count, 0);
		FieldsValueExtractorStrategy::Array(filter, tagIndex, ser, tagType, count);
	}
	void Put(const PathFilter& filter, Variant arg, int offset) {
		FieldArrayAnalizerStrategy::Put(filter, arg, offset);
		FieldsValueExtractorStrategy::Put(filter, std::move(arg), offset);
	}
};

using FieldsArrayValueExtractor = FieldsExplorer<FieldsArrayValueExtractorStrategy>;

}  // namespace reindexer::cjson
