#pragma once

#include <tuple>
#include "fields_explorer.h"
#include "tools/serilize/serializer.h"

namespace reindexer::cjson {

class [[nodiscard]] FieldsValueExtractorStrategy {
protected:
	FieldsValueExtractorStrategy(VariantArray& va, KeyValueType expectedType) noexcept : values_(va), expectedType_(expectedType) {}
	FieldsValueExtractorStrategy(FieldsValueExtractorStrategy& other) noexcept
		: values_(other.values_), expectedType_(other.expectedType_) {}
	void ExactMatchObject() noexcept { values_.MarkObject(); }
	void StartArray() noexcept { std::ignore = values_.MarkArray(); }

	template <typename T>
	void Array(const PathFilter& filter, TagIndex tagIndex, std::span<const T> data, unsigned /*offset*/,
			   TreatAsSingleElement = TreatAsSingleElement_False) {
		if (!filter.Match()) {
			return;
		}
		if (tagIndex.IsAll()) {
			for (const auto& d : data) {
				put(Variant(d));
			}
		} else {
			size_t i{0};
			for (const auto& d : data) {
				if (i++ == tagIndex.AsNumber()) {
					put(Variant(d));
				}
			}
		}
		std::ignore = values_.MarkArray();
	}

	void Array(const PathFilter& filter, TagIndex tagIndex, Serializer& ser, TagType tagType, int count) {
		const KeyValueType kvt{tagType};
		if (tagIndex.IsAll()) {
			for (int i = 0; i < count; ++i) {
				auto value = ser.GetRawVariant(kvt);
				if (filter.Match()) {
					put(std::move(value));
				}
			}
		} else {
			for (int i = 0; i < count; ++i) {
				auto value = ser.GetRawVariant(kvt);
				if (TagIndex(i) == tagIndex && filter.Match()) {
					put(std::move(value));
				}
			}
		}
		if (filter.Match()) {
			std::ignore = values_.MarkArray();
		}
	}

	void Put(const PathFilter& filter, Variant arg, int /*offset*/) {
		if (filter.Match()) {
			return put(std::move(arg));
		}
	}

private:
	void put(Variant arg) {
		expectedType_.EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
								KeyValueType::String, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Uuid,
								KeyValueType::FloatVector> auto) { std::ignore = arg.convert(expectedType_); },
			[](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Composite> auto) noexcept {});
		values_.emplace_back(std::move(arg));
	}

	VariantArray& values_;
	KeyValueType expectedType_{KeyValueType::Undefined{}};
};

using FieldsValueExtractor = FieldsExplorer<FieldsValueExtractorStrategy>;

}  // namespace reindexer::cjson
