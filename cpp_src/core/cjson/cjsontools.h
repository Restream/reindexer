#pragma once

#include "core/payload/payloadiface.h"

namespace reindexer {

template <typename T>
void buildPayloadTuple(const PayloadIface<T>& pl, const TagsMatcher* tagsMatcher, WrSerializer& wrser);

template <typename Validator>
void copyCJsonValue(TagType, Serializer&, WrSerializer&, const Validator&);
void copyCJsonValue(TagType, const Variant& value, WrSerializer&);
void putCJsonRef(TagType, TagName, int tagField, const VariantArray& values, WrSerializer&);
void putCJsonValue(TagType, TagName, const VariantArray& values, WrSerializer&);

TagType arrayKvType2Tag(const VariantArray& values);
void skipCjsonTag(ctag tag, Serializer& rdser, std::array<unsigned, kMaxIndexes>* fieldsArrayOffsets = nullptr);
Variant cjsonValueToVariant(TagType tag, Serializer& rdser, KeyValueType dstType);

struct [[nodiscard]] CJsonNestedArrayAnalizeResult {
	size_t size{0};
	bool isNested{false};
};
CJsonNestedArrayAnalizeResult analizeNestedArray(size_t count, Serializer&);

[[noreturn]] void throwUnexpectedArrayError(std::string_view fieldName, KeyValueType fieldType, std::string_view parserName);
[[noreturn]] void throwUnexpectedArraySizeForFloatVectorError(std::string_view parserName, const PayloadFieldType& fieldRef, size_t size);
[[noreturn]] void throwUnexpectedArrayTypeForFloatVectorError(std::string_view parserName, const PayloadFieldType& fieldRef);
[[noreturn]] void throwUnexpectedNestedArrayError(std::string_view parserName, std::string_view fieldName, KeyValueType fieldType);
[[noreturn]] void throwScalarMultipleEncodesError(const Payload& pl, const PayloadFieldType& f, int field);
[[noreturn]] void throwUnexpectedArraySizeError(std::string_view parserName, std::string_view fieldName, size_t fieldArrayDim,
												size_t arraySize);
[[noreturn]] void throwUnexpected(std::string_view fieldName, KeyValueType expectedType, KeyValueType obtainedType,
								  std::string_view parserName);
[[noreturn]] void throwUnexpected(std::string_view fieldName, KeyValueType expectedType, std::string_view obtainedType,
								  std::string_view parserName);
[[noreturn]] void throwUnexpectedObjectInIndex(std::string_view fieldName, std::string_view parserName);

RX_ALWAYS_INLINE void validateNonArrayFieldRestrictions(const ScalarIndexesSetT& scalarIndexes, const Payload& pl,
														const PayloadFieldType& f, int field, InArray isInArray,
														std::string_view parserName) {
	if (!f.IsArray()) {
		if (isInArray) [[unlikely]] {
			throwUnexpectedNestedArrayError(parserName, f.Name(), f.Type());
		}
		if (scalarIndexes.test(field)) [[unlikely]] {
			throwScalarMultipleEncodesError(pl, f, field);
		}
	}
}

RX_ALWAYS_INLINE void validateArrayFieldRestrictions(std::string_view fieldName, IsArray isArray, size_t fieldArrayDim, size_t arraySize,
													 std::string_view parserName) {
	if (isArray) {
		if (arraySize > 0 && fieldArrayDim > 0 && fieldArrayDim != arraySize) [[unlikely]] {
			throwUnexpectedArraySizeError(parserName, fieldName, fieldArrayDim, arraySize);
		}
	}
}

void DumpCjson(Serializer& cjson, std::ostream& dump, const ConstPayload*, const TagsMatcher* = nullptr, std::string_view tab = "  ");
inline void DumpCjson(Serializer&& cjson, std::ostream& dump, const ConstPayload* pl, const TagsMatcher* tm = nullptr,
					  std::string_view tab = "  ") {
	DumpCjson(cjson, dump, pl, tm, tab);
}

void DumpCjson(Serializer& cjson, std::ostream& dump, const Payload*, const TagsMatcher* = nullptr, std::string_view tab = "  ");
inline void DumpCjson(Serializer&& cjson, std::ostream& dump, const Payload* pl, const TagsMatcher* tm = nullptr,
					  std::string_view tab = "  ") {
	DumpCjson(cjson, dump, pl, tm, tab);
}

inline void DumpCjson(Serializer& cjson, std::ostream& dump, const TagsMatcher* tm = nullptr, std::string_view tab = "  ") {
	DumpCjson(cjson, dump, static_cast<ConstPayload*>(nullptr), tm, tab);
}
inline void DumpCjson(Serializer&& cjson, std::ostream& dump, const TagsMatcher* tm = nullptr, std::string_view tab = "  ") {
	DumpCjson(cjson, dump, tm, tab);
}

static inline Variant convertNullToIndexField(KeyValueType fieldType, std::string_view fieldName, std::string_view parserName,
											  ConvertNull convertNull) {
	using namespace std::string_view_literals;
	if (convertNull) {
		return fieldType.EvaluateOneOf(
			[](KeyValueType::Double) noexcept { return Variant(0.0); }, [](KeyValueType::Float) noexcept { return Variant(0.0f); },
			[](KeyValueType::Bool) noexcept { return Variant(false); }, [](KeyValueType::Int) noexcept { return Variant(0); },
			[](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(0)); },
			[](KeyValueType::String) { return Variant(static_cast<const char*>(nullptr)); },
			[](KeyValueType::Uuid) noexcept { return Variant{Uuid{}}; },
			[](KeyValueType::FloatVector) noexcept { return Variant{ConstFloatVectorView{}}; },
			[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null> auto)
				-> Variant { throwUnexpected(fieldName, fieldType, "null"sv, parserName); });
	} else {
		return fieldType.EvaluateOneOf(
			[](concepts::OneOf<KeyValueType::Double, KeyValueType::Float, KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64,
							   KeyValueType::String, KeyValueType::Uuid> auto) noexcept { return Variant{}; },
			[](KeyValueType::FloatVector) noexcept { return Variant{ConstFloatVectorView{}}; },
			[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null> auto)
				-> Variant { throwUnexpected(fieldName, fieldType, "null"sv, parserName); });
	}
}

static inline void convertValueForIndexField(KeyValueType fieldType, std::string_view fieldName, Variant& value,
											 std::string_view parserName, ConvertToString convertToString, ConvertNull convertNull) {
	if (fieldType.IsSame(value.Type()) || fieldType.Is<KeyValueType::Undefined>()) {
		return;
	} else if (value.IsNullValue()) {
		value = convertNullToIndexField(fieldType, fieldName, parserName, convertNull);
	} else if ((fieldType.IsNumeric() && value.Type().IsNumeric()) ||
			   (fieldType.Is<KeyValueType::Uuid>() && value.Type().Is<KeyValueType::String>()) ||
			   (convertToString && fieldType.Is<KeyValueType::String>() && value.Type().IsNumeric())) {
		value.convert(fieldType);
	} else {
		throwUnexpected(fieldName, fieldType, value.Type().Name(), parserName);
	}
}

}  // namespace reindexer
