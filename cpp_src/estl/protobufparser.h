#pragma once

#include "core/cjson/tagspath.h"
#include "core/keyvalue/variant.h"
#include "tools/serializer.h"

namespace reindexer {

class Schema;
class TagsMatcher;

struct [[nodiscard]] ProtobufValue {
	ProtobufValue();
	ProtobufValue(Variant&& _value, TagName _tagName, KeyValueType itemType, bool isArray);

	template <typename T, typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) &&
												  !std::is_same<T, bool>::value>::type* = nullptr>
	T As(T minv = std::numeric_limits<T>::min(), T maxv = std::numeric_limits<T>::max()) const {
		T v;
		value.Type().EvaluateOneOf(
			[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double> auto) noexcept { v = T(value); },
			[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Composite, KeyValueType::Undefined,
								KeyValueType::Null, KeyValueType::Tuple> auto) {
				throw reindexer::Error(errParseMsgPack, "Impossible to convert type [{}] to number", value.Type().Name());
			});
		if (v < minv || v > maxv) {
			throw reindexer::Error(errParams, "Value is out of bounds: [{},{}]", minv, maxv);
		}
		return v;
	}

	template <typename T,
			  typename std::enable_if<std::is_same<std::string, T>::value || std::is_same<std::string_view, T>::value>::type* = nullptr>
	T As() const {
		if (!value.Type().Is<KeyValueType::String>()) {
			throw reindexer::Error(errParseMsgPack, "Impossible to convert type [{}] to string", value.Type().Name());
		}
		return T(value);
	}

	template <typename T, typename std::enable_if<std::is_same<T, bool>::value>::type* = nullptr>
	T As() const {
		if (!value.Type().Is<KeyValueType::Bool>()) {
			throw reindexer::Error(errParseMsgPack, "Impossible to convert type [{}] to bool", value.Type().Name());
		}
		return T(value);
	}

	bool IsOfPrimitiveType() const {
		return itemType.EvaluateOneOf(
			[](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
							   KeyValueType::Bool> auto) noexcept { return true; },
			[](concepts::OneOf<KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::String,
							   KeyValueType::Undefined, KeyValueType::Uuid, KeyValueType::FloatVector> auto) noexcept { return false; });
	}

	Variant value;
	TagName tagName;
	KeyValueType itemType;
	bool isArray;
};

class ProtobufParser;

struct [[nodiscard]] ProtobufObject {
	ProtobufObject(std::string_view _buf, const Schema& _schema, TagsPath& tagsPath, TagsMatcher& tm)
		: ser(_buf), schema(_schema), tagsPath(tagsPath), tm(tm) {}
	ProtobufObject(const ProtobufObject&) = delete;
	ProtobufObject(ProtobufObject&&) = delete;
	ProtobufObject& operator=(const ProtobufObject&) = delete;
	ProtobufObject& operator=(ProtobufObject&&) = delete;

	Serializer ser;
	const Schema& schema;
	TagsPath& tagsPath;
	TagsMatcher& tm;
};

class [[nodiscard]] ProtobufParser {
public:
	explicit ProtobufParser(ProtobufObject& obj) : object_(obj) {}
	ProtobufParser(const ProtobufParser&) = delete;
	ProtobufParser(ProtobufParser&&) = delete;
	ProtobufParser& operator=(const ProtobufParser&) = delete;
	ProtobufParser& operator=(ProtobufParser&&) = delete;

	ProtobufValue ReadValue();
	Variant ReadArrayItem(KeyValueType fieldType);

	bool IsEof() const;

private:
	ProtobufObject& object_;
};

}  // namespace reindexer
