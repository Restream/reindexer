#pragma once

#include <unordered_map>
#include "core/cjson/objtype.h"
#include "core/cjson/tagsmatcherimpl.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/variant.h"
#include "tools/serializer.h"
#include "vendor/mpark/variant.h"

namespace reindexer {

class Schema;
class TagsMatcher;

struct ProtobufValue {
	ProtobufValue();
	ProtobufValue(Variant&& _value, int _tagName, KeyValueType itemType, bool isArray);

	template <typename T, typename std::enable_if<(std::is_integral<T>::value || std::is_floating_point<T>::value) &&
												  !std::is_same<T, bool>::value>::type* = nullptr>
	T As(T minv = std::numeric_limits<T>::min(), T maxv = std::numeric_limits<T>::max()) const {
		T v;
		switch (value.Type()) {
			case KeyValueInt:
			case KeyValueInt64:
			case KeyValueDouble:
				v = T(value);
				break;
			default:
				throw reindexer::Error(errParseMsgPack, "Impossible to convert type [%s] to number", Variant::TypeName(value.Type()));
		}
		if (v < minv || v > maxv) throw reindexer::Error(errParams, "Value is out of bounds: [%d,%d]", minv, maxv);
		return v;
	}

	template <typename T, typename std::enable_if<std::is_same<std::string, T>::value ||
												  std::is_same<reindexer::string_view, T>::value>::type* = nullptr>
	T As() const {
		if (value.Type() != KeyValueString) {
			throw reindexer::Error(errParseMsgPack, "Impossible to convert type [%s] to string", Variant::TypeName(value.Type()));
		}
		return T(value);
	}

	template <typename T, typename std::enable_if<std::is_same<T, bool>::value>::type* = nullptr>
	T As() const {
		if (value.Type() != KeyValueBool) {
			throw reindexer::Error(errParseMsgPack, "Impossible to convert type [%s] to bool", Variant::TypeName(value.Type()));
		}
		return T(value);
	}

	bool IsOfPrimitiveType() const {
		return (itemType == KeyValueInt) || (itemType == KeyValueInt64) || (itemType == KeyValueDouble) || (itemType == KeyValueBool);
	}

	Variant value;
	int tagName;
	KeyValueType itemType;
	bool isArray;
};

class ProtobufParser;

struct ProtobufObject {
	ProtobufObject(string_view _buf, const Schema& _schema, TagsPath& tagsPath, TagsMatcher& tm)
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

class ProtobufParser {
public:
	explicit ProtobufParser(ProtobufObject& obj) : object_(obj) {}
	ProtobufParser(const ProtobufParser&) = delete;
	ProtobufParser(ProtobufParser&&) = delete;
	ProtobufParser& operator=(const ProtobufParser&) = delete;
	ProtobufParser& operator=(ProtobufParser&&) = delete;

	ProtobufValue ReadValue();
	Variant ReadArrayItem(int fieldType);

	bool IsEof() const;

private:
	ProtobufObject& object_;
};

}  // namespace reindexer
