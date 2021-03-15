#pragma once

#include "core/cjson/objtype.h"
#include "core/cjson/tagslengths.h"
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/p_string.h"
#include "estl/span.h"
#include "estl/string_view.h"
#include "tools/serializer.h"

namespace reindexer {

class Schema;
class TagsMatcher;

const int kNameBit = 0x3;
const int kTypeMask = 0x7;

enum ProtobufTypes {
	PBUF_TYPE_VARINT = 0,
	PBUF_TYPE_FLOAT64 = 1,
	PBUF_TYPE_LENGTHENCODED = 2,
	PBUF_TYPE_FLOAT32 = 5,
};

class ProtobufBuilder {
public:
	ProtobufBuilder()
		: type_(ObjType::TypePlain),
		  ser_(nullptr),
		  tm_(nullptr),
		  tagsPath_(nullptr),
		  schema_(nullptr),
		  sizeHelper_(),
		  itemsFieldIndex_(-1) {}
	ProtobufBuilder(WrSerializer* wrser, ObjType type = ObjType::TypePlain, const Schema* schema = nullptr, const TagsMatcher* tm = nullptr,
					const TagsPath* tagsPath = nullptr, int tagName = -1);
	ProtobufBuilder(ProtobufBuilder&& obj)
		: type_(obj.type_),
		  ser_(obj.ser_),
		  tm_(obj.tm_),
		  tagsPath_(obj.tagsPath_),
		  schema_(obj.schema_),
		  sizeHelper_(std::move(obj.sizeHelper_)),
		  itemsFieldIndex_(obj.itemsFieldIndex_) {}
	ProtobufBuilder(const ProtobufBuilder&) = delete;
	ProtobufBuilder& operator=(ProtobufBuilder&&) = delete;
	ProtobufBuilder& operator=(const ProtobufBuilder&) = delete;
	~ProtobufBuilder() { End(); }

	void SetTagsMatcher(const TagsMatcher* tm) { tm_ = tm; }
	void SetTagsPath(const TagsPath* tagsPath) { tagsPath_ = tagsPath; }

	template <typename T>
	ProtobufBuilder& Put(int fieldIdx, const T& val) {
		put(fieldIdx, val);
		return *this;
	}

	template <typename T>
	ProtobufBuilder& Put(string_view tagName, const T& val) {
		put(tm_->name2tag(tagName), val);
		return *this;
	}

	template <typename T>
	ProtobufBuilder& Null(T) {
		return *this;
	}

	template <typename T, typename std::enable_if<std::is_integral<T>::value || std::is_floating_point<T>::value ||
												  std::is_same<T, bool>::value>::type* = nullptr>
	void Array(int fieldIdx, span<T> data, int /*offset*/ = 0) {
		auto array = ArrayPacked(fieldIdx);
		for (const T& item : data) {
			array.put(0, item);
		}
	}

	template <typename T, typename std::enable_if<std::is_same<reindexer::p_string, T>::value>::type* = nullptr>
	void Array(int fieldIdx, span<T> data, int /*offset*/ = 0) {
		auto array = ArrayNotPacked(fieldIdx);
		for (const T& item : data) {
			array.put(fieldIdx, string_view(item));
		}
	}

	ProtobufBuilder ArrayNotPacked(int fieldIdx) {
		assert(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		return ProtobufBuilder(ser_, ObjType::TypeObjectArray, schema_, tm_, tagsPath_, fieldIdx);
	}

	ProtobufBuilder ArrayPacked(int fieldIdx) {
		assert(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		return ProtobufBuilder(ser_, ObjType::TypeArray, schema_, tm_, tagsPath_, fieldIdx);
	}

	ProtobufBuilder Array(string_view tagName, int size = KUnknownFieldSize) { return Array(tm_->name2tag(tagName), size); }
	ProtobufBuilder Array(int fieldIdx, int = KUnknownFieldSize) { return ArrayNotPacked(fieldIdx); }

	void Array(int fieldIdx, Serializer& rdser, int tagType, int count) {
		if (tagType == TAG_VARINT || tagType == TAG_DOUBLE || tagType == TAG_BOOL) {
			auto array = ArrayPacked(fieldIdx);
			while (count--) packItem(fieldIdx, tagType, rdser, array);
		} else {
			auto array = ArrayNotPacked(fieldIdx);
			while (count--) packItem(fieldIdx, tagType, rdser, array);
		}
	}

	ProtobufBuilder Object(int fieldIdx, int = KUnknownFieldSize);
	ProtobufBuilder Object(string_view tagName, int size = KUnknownFieldSize) { return Object(tm_->name2tag(tagName), size); }

	void End();

private:
	bool getExpectedFieldType(KeyValueType& expectedType);
	void checkIfInconvertibleType(int field, KeyValueType type, KeyValueType first, KeyValueType second);
	void put(int fieldIdx, bool val);
	void put(int fieldIdx, int val);
	void put(int fieldIdx, int64_t val);
	void put(int fieldIdx, double val);
	void put(int fieldIdx, string_view val);
	void put(int fieldIdx, const Variant& val);

	ObjType type_;
	WrSerializer* ser_;
	const TagsMatcher* tm_;
	const TagsPath* tagsPath_;
	const Schema* schema_;
	WrSerializer::VStringHelper sizeHelper_;
	int itemsFieldIndex_;

	int getFieldTag(int fieldIdx) const;
	void putFieldHeader(int fieldIdx, ProtobufTypes type);
	static void packItem(int fieldIdx, int tagType, Serializer& rdser, ProtobufBuilder& array);
};

}  // namespace reindexer
