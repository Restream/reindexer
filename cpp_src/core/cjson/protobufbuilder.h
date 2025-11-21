#pragma once

#include <span>
#include <string_view>
#include "core/cjson/tagslengths.h"
#include "core/cjson/tagsmatcher.h"
#include "core/enums.h"
#include "core/keyvalue/p_string.h"
#include "tools/serializer.h"

namespace reindexer {

class Schema;
class TagsMatcher;

constexpr uint64_t kTypeBit = 0x3;
constexpr uint64_t kTypeMask = (uint64_t(1) << kTypeBit) - uint64_t(1);

enum [[nodiscard]] ProtobufTypes {
	PBUF_TYPE_VARINT = 0,
	PBUF_TYPE_FLOAT64 = 1,
	PBUF_TYPE_LENGTHENCODED = 2,
	PBUF_TYPE_FLOAT32 = 5,
};

namespace builders {

class [[nodiscard]] ProtobufBuilder {
public:
	ProtobufBuilder() noexcept
		: type_(ObjType::TypePlain),
		  ser_(nullptr),
		  tm_(nullptr),
		  tagsPath_(nullptr),
		  schema_(nullptr),
		  sizeHelper_(),
		  itemsFieldIndex_(TagName::Empty()) {}
	ProtobufBuilder(WrSerializer* wrser, ObjType objType = ObjType::TypePlain, const Schema* schema = nullptr,
					const TagsMatcher* tm = nullptr, const TagsPath* tagsPath = nullptr)
		: ProtobufBuilder{wrser, objType, schema, tm, tagsPath, TagName::Empty()} {}
	ProtobufBuilder(WrSerializer*, ObjType, const Schema*, const TagsMatcher*, const TagsPath*, concepts::TagNameOrIndex auto);
	ProtobufBuilder(ProtobufBuilder&& obj) noexcept
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

	void SetTagsMatcher(const TagsMatcher* tm) noexcept { tm_ = tm; }
	void SetTagsPath(const TagsPath* tagsPath) noexcept { tagsPath_ = tagsPath; }

	template <typename T>
	void Put(concepts::TagNameOrIndex auto tagName, const T& val, int /*offset*/ = 0) {
		put(tagName, val);
	}
	template <typename T>
	void Put(int tagName, const T& val, int offset = 0) {
		Put(TagName(tagName), val, offset);
	}

	template <typename T>
	void Put(std::string_view tagName, const T& val, int /*offset*/ = 0) {
		assertrx_throw(tm_);
		put(tm_->name2tag(tagName), val);
	}

	template <typename T>
	void Null(T) noexcept {}

	template <typename T, typename std::enable_if<std::is_integral<T>::value || std::is_floating_point<T>::value ||
												  std::is_same<T, bool>::value>::type* = nullptr>
	void Array(concepts::TagNameOrIndex auto tag, std::span<const T> data, int /*offset*/ = 0) {
		auto array = ArrayPacked(tag);
		for (const T& item : data) {
			array.put(TagName::Empty(), item);
		}
	}

	template <typename T, typename std::enable_if<std::is_same<reindexer::p_string, T>::value>::type* = nullptr>
	void Array(concepts::TagNameOrIndex auto tag, std::span<const T> data, int /*offset*/ = 0) {
		auto array = ArrayNotPacked(tag);
		for (const T& item : data) {
			array.put(tag, std::string_view(item));
		}
	}
	void Array(concepts::TagNameOrIndex auto tag, std::span<const Uuid> data, int /*offset*/ = 0) {
		auto array = ArrayNotPacked(tag);
		for (Uuid item : data) {
			array.put(tag, item);
		}
	}

	ProtobufBuilder ArrayNotPacked(concepts::TagNameOrIndex auto tag) {
		assertrx_throw(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		return ProtobufBuilder(ser_, ObjType::TypeObjectArray, schema_, tm_, tagsPath_, tag);
	}

	ProtobufBuilder ArrayPacked(concepts::TagNameOrIndex auto tag) {
		assertrx_throw(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		return ProtobufBuilder(ser_, ObjType::TypeArray, schema_, tm_, tagsPath_, tag);
	}

	ProtobufBuilder Array(std::string_view tagName, int size = KUnknownFieldSize) { return Array(tm_->name2tag(tagName), size); }
	ProtobufBuilder Array(concepts::TagNameOrIndex auto tag, int = KUnknownFieldSize) { return ArrayNotPacked(tag); }

	void Array(concepts::TagNameOrIndex auto tag, Serializer& rdser, TagType tagType, int count) {
		if (tagType == TAG_VARINT || tagType == TAG_DOUBLE || tagType == TAG_FLOAT || tagType == TAG_BOOL) {
			auto array = ArrayPacked(tag);
			while (count--) {
				packItem(tag, tagType, rdser, array);
			}
		} else {
			auto array = ArrayNotPacked(tag);
			while (count--) {
				packItem(tag, tagType, rdser, array);
			}
		}
	}
	void Array(int tag, Serializer& rdser, TagType tagType, int count) { return Array(TagName(tag), rdser, tagType, count); }

	ProtobufBuilder Object() { return Object(TagName::Empty()); }
	ProtobufBuilder Object(concepts::TagNameOrIndex auto tag, int = KUnknownFieldSize);
	ProtobufBuilder Object(std::string_view tagName, int size = KUnknownFieldSize) { return Object(tm_->name2tag(tagName), size); }

	void End();

	template <typename... Args>
	void Object(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Object(int, Args...) = delete;
	template <typename... Args>
	void Array(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Array(int, Args...) = delete;
	template <typename... Args>
	void Put(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(int, Args...) = delete;
	template <typename... Args>
	void Null(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Null(int, Args...) = delete;
	void ArrayPacked(int) = delete;
	void ArrayNotPacked(int) = delete;

private:
	TagName toItemsFieldIndex(TagName tagName) noexcept { return tagName; }
	TagName toItemsFieldIndex(TagIndex) noexcept { return TagName::Empty(); }
	std::string_view tagToName(TagName);
	std::string_view tagToName(TagIndex);
	[[nodiscard]] std::pair<KeyValueType, bool> getExpectedFieldType() const;
	void put(concepts::TagNameOrIndex auto tag, bool val);
	void put(concepts::TagNameOrIndex auto tag, int val);
	void put(concepts::TagNameOrIndex auto tag, int64_t val);
	void put(concepts::TagNameOrIndex auto tag, double val);
	void put(concepts::TagNameOrIndex auto tag, float val);
	void put(concepts::TagNameOrIndex auto tag, std::string_view val);
	void put(concepts::TagNameOrIndex auto tag, const Variant& val);
	void put(concepts::TagNameOrIndex auto tag, Uuid val);

	ObjType type_{ObjType::TypePlain};
	WrSerializer* ser_{nullptr};
	const TagsMatcher* tm_{nullptr};
	const TagsPath* tagsPath_{nullptr};
	const Schema* schema_{nullptr};
	WrSerializer::VStringHelper sizeHelper_;
	TagName itemsFieldIndex_{TagName::Empty()};

	[[nodiscard]] TagName getFieldTag(TagName) const;
	[[nodiscard]] TagName getFieldTag(TagIndex) const { return getFieldTag(TagName::Empty()); }
	void putFieldHeader(concepts::TagNameOrIndex auto, ProtobufTypes);
	static void packItem(concepts::TagNameOrIndex auto, TagType, Serializer&, ProtobufBuilder& array);
};

}  // namespace builders
using builders::ProtobufBuilder;
}  // namespace reindexer
