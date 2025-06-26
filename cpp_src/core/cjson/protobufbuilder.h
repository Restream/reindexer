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

enum ProtobufTypes {
	PBUF_TYPE_VARINT = 0,
	PBUF_TYPE_FLOAT64 = 1,
	PBUF_TYPE_LENGTHENCODED = 2,
	PBUF_TYPE_FLOAT32 = 5,
};

class ProtobufBuilder {
public:
	ProtobufBuilder() noexcept
		: type_(ObjType::TypePlain),
		  ser_(nullptr),
		  tm_(nullptr),
		  tagsPath_(nullptr),
		  schema_(nullptr),
		  sizeHelper_(),
		  itemsFieldIndex_(TagName::Empty()) {}
	ProtobufBuilder(WrSerializer*, ObjType = ObjType::TypePlain, const Schema* = nullptr, const TagsMatcher* = nullptr,
					const TagsPath* = nullptr, TagName = TagName::Empty());
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
	ProtobufBuilder& Put(TagName tagName, const T& val, int /*offset*/ = 0) {
		put(tagName, val);
		return *this;
	}
	template <typename T>
	ProtobufBuilder& Put(int tagName, const T& val, int offset = 0) {
		return Put(TagName(tagName), val, offset);
	}

	template <typename T>
	ProtobufBuilder& Put(std::string_view tagName, const T& val, int /*offset*/ = 0) {
		assertrx_throw(tm_);
		put(tm_->name2tag(tagName), val);
		return *this;
	}

	template <typename T>
	ProtobufBuilder& Null(T) noexcept {
		return *this;
	}

	template <typename T, typename std::enable_if<std::is_integral<T>::value || std::is_floating_point<T>::value ||
												  std::is_same<T, bool>::value>::type* = nullptr>
	void Array(TagName tagName, std::span<const T> data, int /*offset*/ = 0) {
		auto array = ArrayPacked(tagName);
		for (const T& item : data) {
			array.put(TagName::Empty(), item);
		}
	}

	template <typename T, typename std::enable_if<std::is_same<reindexer::p_string, T>::value>::type* = nullptr>
	void Array(TagName tagName, std::span<const T> data, int /*offset*/ = 0) {
		auto array = ArrayNotPacked(tagName);
		for (const T& item : data) {
			array.put(tagName, std::string_view(item));
		}
	}
	void Array(TagName tagName, std::span<const Uuid> data, int /*offset*/ = 0) {
		auto array = ArrayNotPacked(tagName);
		for (Uuid item : data) {
			array.put(tagName, item);
		}
	}

	ProtobufBuilder ArrayNotPacked(TagName tagName) {
		assertrx(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		return ProtobufBuilder(ser_, ObjType::TypeObjectArray, schema_, tm_, tagsPath_, tagName);
	}

	ProtobufBuilder ArrayPacked(TagName tagName) {
		assertrx(type_ != ObjType::TypeArray && type_ != ObjType::TypeObjectArray);
		return ProtobufBuilder(ser_, ObjType::TypeArray, schema_, tm_, tagsPath_, tagName);
	}

	ProtobufBuilder Array(std::string_view tagName, int size = KUnknownFieldSize) { return Array(tm_->name2tag(tagName), size); }
	ProtobufBuilder Array(TagName tagName, int = KUnknownFieldSize) { return ArrayNotPacked(tagName); }

	void Array(TagName tagName, Serializer& rdser, TagType tagType, int count) {
		if (tagType == TAG_VARINT || tagType == TAG_DOUBLE || tagType == TAG_BOOL) {
			auto array = ArrayPacked(tagName);
			while (count--) {
				packItem(tagName, tagType, rdser, array);
			}
		} else {
			auto array = ArrayNotPacked(tagName);
			while (count--) {
				packItem(tagName, tagType, rdser, array);
			}
		}
	}
	void Array(int tagName, Serializer& rdser, TagType tagType, int count) { return Array(TagName(tagName), rdser, tagType, count); }

	ProtobufBuilder Object(TagName = TagName::Empty(), int = KUnknownFieldSize);
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
	std::pair<KeyValueType, bool> getExpectedFieldType() const;
	void put(TagName, bool);
	void put(TagName, int);
	void put(TagName, int64_t);
	void put(TagName, double);
	void put(TagName, float);
	void put(TagName, std::string_view);
	void put(TagName, const Variant&);
	void put(TagName, Uuid);

	ObjType type_{ObjType::TypePlain};
	WrSerializer* ser_{nullptr};
	const TagsMatcher* tm_{nullptr};
	const TagsPath* tagsPath_{nullptr};
	const Schema* schema_{nullptr};
	WrSerializer::VStringHelper sizeHelper_;
	TagName itemsFieldIndex_{TagName::Empty()};

	TagName getFieldTag(TagName) const;
	void putFieldHeader(TagName, ProtobufTypes);
	static void packItem(TagName, TagType, Serializer&, ProtobufBuilder& array);
};

}  // namespace reindexer
