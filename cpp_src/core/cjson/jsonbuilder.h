#pragma once

#include <span>
#include <sstream>
#include "tagslengths.h"
#include "tagsmatcher.h"

namespace reindexer {
namespace builders {
class [[nodiscard]] JsonBuilder {
public:
	JsonBuilder() noexcept : ser_(nullptr), tm_(nullptr) {}
	JsonBuilder(WrSerializer& ser, ObjType type = ObjType::TypeObject, const TagsMatcher* tm = nullptr, bool emitTrailingForFloat = true);
	~JsonBuilder() { End(); }
	JsonBuilder(const JsonBuilder&) = delete;
	JsonBuilder(JsonBuilder&& other) noexcept
		: ser_(other.ser_), tm_(other.tm_), type_(other.type_), count_(other.count_), emitTrailingForFloat_(other.emitTrailingForFloat_) {
		other.type_ = ObjType::TypePlain;
	}
	JsonBuilder& operator=(const JsonBuilder&) = delete;
	JsonBuilder& operator=(JsonBuilder&&) = delete;

	void SetTagsMatcher(const TagsMatcher* tm) noexcept { tm_ = tm; }
	void EmitTrailingForFloat(bool val) noexcept { emitTrailingForFloat_ = val; }

	/// Start new object
	JsonBuilder Object(std::string_view name = {}, int size = KUnknownFieldSize);
	JsonBuilder Object(concepts::TagNameOrIndex auto tag, int size = KUnknownFieldSize) { return Object(getNameByTag(tag), size); }

	void AddArray(std::string_view name, int size = KUnknownFieldSize) { std::ignore = Array(name, size); }
	JsonBuilder Array(std::string_view name, int size = KUnknownFieldSize);
	JsonBuilder Array(concepts::TagNameOrIndex auto tag, int size = KUnknownFieldSize) { return Array(getNameByTag(tag), size); }

	template <typename T>
	void Array(concepts::TagNameOrIndex auto tag, std::span<const T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(tag);
		for (const auto& d : data) {
			node.Put(TagName::Empty(), d);
		}
	}
	template <typename T>
	void Array(std::string_view n, std::span<const T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(n);
		for (const auto& d : data) {
			node.Put(TagName::Empty(), d);
		}
	}
	template <typename T>
	void Array(std::string_view n, std::initializer_list<T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(n);
		for (const auto& d : data) {
			node.Put(TagName::Empty(), d);
		}
	}

	void Array(concepts::TagNameOrIndex auto tag, Serializer& ser, TagType tagType, int count) {
		JsonBuilder node = Array(tag);
		const KeyValueType kvt{tagType};
		while (count--) {
			node.Put(TagName::Empty(), ser.GetRawVariant(kvt));
		}
	}

	void Put(std::string_view name, const Variant& arg, int offset = 0);
	void Put(std::string_view name, std::string_view arg, int offset = 0);
	void Put(std::string_view name, Uuid arg, int offset = 0);
	void Put(std::string_view name, const char* arg, int offset = 0) { return Put(name, std::string_view(arg), offset); }
	template <typename T, typename std::enable_if<std::is_integral<T>::value>::type* = nullptr>
	void Put(std::string_view name, const T& arg, int /*offset*/ = 0) {
		putName(name);
		(*ser_) << arg;
	}
	template <typename T, typename std::enable_if<std::is_floating_point<T>::value>::type* = nullptr>
	void Put(std::string_view name, const T& arg, int /*offset*/ = 0) {
		putName(name);
		if (emitTrailingForFloat_) {
			(*ser_) << arg;
		} else {
			ser_->PutFPStrNoTrailing(arg);
		}
	}
	template <typename T>
	void Put(concepts::TagNameOrIndex auto tag, const T& arg, int offset = 0) {
		Put(getNameByTag(tag), arg, offset);
	}

	template <typename T, std::enable_if_t<!std::is_arithmetic_v<T> && !std::is_constructible_v<std::string_view, T>>* = nullptr>
	void Put(std::string_view name, const T& arg, int /*offset*/ = 0) {
		putName(name);
		std::ostringstream sstream;
		sstream << arg;
		ser_->PrintJsonString(sstream.str());
	}

	void Raw(concepts::TagNameOrIndex auto tag, std::string_view arg) { return Raw(getNameByTag(tag), arg); }
	void Raw(std::string_view name, std::string_view arg);
	void Raw(std::string_view arg) { return Raw(std::string_view{}, arg); }
	void Json(std::string_view name, std::string_view arg) { return Raw(name, arg); }
	void Json(std::string_view arg) { return Raw(arg); }

	void Null(concepts::TagNameOrIndex auto tag) { return Null(getNameByTag(tag)); }
	void Null(std::string_view name);

	void End();

	template <typename... Args>
	void Object(int, Args...) = delete;
	template <typename... Args>
	void Object(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Array(int, Args...) = delete;
	template <typename... Args>
	void Array(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Null(int, Args...) = delete;
	template <typename... Args>
	void Null(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Raw(int, Args...) = delete;
	template <typename... Args>
	void Raw(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(int, Args...) = delete;
	template <typename... Args>
	void Put(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Json(int, Args...) = delete;
	template <typename... Args>
	void Json(std::nullptr_t, Args...) = delete;

private:
	void putName(std::string_view name);
	[[nodiscard]] std::string_view getNameByTag(TagName);
	[[nodiscard]] std::string_view getNameByTag(TagIndex) noexcept { return {}; }

	WrSerializer* ser_;
	const TagsMatcher* tm_;
	ObjType type_ = ObjType::TypePlain;
	int count_ = 0;
	bool emitTrailingForFloat_ = true;
};

}  // namespace builders
using builders::JsonBuilder;
}  // namespace reindexer
