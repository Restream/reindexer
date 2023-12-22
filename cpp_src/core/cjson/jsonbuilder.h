#pragma once

#include "estl/span.h"
#include "objtype.h"
#include "tagslengths.h"
#include "tagsmatcher.h"
#include "vendor/gason/gason.h"

namespace reindexer {
class JsonBuilder {
public:
	JsonBuilder() noexcept : ser_(nullptr), tm_(nullptr) {}
	JsonBuilder(WrSerializer &ser, ObjType type = ObjType::TypeObject, const TagsMatcher *tm = nullptr, bool emitTrailingForFloat = true);
	~JsonBuilder() { End(); }
	JsonBuilder(const JsonBuilder &) = delete;
	JsonBuilder(JsonBuilder &&other) noexcept
		: ser_(other.ser_), tm_(other.tm_), type_(other.type_), count_(other.count_), emitTrailingForFloat_(other.emitTrailingForFloat_) {
		other.type_ = ObjType::TypePlain;
	}
	JsonBuilder &operator=(const JsonBuilder &) = delete;
	JsonBuilder &operator=(JsonBuilder &&) = delete;

	void SetTagsMatcher(const TagsMatcher *tm) noexcept { tm_ = tm; }
	void EmitTrailingForFloat(bool val) noexcept { emitTrailingForFloat_ = val; }

	/// Start new object
	JsonBuilder Object(std::string_view name = {}, int size = KUnknownFieldSize);
	JsonBuilder Object(std::nullptr_t, int size = KUnknownFieldSize) { return Object(std::string_view{}, size); }
	JsonBuilder Object(int tagName, int size = KUnknownFieldSize) { return Object(getNameByTag(tagName), size); }

	JsonBuilder Array(std::string_view name, int size = KUnknownFieldSize);
	JsonBuilder Array(int tagName, int size = KUnknownFieldSize) { return Array(getNameByTag(tagName), size); }

	template <typename T>
	void Array(int tagName, span<T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(tagName);
		for (const auto &d : data) node.Put({}, d);
	}
	template <typename T>
	void Array(std::string_view n, span<T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(n);
		for (const auto &d : data) node.Put({}, d);
	}
	template <typename T>
	void Array(std::string_view n, std::initializer_list<T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(n);
		for (const auto &d : data) node.Put({}, d);
	}

	void Array(int tagName, Serializer &ser, TagType tagType, int count) {
		JsonBuilder node = Array(tagName);
		while (count--) node.Put({}, ser.GetRawVariant(KeyValueType{tagType}));
	}

	JsonBuilder &Put(std::string_view name, const Variant &arg, int offset = 0);
	JsonBuilder &Put(std::nullptr_t, const Variant &arg, int offset = 0) { return Put(std::string_view{}, arg, offset); }
	JsonBuilder &Put(std::string_view name, std::string_view arg, int offset = 0);
	JsonBuilder &Put(std::string_view name, Uuid arg, int offset = 0);
	JsonBuilder &Put(std::nullptr_t, std::string_view arg, int offset = 0) { return Put(std::string_view{}, arg, offset); }
	JsonBuilder &Put(std::string_view name, const char *arg, int offset = 0) { return Put(name, std::string_view(arg), offset); }
	template <typename T, typename std::enable_if<std::is_integral<T>::value>::type * = nullptr>
	JsonBuilder &Put(std::string_view name, const T &arg, int /*offset*/ = 0) {
		putName(name);
		(*ser_) << arg;
		return *this;
	}
	template <typename T, typename std::enable_if<std::is_floating_point<T>::value>::type * = nullptr>
	JsonBuilder &Put(std::string_view name, const T &arg, int /*offset*/ = 0) {
		putName(name);
		if (emitTrailingForFloat_) {
			(*ser_) << arg;
		} else {
			ser_->PutDoubleStrNoTrailing(arg);
		}
		return *this;
	}
	template <typename T>
	JsonBuilder &Put(int tagName, const T &arg, int offset = 0) {
		return Put(getNameByTag(tagName), arg, offset);
	}

	JsonBuilder &Raw(int tagName, std::string_view arg) { return Raw(getNameByTag(tagName), arg); }
	JsonBuilder &Raw(std::string_view name, std::string_view arg);
	JsonBuilder &Raw(std::nullptr_t, std::string_view arg) { return Raw(std::string_view{}, arg); }
	JsonBuilder &Json(std::string_view name, std::string_view arg) { return Raw(name, arg); }
	JsonBuilder &Json(std::nullptr_t, std::string_view arg) { return Raw(std::string_view{}, arg); }

	JsonBuilder &Null(int tagName) { return Null(getNameByTag(tagName)); }
	JsonBuilder &Null(std::string_view name);

	JsonBuilder &End();

protected:
	void putName(std::string_view name);
	std::string_view getNameByTag(int tagName);

	WrSerializer *ser_;
	const TagsMatcher *tm_;
	ObjType type_ = ObjType::TypePlain;
	int count_ = 0;
	bool emitTrailingForFloat_ = true;
};

}  // namespace reindexer
