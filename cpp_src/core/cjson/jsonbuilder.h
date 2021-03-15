#pragma once

#include "estl/span.h"
#include "objtype.h"
#include "tagslengths.h"
#include "tagsmatcher.h"
#include "vendor/gason/gason.h"

namespace reindexer {
class JsonBuilder {
public:
	JsonBuilder() : ser_(nullptr), tm_(nullptr) {}
	JsonBuilder(WrSerializer &ser, ObjType type = ObjType::TypeObject, const TagsMatcher *tm = nullptr);
	~JsonBuilder();
	JsonBuilder(const JsonBuilder &) = delete;
	JsonBuilder(JsonBuilder &&other) : ser_(other.ser_), tm_(other.tm_), type_(other.type_), count_(other.count_) {
		other.type_ = ObjType::TypePlain;
	}
	JsonBuilder &operator=(const JsonBuilder &) = delete;
	JsonBuilder &operator=(JsonBuilder &&) = delete;

	void SetTagsMatcher(const TagsMatcher *tm) { tm_ = tm; }
	void SetTagsPath(const TagsPath *) {}

	/// Start new object
	JsonBuilder Object(string_view name = {}, int size = KUnknownFieldSize);
	JsonBuilder Object(int tagName, int size = KUnknownFieldSize) { return Object(getNameByTag(tagName), size); }

	JsonBuilder Array(string_view name, int size = KUnknownFieldSize);
	JsonBuilder Array(int tagName, int size = KUnknownFieldSize) { return Array(getNameByTag(tagName), size); }

	template <typename T>
	void Array(int tagName, span<T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(tagName);
		for (auto d : data) node.Put({}, d);
	}
	template <typename T>
	void Array(string_view n, span<T> data, int /*offset*/ = 0) {
		JsonBuilder node = Array(n);
		for (auto d : data) node.Put({}, d);
	}

	void Array(int tagName, Serializer &ser, int tagType, int count) {
		JsonBuilder node = Array(tagName);
		while (count--) node.Put({}, ser.GetRawVariant(KeyValueType(tagType)));
	}

	JsonBuilder &Put(string_view name, const Variant &arg);
	JsonBuilder &Put(string_view name, string_view arg);
	JsonBuilder &Put(string_view name, const char *arg) { return Put(name, string_view(arg)); }
	template <typename T, typename std::enable_if<std::is_integral<T>::value || std::is_floating_point<T>::value>::type * = nullptr>
	JsonBuilder &Put(string_view name, T arg) {
		putName(name);
		(*ser_) << arg;
		return *this;
	}
	template <typename T>
	JsonBuilder &Put(int tagName, const T &arg) {
		return Put(getNameByTag(tagName), arg);
	}

	JsonBuilder &Raw(int tagName, string_view arg) { return Raw(getNameByTag(tagName), arg); }
	JsonBuilder &Raw(string_view name, string_view arg);
	JsonBuilder &Json(string_view name, string_view arg) { return Raw(name, arg); }

	JsonBuilder &Null(int tagName) { return Null(getNameByTag(tagName)); }
	JsonBuilder &Null(string_view name);

	JsonBuilder &End();

protected:
	void putName(string_view name);
	string_view getNameByTag(int tagName);

	WrSerializer *ser_;
	const TagsMatcher *tm_;
	ObjType type_ = ObjType::TypePlain;
	int count_ = 0;
};

}  // namespace reindexer
