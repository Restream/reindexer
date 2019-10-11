#pragma once

#include "estl/span.h"
#include "tagsmatcher.h"

namespace reindexer {
class JsonBuilder {
public:
	enum ObjType {
		TypeObject,
		TypeArray,
		TypePlain,
	};

	JsonBuilder() : ser_(nullptr), tm_(nullptr){};
	JsonBuilder(WrSerializer &ser, ObjType type = TypeObject, const TagsMatcher *tm = nullptr);
	~JsonBuilder();
	JsonBuilder(const JsonBuilder &) = delete;
	JsonBuilder(JsonBuilder &&other) : ser_(other.ser_), tm_(other.tm_), type_(other.type_), count_(other.count_) {
		other.type_ = TypePlain;
	}
	JsonBuilder &operator=(const JsonBuilder &) = delete;
	JsonBuilder &operator=(JsonBuilder &&) = delete;

	void SetTagsMatcher(const TagsMatcher *tm);

	/// Start new object
	JsonBuilder Object(string_view name = {});
	JsonBuilder Object(int tagName) { return Object(getNameByTag(tagName)); }

	JsonBuilder Array(string_view name);
	JsonBuilder Array(int tagName) { return Array(getNameByTag(tagName)); }

	template <typename T>
	void Array(int tagName, span<T> data) {
		JsonBuilder node = Array(tagName);
		for (auto d : data) node.Put({}, d);
	}
	template <typename T>
	void Array(string_view n, span<T> data) {
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

	JsonBuilder &Null(int tagName) { return Null(getNameByTag(tagName)); }
	JsonBuilder &Null(string_view name);

	JsonBuilder &End();

protected:
	void putName(string_view name);
	string_view getNameByTag(int tagName);

	WrSerializer *ser_;
	const TagsMatcher *tm_;
	ObjType type_ = TypePlain;
	int count_ = 0;
};

}  // namespace reindexer
