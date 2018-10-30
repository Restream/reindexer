#pragma once

#include "tagsmatcher.h"

namespace reindexer {
class JsonBuilder {
public:
	enum ObjType {
		TypeObject,
		TypeArray,
		TypePlain,
	};

	JsonBuilder() : ser_(nullptr){};
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
	JsonBuilder Object(const char *name = nullptr);
	JsonBuilder Object(int tagName) { return Object(tm_->tag2name(tagName).c_str()); }

	JsonBuilder Array(const char *name);
	JsonBuilder Array(int tagName) { return Array(tm_->tag2name(tagName).c_str()); }

	template <typename T>
	void Array(int tagName, span<T> data) {
		JsonBuilder node = Array(tagName);
		for (auto d : data) node.Put(nullptr, d);
	}

	void Array(int tagName, Serializer &ser, int tagType, int count) {
		JsonBuilder node = Array(tagName);
		while (count--) node.Put(nullptr, ser.GetRawVariant(KeyValueType(tagType)));
	}

	JsonBuilder &Put(const char *name, const Variant &arg);
	JsonBuilder &Put(const char *name, const string_view &arg);
	JsonBuilder &Put(const char *name, const char *arg) { return Put(name, string_view(arg)); }
	template <typename T, typename std::enable_if<std::is_integral<T>::value || std::is_floating_point<T>::value>::type * = nullptr>
	JsonBuilder &Put(const char *name, T arg) {
		putName(name);
		(*ser_) << arg;
		return *this;
	}
	template <typename T>
	JsonBuilder &Put(int tagName, const T &arg) {
		return Put(tm_->tag2name(tagName).c_str(), arg);
	}

	JsonBuilder &Raw(int tagName, const string_view &arg) { return Raw(tm_->tag2name(tagName).c_str(), arg); }
	JsonBuilder &Raw(const char *name, const string_view &arg);

	JsonBuilder &Null(int tagName) { return Null(tm_->tag2name(tagName).c_str()); }
	JsonBuilder &Null(const char *name);

	JsonBuilder &End();

protected:
	void putName(const char *name);
	WrSerializer *ser_;
	const TagsMatcher *tm_;
	ObjType type_ = TypePlain;
	int count_ = 0;
};

}  // namespace reindexer
