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
	void SetTagsMatcher(const TagsMatcher *tm);

	JsonBuilder &operator=(const JsonBuilder &) = delete;
	JsonBuilder &operator=(JsonBuilder &&) = delete;

	/// Start new object
	JsonBuilder Object(const char *name = nullptr);
	JsonBuilder Array(const char *name, bool denseArray = false);

	JsonBuilder &Put(const char *name, const Variant &arg);
	JsonBuilder &Put(const char *name, bool arg);
	JsonBuilder &Put(const char *name, int64_t arg);
	JsonBuilder &Put(const char *name, double arg);
	JsonBuilder &Put(const char *name, const string_view &arg);
	JsonBuilder &Raw(const char *name, const string_view &arg);
	JsonBuilder &Null(const char *name);

	JsonBuilder &Put(const char *name, const char *arg) { return Put(name, string_view(arg)); }
	JsonBuilder &Put(const char *name, int arg) { return Put(name, int64_t(arg)); }
	JsonBuilder &Put(const char *name, size_t arg) { return Put(name, int64_t(arg)); };
	JsonBuilder &Put(const char *name, unsigned arg) { return Put(name, int64_t(arg)); };

	JsonBuilder Object(int tagName) { return Object(tm_->tag2name(tagName).c_str()); }
	JsonBuilder Array(int tagName, bool denseArray = false) { return Array(tm_->tag2name(tagName).c_str(), denseArray); }

	JsonBuilder &Put(int tagName, const Variant &arg) { return Put(tm_->tag2name(tagName).c_str(), arg); }
	JsonBuilder &Put(int tagName, bool arg) { return Put(tm_->tag2name(tagName).c_str(), arg); }
	JsonBuilder &Put(int tagName, int64_t arg) { return Put(tm_->tag2name(tagName).c_str(), arg); }
	JsonBuilder &Put(int tagName, double arg) { return Put(tm_->tag2name(tagName).c_str(), arg); }
	JsonBuilder &Put(int tagName, const string_view &arg) { return Put(tm_->tag2name(tagName).c_str(), arg); }
	JsonBuilder &Raw(int tagName, const string_view &arg) { return Raw(tm_->tag2name(tagName).c_str(), arg); }
	JsonBuilder &Null(int tagName) { return Null(tm_->tag2name(tagName).c_str()); }

	JsonBuilder &Put(int tagName, const char *arg) { return Put(tagName, string_view(arg)); }
	JsonBuilder &Put(int tagName, int arg) { return Put(tagName, int64_t(arg)); }
	JsonBuilder &Put(int tagName, size_t arg) { return Put(tagName, int64_t(arg)); };
	JsonBuilder &Put(int tagName, unsigned arg) { return Put(tagName, int64_t(arg)); };

	JsonBuilder &End();

protected:
	void putName(const char *name);
	WrSerializer *ser_;
	const TagsMatcher *tm_;
	ObjType type_ = TypePlain;
	int count_ = 0;
};

}  // namespace reindexer
