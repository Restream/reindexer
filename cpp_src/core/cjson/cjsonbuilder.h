#pragma once

#include "tagsmatcher.h"

namespace reindexer {
class CJsonBuilder {
public:
	enum ObjType {
		TypeObject,
		TypeArray,
		TypePlain,
	};

	CJsonBuilder(WrSerializer &ser, ObjType = TypeObject, TagsMatcher *tm = nullptr, int tagName = 0, bool denseArray = false);
	CJsonBuilder() : tm_(nullptr), ser_(nullptr), type_(TypePlain) {}
	~CJsonBuilder();
	CJsonBuilder(const CJsonBuilder &) = delete;
	CJsonBuilder(CJsonBuilder &&other)
		: tm_(other.tm_),
		  ser_(other.ser_),
		  type_(other.type_),
		  savePos_(other.savePos_),
		  count_(other.count_),
		  lastTag_(other.lastTag_),
		  denseArray_(other.denseArray_) {
		other.type_ = TypePlain;
	}

	CJsonBuilder &operator=(const CJsonBuilder &) = delete;
	CJsonBuilder &operator=(CJsonBuilder &&) = delete;

	void SetTagsMatcher(const TagsMatcher *tm);

	/// Start new object
	CJsonBuilder Object(int tagName);
	CJsonBuilder Array(int tagName, bool dense = false);

	CJsonBuilder Array(const char *name) { return Array(tm_->name2tag(name, true)); }
	CJsonBuilder Object(const char *name) { return Object(tm_->name2tag(name, true)); }

	CJsonBuilder &Put(const char *name, const Variant &kv) { return Put(tm_->name2tag(name, true), kv); }

	CJsonBuilder &Put(const char *name, bool arg) { return Put(tm_->name2tag(name, true), arg); }
	CJsonBuilder &Put(const char *name, int arg) { return Put(tm_->name2tag(name, true), arg); }
	CJsonBuilder &Put(const char *name, int64_t arg) { return Put(tm_->name2tag(name, true), arg); }
	CJsonBuilder &Put(const char *name, double arg) { return Put(tm_->name2tag(name, true), arg); }
	CJsonBuilder &Put(const char *name, const string_view &arg) { return Put(tm_->name2tag(name, true), arg); }
	CJsonBuilder &Put(const char *name, const char *arg) { return Put(tm_->name2tag(name, true), arg); }
	CJsonBuilder &Null(const char *name) { return Null(tm_->name2tag(name, true)); }
	CJsonBuilder &Ref(const char *name, int type, int field) { return Ref(tm_->name2tag(name, true), type, field); }

	CJsonBuilder &Put(int tagName, bool arg);
	CJsonBuilder &Put(int tagName, int arg);
	CJsonBuilder &Put(int tagName, int64_t arg);
	CJsonBuilder &Put(int tagName, double arg);
	CJsonBuilder &Put(int tagName, const string_view &arg);
	CJsonBuilder &Ref(int tagName, int type, int field);
	CJsonBuilder &Ref(int tagName, const Variant &v, int field);
	CJsonBuilder &ArrayRef(int tagName, int field, int count);
	CJsonBuilder &Null(int tagName);
	CJsonBuilder &Put(int tagName, const Variant &kv);
	CJsonBuilder &Put(int tagName, const char *arg) { return Put(tagName, string_view(arg)); };

	CJsonBuilder &End();

protected:
	inline void putTag(int tag, int tagType);
	TagsMatcher *tm_;
	WrSerializer *ser_;
	ObjType type_;
	int savePos_ = 0;
	int count_ = 0;
	int lastTag_ = 0;
	bool denseArray_ = false;
};

}  // namespace reindexer
