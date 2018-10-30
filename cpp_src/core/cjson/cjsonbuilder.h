#pragma once

#include "core/keyvalue/p_string.h"
#include "tagsmatcher.h"

namespace reindexer {

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser);

class CJsonBuilder {
public:
	enum ObjType {
		TypeObject,
		TypeArray,
		TypePlain,
	};

	CJsonBuilder(WrSerializer &ser, ObjType = TypeObject, TagsMatcher *tm = nullptr, int tagName = 0);
	CJsonBuilder() : tm_(nullptr), ser_(nullptr), type_(TypePlain) {}
	~CJsonBuilder();
	CJsonBuilder(const CJsonBuilder &) = delete;
	CJsonBuilder(CJsonBuilder &&other)
		: tm_(other.tm_), ser_(other.ser_), type_(other.type_), savePos_(other.savePos_), count_(other.count_) {
		other.type_ = TypePlain;
	}

	CJsonBuilder &operator=(const CJsonBuilder &) = delete;
	CJsonBuilder &operator=(CJsonBuilder &&) = delete;

	void SetTagsMatcher(const TagsMatcher *tm);

	/// Start new object
	CJsonBuilder Object(int tagName);
	CJsonBuilder Array(int tagName);

	CJsonBuilder Array(const char *name) { return Array(tm_->name2tag(name, true)); }
	CJsonBuilder Object(const char *name) { return Object(tm_->name2tag(name, true)); }

	void Array(int tagName, span<p_string> data) {
		ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
		ser_->PutUInt32(int(carraytag(data.size(), TAG_STRING)));
		for (auto d : data) ser_->PutVString(d);
	}
	void Array(int tagName, span<int> data) {
		ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
		ser_->PutUInt32(int(carraytag(data.size(), TAG_VARINT)));
		for (auto d : data) ser_->PutVarint(d);
	}
	void Array(int tagName, span<int64_t> data) {
		ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
		ser_->PutUInt32(int(carraytag(data.size(), TAG_VARINT)));
		for (auto d : data) ser_->PutVarint(d);
	}
	void Array(int tagName, span<bool> data) {
		ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
		ser_->PutUInt32(int(carraytag(data.size(), TAG_BOOL)));
		for (auto d : data) ser_->PutBool(d);
	}
	void Array(int tagName, span<double> data) {
		ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
		ser_->PutUInt32(int(carraytag(data.size(), TAG_DOUBLE)));
		for (auto d : data) ser_->PutDouble(d);
	}
	void Array(int tagName, Serializer &ser, int tagType, int count) {
		ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
		ser_->PutUInt32(int(carraytag(count, tagType)));
		while (count--) copyCJsonValue(tagType, ser, *ser_);
	}

	template <typename T>
	CJsonBuilder &Put(const char *name, T arg) {
		return Put(tm_->name2tag(name, true), arg);
	}

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
};

}  // namespace reindexer
