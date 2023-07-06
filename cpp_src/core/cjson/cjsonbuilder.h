#pragma once

#include "core/keyvalue/p_string.h"
#include "estl/span.h"
#include "objtype.h"
#include "tagsmatcher.h"

namespace reindexer {

void copyCJsonValue(TagType tagType, Serializer &rdser, WrSerializer &wrser);

class CJsonBuilder {
public:
	CJsonBuilder(WrSerializer &ser, ObjType = ObjType::TypeObject, const TagsMatcher *tm = nullptr, int tagName = 0);
	CJsonBuilder() noexcept : tm_(nullptr), ser_(nullptr), type_(ObjType::TypePlain) {}
	~CJsonBuilder() { End(); }
	CJsonBuilder(const CJsonBuilder &) = delete;
	CJsonBuilder(CJsonBuilder &&other) noexcept
		: tm_(other.tm_), ser_(other.ser_), type_(other.type_), savePos_(other.savePos_), count_(other.count_), itemType_(other.itemType_) {
		other.type_ = ObjType::TypePlain;
	}

	CJsonBuilder &operator=(const CJsonBuilder &) = delete;
	CJsonBuilder &operator=(CJsonBuilder &&) = delete;

	void SetTagsMatcher(const TagsMatcher *tm) noexcept { tm_ = tm; }

	/// Start new object
	CJsonBuilder Object(int tagName);
	CJsonBuilder Array(int tagName, ObjType type = ObjType::TypeObjectArray);

	[[noreturn]] CJsonBuilder Array(std::string_view name, ObjType type = ObjType::TypeObjectArray) {
		throw Error(errLogic, "CJSON builder doesn't work with string tags [%s, %d]!", name.data(), int(type));
	}
	CJsonBuilder Object(std::nullptr_t) { return Object(0); }

	void Array(int tagName, span<p_string> data, int /*offset*/ = 0) {
		ser_->PutCTag(ctag{TAG_ARRAY, tagName});
		ser_->PutCArrayTag(carraytag(data.size(), TAG_STRING));
		for (auto d : data) ser_->PutVString(d);
	}
	void Array(int tagName, span<Uuid> data, int offset = 0);
	void Array(int tagName, span<int> data, int /*offset*/ = 0) {
		ser_->PutCTag(ctag{TAG_ARRAY, tagName});
		ser_->PutCArrayTag(carraytag(data.size(), TAG_VARINT));
		for (auto d : data) ser_->PutVarint(d);
	}
	void Array(int tagName, span<int64_t> data, int /*offset*/ = 0) {
		ser_->PutCTag(ctag{TAG_ARRAY, tagName});
		ser_->PutCArrayTag(carraytag(data.size(), TAG_VARINT));
		for (auto d : data) ser_->PutVarint(d);
	}
	void Array(int tagName, span<bool> data, int /*offset*/ = 0) {
		ser_->PutCTag(ctag{TAG_ARRAY, tagName});
		ser_->PutCArrayTag(carraytag(data.size(), TAG_BOOL));
		for (auto d : data) ser_->PutBool(d);
	}
	void Array(int tagName, span<double> data, int /*offset*/ = 0) {
		ser_->PutCTag(ctag{TAG_ARRAY, tagName});
		ser_->PutCArrayTag(carraytag(data.size(), TAG_DOUBLE));
		for (auto d : data) ser_->PutDouble(d);
	}
	void Array(int tagName, Serializer &ser, TagType tagType, int count) {
		ser_->PutCTag(ctag{TAG_ARRAY, tagName});
		ser_->PutCArrayTag(carraytag(count, tagType));
		while (count--) copyCJsonValue(tagType, ser, *ser_);
	}

	template <typename T>
	CJsonBuilder &Put(std::nullptr_t, const T &arg, int offset = 0) {
		return Put(0, arg, offset);
	}

	void Write(std::string_view data) { ser_->Write(data); }

	CJsonBuilder &Null(std::nullptr_t) { return Null(0); }

	CJsonBuilder &Put(int tagName, bool arg, int offset = 0);
	CJsonBuilder &Put(int tagName, int arg, int offset = 0);
	CJsonBuilder &Put(int tagName, int64_t arg, int offset = 0);
	CJsonBuilder &Put(int tagName, double arg, int offset = 0);
	CJsonBuilder &Put(int tagName, std::string_view arg, int offset = 0);
	CJsonBuilder &Put(int tagName, Uuid arg, int offset = 0);
	CJsonBuilder &Ref(int tagName, const Variant &v, int field);
	CJsonBuilder &ArrayRef(int tagName, int field, int count);
	CJsonBuilder &Null(int tagName);
	CJsonBuilder &Put(int tagName, const Variant &kv, int offset = 0);
	CJsonBuilder &Put(int tagName, const char *arg, int offset = 0) { return Put(tagName, std::string_view(arg), offset); }
	CJsonBuilder &End() {
		switch (type_) {
			case ObjType::TypeArray:
				*(reinterpret_cast<carraytag *>(ser_->Buf() + savePos_)) = carraytag(count_, itemType_);
				break;
			case ObjType::TypeObjectArray:
				*(reinterpret_cast<carraytag *>(ser_->Buf() + savePos_)) = carraytag(count_, TAG_OBJECT);
				break;
			case ObjType::TypeObject:
				ser_->PutCTag(kCTagEnd);
				break;
			case ObjType::TypePlain:
				break;
		}
		type_ = ObjType::TypePlain;
		return *this;
	}

	ObjType Type() const noexcept { return type_; }

private:
	inline void putTag(int tagName, TagType tagType) { ser_->PutCTag(ctag{tagType, tagName}); }

	const TagsMatcher *tm_;
	WrSerializer *ser_;
	ObjType type_;
	int savePos_ = 0;
	int count_ = 0;
	TagType itemType_ = TAG_OBJECT;
};

}  // namespace reindexer
