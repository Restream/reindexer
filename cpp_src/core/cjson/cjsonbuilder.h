#pragma once

#include <span>
#include "core/enums.h"
#include "core/keyvalue/p_string.h"
#include "core/type_consts.h"
#include "tagsmatcher.h"

namespace reindexer {
namespace builders {
class [[nodiscard]] CJsonBuilder {
public:
	CJsonBuilder(WrSerializer& ser, ObjType objType = ObjType::TypeObject, const TagsMatcher* tm = nullptr)
		: CJsonBuilder{ser, objType, tm, TagName::Empty()} {}
	CJsonBuilder(WrSerializer&, ObjType, const TagsMatcher*, concepts::TagNameOrIndex auto);
	CJsonBuilder() noexcept : tm_(nullptr), ser_(nullptr), type_(ObjType::TypePlain) {}
	~CJsonBuilder() { End(); }
	CJsonBuilder(const CJsonBuilder&) = delete;
	CJsonBuilder(CJsonBuilder&& other) noexcept
		: tm_(other.tm_), ser_(other.ser_), type_(other.type_), savePos_(other.savePos_), count_(other.count_), itemType_(other.itemType_) {
		other.type_ = ObjType::TypePlain;
	}

	CJsonBuilder& operator=(const CJsonBuilder&) = delete;
	CJsonBuilder& operator=(CJsonBuilder&&) = delete;

	void SetTagsMatcher(const TagsMatcher* tm) noexcept { tm_ = tm; }

	/// Start new object
	CJsonBuilder Object() { return Object(TagName::Empty()); }
	CJsonBuilder Object(concepts::TagNameOrIndex auto);
	CJsonBuilder Array(concepts::TagNameOrIndex auto, ObjType = ObjType::TypeObjectArray);

	[[noreturn]] CJsonBuilder Array(std::string_view name, ObjType type = ObjType::TypeObjectArray) {
		throw Error(errLogic, "CJSON builder doesn't work with string tags [{}, {}]!", name.data(), int(type));
	}

	void Array(concepts::TagNameOrIndex auto tag, std::span<const p_string> data, int /*offset*/ = 0) {
		putTag(tag, TAG_ARRAY);
		ser_->PutCArrayTag(carraytag(data.size(), TAG_STRING));
		for (auto d : data) {
			ser_->PutVString(d);
		}
		++count_;
	}
	void Array(concepts::TagNameOrIndex auto, std::span<const Uuid> data, int offset = 0);
	void Array(concepts::TagNameOrIndex auto tag, std::span<const int> data, int /*offset*/ = 0) {
		putTag(tag, TAG_ARRAY);
		ser_->PutCArrayTag(carraytag(data.size(), TAG_VARINT));
		for (auto d : data) {
			ser_->PutVarint(d);
		}
		++count_;
	}
	void Array(concepts::TagNameOrIndex auto tag, std::span<const int64_t> data, int /*offset*/ = 0) {
		putTag(tag, TAG_ARRAY);
		ser_->PutCArrayTag(carraytag(data.size(), TAG_VARINT));
		for (auto d : data) {
			ser_->PutVarint(d);
		}
		++count_;
	}
	void Array(concepts::TagNameOrIndex auto tag, std::span<const bool> data, int /*offset*/ = 0) {
		putTag(tag, TAG_ARRAY);
		ser_->PutCArrayTag(carraytag(data.size(), TAG_BOOL));
		for (auto d : data) {
			ser_->PutBool(d);
		}
		++count_;
	}
	void Array(concepts::TagNameOrIndex auto tag, std::span<const double> data, int /*offset*/ = 0) {
		putTag(tag, TAG_ARRAY);
		ser_->PutCArrayTag(carraytag(data.size(), TAG_DOUBLE));
		for (auto d : data) {
			ser_->PutDouble(d);
		}
		++count_;
	}
	void Array(concepts::TagNameOrIndex auto tag, std::span<const float> data, int /*offset*/ = 0) {
		putTag(tag, TAG_ARRAY);
		ser_->PutCArrayTag(carraytag(data.size(), TAG_FLOAT));
		for (auto d : data) {
			ser_->PutFloat(d);
		}
		++count_;
	}
	void Array(concepts::TagNameOrIndex auto, Serializer&, TagType, int count);
	void HeteroArray(concepts::TagNameOrIndex auto tag, int count) {
		putTag(tag, TAG_ARRAY);
		ser_->PutCArrayTag(carraytag(count, TAG_OBJECT));
	}

	void Write(std::string_view data) { ser_->Write(data); }

	void Put(concepts::TagNameOrIndex auto, bool, int offset = 0);
	void Put(concepts::TagNameOrIndex auto, int, int offset = 0);
	void Put(concepts::TagNameOrIndex auto, int64_t, int offset = 0);
	void Put(concepts::TagNameOrIndex auto, double, int offset = 0);
	void Put(concepts::TagNameOrIndex auto, float, int offset = 0);
	void Put(concepts::TagNameOrIndex auto, std::string_view, int offset = 0);
	void Put(concepts::TagNameOrIndex auto, Uuid, int offset = 0);
	void Ref(concepts::TagNameOrIndex auto, const KeyValueType&, int field);
	void ArrayRef(concepts::TagNameOrIndex auto, int field, int count);
	void Null(concepts::TagNameOrIndex auto = TagName::Empty());
	void Put(concepts::TagNameOrIndex auto, const Variant& kv, int offset = 0);
	void Put(concepts::TagNameOrIndex auto tag, const char* arg, int offset = 0) { return Put(tag, std::string_view(arg), offset); }
	void End() {
		switch (type_) {
			case ObjType::TypeArray:
				*(reinterpret_cast<carraytag*>(ser_->Buf() + savePos_)) = carraytag(count_, itemType_);
				break;
			case ObjType::TypeObjectArray:
				*(reinterpret_cast<carraytag*>(ser_->Buf() + savePos_)) = carraytag(count_, TAG_OBJECT);
				break;
			case ObjType::TypeObject:
				ser_->PutCTag(kCTagEnd);
				break;
			case ObjType::TypePlain:
				break;
		}
		type_ = ObjType::TypePlain;
	}

	[[nodiscard]] ObjType Type() const noexcept { return type_; }

	template <typename... Args>
	void Object(int, Args...) = delete;
	template <typename... Args>
	void Object(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Array(int, Args...) = delete;
	template <typename... Args>
	void Array(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Put(int, Args...) = delete;
	template <typename... Args>
	void Put(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Null(int, Args...) = delete;
	template <typename... Args>
	void Null(std::nullptr_t, Args...) = delete;

private:
	inline void putTag(TagName tagName, TagType tagType, int field = -1) { ser_->PutCTag(ctag{tagType, tagName, field}); }
	inline void putTag(TagIndex, TagType tagType, int field = -1) { ser_->PutCTag(ctag{tagType, TagName::Empty(), field}); }

	const TagsMatcher* tm_;
	WrSerializer* ser_;
	ObjType type_;
	int savePos_ = 0;
	int count_ = 0;
	TagType itemType_ = TAG_OBJECT;
};

}  // namespace builders
using builders::CJsonBuilder;
}  // namespace reindexer
