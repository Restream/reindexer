#pragma once

#include <span>
#include "core/cjson/tagslengths.h"
#include "core/cjson/tagsmatcher.h"
#include "core/enums.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "vendor/msgpack/msgpack.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {
namespace builders {
class [[nodiscard]] MsgPackBuilder {
public:
	MsgPackBuilder(WrSerializer& wrser, ObjType type, size_t size);
	MsgPackBuilder(msgpack_packer& packer, ObjType type, size_t size);
	MsgPackBuilder(WrSerializer& wrser, const TagsLengths* tagsLengths, int* startTag, ObjType = ObjType::TypeObject,
				   const TagsMatcher* tm = nullptr);
	MsgPackBuilder(msgpack_packer& packer, const TagsLengths* tagsLengths, int* startTag, ObjType = ObjType::TypeObject,
				   const TagsMatcher* tm = nullptr);
	MsgPackBuilder() noexcept : tm_(nullptr), packer_(), tagsLengths_(nullptr), type_(ObjType::TypePlain), tagIndex_(nullptr) {}
	~MsgPackBuilder() { End(); }
	MsgPackBuilder(MsgPackBuilder&& other) noexcept
		: tm_(other.tm_), packer_(other.packer_), tagsLengths_(other.tagsLengths_), type_(other.type_), tagIndex_(other.tagIndex_) {}

	MsgPackBuilder(const MsgPackBuilder&) = delete;
	MsgPackBuilder& operator=(const MsgPackBuilder&) = delete;
	MsgPackBuilder& operator=(MsgPackBuilder&&) = delete;

	void SetTagsMatcher(const TagsMatcher* tm) noexcept { tm_ = tm; }

	void Raw(std::string_view, std::string_view) noexcept {}
	void Raw(std::string_view) noexcept {}

	template <typename N, typename T>
	void Array(N tagName, std::span<T> data, int /*offset*/ = 0) {
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (const T& v : data) {
			packValue(v);
		}
	}
	template <typename N>
	void Array(N tagName, std::span<Uuid> data, int /*offset*/ = 0) {
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (Uuid v : data) {
			packValue(v);
		}
	}

	template <typename T>
	void Array(T tagName, std::span<p_string> data, int /*offset*/ = 0) {
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (const p_string& v : data) {
			packValue(std::string_view(v));
		}
	}

	template <typename T>
	MsgPackBuilder Array(T tagName, int size = KUnknownFieldSize) {
		packKeyName(tagName);
		if (size == KUnknownFieldSize) {
			assertrx(tagsLengths_ && tagIndex_);
			return MsgPackBuilder(packer_, tagsLengths_, tagIndex_, ObjType::TypeObjectArray, tm_);
		} else {
			return MsgPackBuilder(packer_, ObjType::TypeObjectArray, size);
		}
	}
	void Array(concepts::TagNameOrIndex auto, Serializer&, TagType, int count);

	template <typename T>
	MsgPackBuilder Object(T tagName, int size = KUnknownFieldSize) {
		packKeyName(tagName);
		if (isArray()) {
			skipTag();
		}
		if (size == KUnknownFieldSize) {
			assertrx(tagsLengths_ && tagIndex_);
			return MsgPackBuilder(packer_, tagsLengths_, tagIndex_, ObjType::TypeObject, tm_);
		} else {
			return MsgPackBuilder(packer_, ObjType::TypeObject, size);
		}
	}
	MsgPackBuilder Object() { return Object(std::string_view{}); }

	template <typename T>
	void Null(T tagName) {
		skipTag();
		packKeyName(tagName);
		packNil();
	}

	template <typename T, typename N>
	void Put(N tagName, const T& arg, int /*offset*/ = 0) {
		if (isArray()) {
			skipTag();
		}
		skipTag();
		packKeyName(tagName);
		packValue(arg);
		if (isArray()) {
			skipTag();
		}
	}

	template <typename N>
	void Put(N tagName, Uuid arg) {
		if (isArray()) {
			skipTag();
		}
		skipTag();
		packKeyName(tagName);
		packValue(arg);
		if (isArray()) {
			skipTag();
		}
	}

	template <typename T>
	void Put(T tagName, const Variant& kv, int offset = 0) {
		if (isArray()) {
			skipTag();
		}
		skipTag();
		packKeyName(tagName);
		kv.Type().EvaluateOneOf(
			[&](KeyValueType::Int) { packValue(int(kv)); }, [&](KeyValueType::Int64) { packValue(int64_t(kv)); },
			[&](KeyValueType::Double) { packValue(double(kv)); }, [&](KeyValueType::Float) { packValue(float(kv)); },
			[&](KeyValueType::String) { packValue(std::string_view(kv)); }, [&](KeyValueType::Null) { packNil(); },
			[&](KeyValueType::Bool) { packValue(bool(kv)); },
			[&](KeyValueType::Tuple) {
				auto arrNode = Array(tagName);
				for (auto& val : kv.getCompositeValues()) {
					arrNode.Put(TagName::Empty(), val, offset);
				}
			},
			[&](KeyValueType::Uuid) { packValue(Uuid{kv}); },
			[](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto) noexcept {
				assertrx_throw(false);
			});
		if (isArray()) {
			skipTag();
		}
	}

	void Json(std::string_view name, std::string_view arg);
	void Json(std::string_view arg) { Json(std::string_view{}, arg); }

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
	void Put(int, Args...) = delete;
	template <typename... Args>
	void Put(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Null(int, Args...) = delete;
	template <typename... Args>
	void Null(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Raw(std::nullptr_t, Args...) = delete;
	template <typename... Args>
	void Raw(int, Args...) = delete;
	template <typename... Args>
	void Json(int, Args...) = delete;
	template <typename... Args>
	void Json(std::nullptr_t, Args...) = delete;

private:
	void init(int size);
	void packCJsonValue(TagType, Serializer&);

	void packNil() { msgpack_pack_nil(&packer_); }
	void packMap(size_t size) { msgpack_pack_map(&packer_, size); }
	void packArray(size_t size) { msgpack_pack_array(&packer_, size); }
	void packValue(int arg) { msgpack_pack_int(&packer_, arg); }
	void packValue(int64_t arg) { msgpack_pack_int64(&packer_, arg); }
	void packValue(double arg) { msgpack_pack_double(&packer_, arg); }
	void packValue(float arg) { msgpack_pack_float(&packer_, arg); }

	void packValue(std::string_view arg) {
		msgpack_pack_str(&packer_, arg.size());
		msgpack_pack_str_body(&packer_, arg.data(), arg.length());
	}
	void packValue(bool arg) {
		if (arg) {
			msgpack_pack_true(&packer_);
		} else {
			msgpack_pack_false(&packer_);
		}
	}
	void packValue(Uuid arg) {
		thread_local char buf[Uuid::kStrFormLen];
		arg.PutToStr(buf);
		packValue(std::string_view{buf, Uuid::kStrFormLen});
	}

	[[nodiscard]] bool isArray() const { return type_ == ObjType::TypeArray || type_ == ObjType::TypeObjectArray; }

	void packKeyName(std::nullptr_t) = delete;
	void packKeyName(std::string_view name) {
		if (!name.empty() && !isArray()) {
			packValue(name);
		}
	}
	void packKeyName(TagName tagName) {
		if (!tagName.IsEmpty() && !isArray()) {
			packValue(tm_->tag2name(tagName));
		}
	}
	void packKeyName(TagIndex) {}

	[[nodiscard]] int getTagSize() {
		if (tagsLengths_) {
			return (*tagsLengths_)[(*tagIndex_)++];
		}
		throw Error(errLogic, "Tags length is not initialized");
	}

	void skipTag() {
		if (tagsLengths_) {
			++(*tagIndex_);
		}
	}

	void skipTagIfEqual(TagValues tagVal) {
		if (tagsLengths_ && tagIndex_ && unsigned(*tagIndex_) < tagsLengths_->size() && (*tagsLengths_)[(*tagIndex_)] == tagVal) {
			skipTag();
		}
	}

	void appendJsonObject(std::string_view name, const gason::JsonNode& obj);

	const TagsMatcher* tm_;
	msgpack_packer packer_;
	const TagsLengths* tagsLengths_;
	ObjType type_;
	int* tagIndex_;
};
}  // namespace builders
using builders::MsgPackBuilder;
}  // namespace reindexer
