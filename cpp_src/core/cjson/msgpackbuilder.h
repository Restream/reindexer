#pragma once

#include "core/cjson/objtype.h"
#include "core/cjson/tagslengths.h"
#include "core/cjson/tagsmatcher.h"
#include "core/keyvalue/p_string.h"
#include "core/payload/payloadiface.h"
#include "estl/span.h"
#include "vendor/msgpack/msgpack.h"

namespace gason {
struct JsonNode;
}

namespace reindexer {

class MsgPackBuilder {
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
	MsgPackBuilder Raw(std::string_view, std::string_view) noexcept { return MsgPackBuilder(); }
	MsgPackBuilder Raw(std::nullptr_t, std::string_view arg) { return Raw(std::string_view{}, arg); }

	template <typename N, typename T>
	void Array(N tagName, span<T> data, int /*offset*/ = 0) {
		checkIfCorrectArray(tagName);
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (const T& v : data) {
			packValue(v);
		}
	}
	template <typename N>
	void Array(N tagName, span<Uuid> data, int /*offset*/ = 0) {
		checkIfCorrectArray(tagName);
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (Uuid v : data) {
			packValue(v);
		}
	}

	template <typename T>
	void Array(T tagName, span<p_string> data, int /*offset*/ = 0) {
		checkIfCorrectArray(tagName);
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (const p_string& v : data) {
			packValue(std::string_view(v));
		}
	}

	template <typename T>
	MsgPackBuilder Array(T tagName, int size = KUnknownFieldSize) {
		checkIfCorrectArray(tagName);
		packKeyName(tagName);
		if (size == KUnknownFieldSize) {
			assertrx(tagsLengths_ && tagIndex_);
			return MsgPackBuilder(packer_, tagsLengths_, tagIndex_, ObjType::TypeObjectArray, tm_);
		} else {
			return MsgPackBuilder(packer_, ObjType::TypeObjectArray, size);
		}
	}
	void Array(int tagName, Serializer& ser, TagType, int count);

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

	template <typename T>
	MsgPackBuilder& Null(T tagName) {
		skipTag();
		packKeyName(tagName);
		packNil();
		return *this;
	}

	template <typename T, typename N>
	MsgPackBuilder& Put(N tagName, const T& arg, int /*offset*/ = 0) {
		if (isArray()) {
			skipTag();
		}
		skipTag();
		packKeyName(tagName);
		packValue(arg);
		if (isArray()) {
			skipTag();
		}
		return *this;
	}

	template <typename N>
	MsgPackBuilder& Put(N tagName, Uuid arg) {
		if (isArray()) {
			skipTag();
		}
		skipTag();
		packKeyName(tagName);
		packValue(arg);
		if (isArray()) {
			skipTag();
		}
		return *this;
	}

	template <typename T>
	MsgPackBuilder& Put(T tagName, const Variant& kv, int offset = 0) {
		if (isArray()) {
			skipTag();
		}
		skipTag();
		packKeyName(tagName);
		kv.Type().EvaluateOneOf(
			[&](KeyValueType::Int) { packValue(int(kv)); }, [&](KeyValueType::Int64) { packValue(int64_t(kv)); },
			[&](KeyValueType::Double) { packValue(double(kv)); }, [&](KeyValueType::String) { packValue(std::string_view(kv)); },
			[&](KeyValueType::Null) { packNil(); }, [&](KeyValueType::Bool) { packValue(bool(kv)); },
			[&](KeyValueType::Tuple) {
				auto arrNode = Array(tagName);
				for (auto& val : kv.getCompositeValues()) {
					arrNode.Put(0, val, offset);
				}
			},
			[&](KeyValueType::Uuid) { packValue(Uuid{kv}); }, [](OneOf<KeyValueType::Composite, KeyValueType::Undefined>) noexcept {});
		if (isArray()) {
			skipTag();
		}
		return *this;
	}

	MsgPackBuilder& Json(std::string_view name, std::string_view arg);

	MsgPackBuilder& End();

private:
	void init(int size);
	void packCJsonValue(TagType, Serializer&);

	void packNil() { msgpack_pack_nil(&packer_); }
	void packMap(size_t size) { msgpack_pack_map(&packer_, size); }
	void packArray(size_t size) { msgpack_pack_array(&packer_, size); }
	void packValue(int arg) { msgpack_pack_int(&packer_, arg); }
	void packValue(int64_t arg) { msgpack_pack_int64(&packer_, arg); }
	void packValue(double arg) { msgpack_pack_double(&packer_, arg); }

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

	bool isArray() const { return type_ == ObjType::TypeArray || type_ == ObjType::TypeObjectArray; }

	void checkIfCorrectArray(std::string_view) const {}

	void checkIfCorrectArray(int tagName) const {
		if (tagName == 0) {
			throw Error(errLogic, "Arrays of arrays are not supported in cjson");
		}
	}

	void packKeyName(std::nullptr_t) {}
	void packKeyName(std::string_view name) {
		if (!name.empty() && !isArray()) {
			packValue(name);
		}
	}
	void packKeyName(int tagName) {
		if (tagName != 0 && !isArray()) {
			packValue(tm_->tag2name(tagName));
		}
	}

	int getTagSize() {
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

}  // namespace reindexer
