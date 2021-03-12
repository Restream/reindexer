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
	MsgPackBuilder(WrSerializer &wrser, ObjType type, size_t size);
	MsgPackBuilder(msgpack_packer &packer, ObjType type, size_t size);
	MsgPackBuilder(WrSerializer &wrser, const TagsLengths *tagsLengths, int *startTag, ObjType = ObjType::TypeObject,
				   const TagsMatcher *tm = nullptr);
	MsgPackBuilder(msgpack_packer &packer, const TagsLengths *tagsLengths, int *startTag, ObjType = ObjType::TypeObject,
				   const TagsMatcher *tm = nullptr);
	MsgPackBuilder() : tm_(nullptr), packer_(), tagsLengths_(nullptr), type_(ObjType::TypePlain), tagIndex_(nullptr) {}
	~MsgPackBuilder();
	MsgPackBuilder(MsgPackBuilder &&other)
		: tm_(other.tm_), packer_(other.packer_), tagsLengths_(other.tagsLengths_), type_(other.type_), tagIndex_(other.tagIndex_) {}

	MsgPackBuilder(const MsgPackBuilder &) = delete;
	MsgPackBuilder &operator=(const MsgPackBuilder &) = delete;
	MsgPackBuilder &operator=(MsgPackBuilder &&) = delete;

	void SetTagsMatcher(const TagsMatcher *tm) { tm_ = tm; }
	void SetTagsPath(const TagsPath *) {}

	template <typename N, typename T>
	void Array(N tagName, span<T> data, int /*offset*/ = 0) {
		checkIfCorrectArray(tagName);
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (const T &v : data) packValue(v);
	}

	template <typename T>
	void Array(T tagName, span<p_string> data, int /*offset*/ = 0) {
		checkIfCorrectArray(tagName);
		skipTag();
		packKeyName(tagName);
		packArray(data.size());
		for (const p_string &v : data) packValue(string_view(v));
	}

	template <typename T>
	MsgPackBuilder Array(T tagName, int size = KUnknownFieldSize) {
		checkIfCorrectArray(tagName);
		packKeyName(tagName);
		if (size == KUnknownFieldSize) {
			assert(tagsLengths_ && tagIndex_);
			return MsgPackBuilder(packer_, tagsLengths_, tagIndex_, ObjType::TypeObjectArray, tm_);
		} else {
			return MsgPackBuilder(packer_, ObjType::TypeObjectArray, size);
		}
	}
	void Array(int tagName, Serializer &ser, int tagType, int count);

	template <typename T>
	MsgPackBuilder Object(T tagName, int size = KUnknownFieldSize) {
		packKeyName(tagName);
		if (isArray()) skipTag();
		if (size == KUnknownFieldSize) {
			assert(tagsLengths_ && tagIndex_);
			return MsgPackBuilder(packer_, tagsLengths_, tagIndex_, ObjType::TypeObject, tm_);
		} else {
			return MsgPackBuilder(packer_, ObjType::TypeObject, size);
		}
	}

	template <typename T>
	MsgPackBuilder &Null(T tagName) {
		skipTag();
		packKeyName(tagName);
		packNil();
		return *this;
	}

	template <typename T, typename N>
	MsgPackBuilder &Put(N tagName, T arg) {
		if (isArray()) skipTag();
		skipTag();
		packKeyName(tagName);
		packValue(arg);
		if (isArray()) skipTag();
		return *this;
	}

	template <typename T>
	MsgPackBuilder &Put(T tagName, const Variant &kv) {
		if (isArray()) skipTag();
		skipTag();
		packKeyName(tagName);
		switch (kv.Type()) {
			case KeyValueInt:
				packValue(int(kv));
				break;
			case KeyValueInt64:
				packValue(int64_t(kv));
				break;
			case KeyValueDouble:
				packValue(double(kv));
				break;
			case KeyValueString:
				packValue(p_string(kv).toString());
				break;
			case KeyValueNull:
				packNil();
				break;
			case KeyValueBool:
				packValue(bool(kv));
				break;
			case KeyValueTuple: {
				auto arrNode = Array(tagName);
				for (auto &val : kv.getCompositeValues()) {
					arrNode.Put(0, val);
				}
				break;
			}
			default:
				break;
		}
		if (isArray()) skipTag();
		return *this;
	}

	MsgPackBuilder &Json(string_view name, string_view arg);

	MsgPackBuilder &End();

protected:
	void init(int size);
	void packCJsonValue(int tagType, Serializer &rdser);

	void packNil() { msgpack_pack_nil(&packer_); }
	void packMap(size_t size) { msgpack_pack_map(&packer_, size); }
	void packArray(size_t size) { msgpack_pack_array(&packer_, size); }
	void packValue(int arg) { msgpack_pack_int(&packer_, arg); }
	void packValue(int64_t arg) { msgpack_pack_int64(&packer_, arg); }
	void packValue(double arg) { msgpack_pack_double(&packer_, arg); }

	void packValue(const string &arg) {
		msgpack_pack_str(&packer_, arg.size());
		msgpack_pack_str_body(&packer_, arg.data(), arg.length());
	}
	void packValue(string_view arg) {
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

	bool isArray() const { return type_ == ObjType::TypeArray || type_ == ObjType::TypeObjectArray; }

	void checkIfCorrectArray(string_view) const {}

	void checkIfCorrectArray(int tagName) const {
		if (tagName == 0) throw Error(errLogic, "Arrays of arrays are not supported in cjson");
	}

	void packKeyName(string_view name) {
		if (!name.empty() && !isArray()) packValue(name);
	}
	void packKeyName(int tagName) {
		if (tagName != 0 && !isArray()) packValue(tm_->tag2name(tagName));
	}

	int getTagSize() { return (*tagsLengths_)[(*tagIndex_)++]; }

	void skipTag() {
		if (tagsLengths_) ++(*tagIndex_);
	}

	void skipTagIfEqual(TagValues tagVal) {
		if ((tagsLengths_ && tagIndex_) && (*tagsLengths_)[(*tagIndex_)] == tagVal) {
			skipTag();
		}
	}

	void appendJsonObject(string_view name, const gason::JsonNode &obj);

	const TagsMatcher *tm_;
	msgpack_packer packer_;
	const TagsLengths *tagsLengths_;
	ObjType type_;
	int *tagIndex_;
};

}  // namespace reindexer
