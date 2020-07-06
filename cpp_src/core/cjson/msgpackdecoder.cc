#include "msgpackdecoder.h"
#include "core/cjson/objtype.h"
#include "core/cjson/tagsmatcher.h"
#include "vendor/msgpack/msgpack.h"
#include "vendor/msgpack/msgpackparser.h"

namespace reindexer {

MsgPackDecoder::MsgPackDecoder(TagsMatcher* tagsMatcher) : tm_(tagsMatcher) {}

template <typename T>
void MsgPackDecoder::setValue(Payload* pl, CJsonBuilder& builder, const T& value, int tagName) {
	int field = tm_->tags2field(tagsPath_.data(), tagsPath_.size());
	if (field > 0) {
		Variant val(value);
		pl->Set(field, {val}, true);
		builder.Ref(tagName, val, field);
	} else {
		builder.Put(tagName, value);
	}
}

void MsgPackDecoder::iterateOverArray(const msgpack_object* begin, const msgpack_object* end, Payload* pl, CJsonBuilder& array) {
	for (const msgpack_object* p = begin; p != end; ++p) {
		decode(pl, array, *p, 0);
	}
}

void MsgPackDecoder::decode(Payload* pl, CJsonBuilder& builder, const msgpack_object& obj, int tagName) {
	if (tagName) tagsPath_.push_back(tagName);
	switch (obj.type) {
		case MSGPACK_OBJECT_NIL:
			builder.Null(tagName);
			break;
		case MSGPACK_OBJECT_BOOLEAN:
			setValue(pl, builder, obj.via.boolean, tagName);
			break;
		case MSGPACK_OBJECT_POSITIVE_INTEGER:
			setValue(pl, builder, int64_t(obj.via.u64), tagName);
			break;
		case MSGPACK_OBJECT_NEGATIVE_INTEGER:
			setValue(pl, builder, obj.via.i64, tagName);
			break;
		case MSGPACK_OBJECT_FLOAT32:
		case MSGPACK_OBJECT_FLOAT64:
			setValue(pl, builder, double(obj.via.f64), tagName);
			break;
		case MSGPACK_OBJECT_STR:
			setValue(pl, builder, string(obj.via.str.ptr, obj.via.str.size), tagName);
			break;
		case MSGPACK_OBJECT_ARRAY: {
			int count = 0;
			const msgpack_object* begin = obj.via.array.ptr;
			const msgpack_object* end = begin + obj.via.array.size;
			msgpack_object_type prevType = MSGPACK_OBJECT_NIL;
			ObjType type = ObjType::TypeArray;
			for (const msgpack_object* p = begin; p != end; ++p, ++count) {
				if (type != ObjType::TypeObjectArray) {
					if (p != begin && prevType != p->type) {
						type = ObjType::TypeObjectArray;
					}
					prevType = p->type;
				}
			}
			int field = tm_->tags2field(tagsPath_.data(), tagsPath_.size());
			if (field > 0) {
				auto& array = builder.ArrayRef(tagName, field, count);
				iterateOverArray(begin, end, pl, array);
			} else {
				auto array = builder.Array(tagName, type);
				iterateOverArray(begin, end, pl, array);
			}
			break;
		}
		case MSGPACK_OBJECT_MAP: {
			const msgpack_object_kv* begin = obj.via.map.ptr;
			const msgpack_object_kv* end = begin + obj.via.map.size;
			auto object = builder.Object(tagName);
			for (const msgpack_object_kv* p = begin; p != end; ++p) {
				assert(p && p->key.type == MSGPACK_OBJECT_STR);
				int tag = tm_->name2tag(string_view(p->key.via.str.ptr, p->key.via.str.size), true);
				decode(pl, object, p->val, tag);
			}
			break;
		}
		case MSGPACK_OBJECT_BIN:
		case MSGPACK_OBJECT_EXT:
			break;
		default:
			throw Error(errParams, "Unsupported msgpack type: %d, %d", obj.type, obj.via.u64);
	}
	if (tagName) tagsPath_.pop_back();
}

Error MsgPackDecoder::Decode(string_view buf, Payload* pl, WrSerializer& wrser, size_t& offset) {
	try {
		tagsPath_.clear();
		MsgPackParser parser;
		MsgPackValue data = parser.Parse(buf, offset);
		if (!data.p) return Error(errLogic, "Error unpacking msgpack data");
		CJsonBuilder cjsonBuilder(wrser, ObjType::TypePlain, tm_, 0);
		decode(pl, cjsonBuilder, *(data.p), 0);
	} catch (const Error& err) {
		return err;
	}
	return errOK;
}

}  // namespace reindexer
