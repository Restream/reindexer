#include "msgpackdecoder.h"

#include "core/cjson/objtype.h"
#include "core/cjson/tagsmatcher.h"
#include "tools/flagguard.h"
#include "vendor/msgpack/msgpack.h"

namespace reindexer {

MsgPackDecoder::MsgPackDecoder(TagsMatcher* tagsMatcher) : tm_(tagsMatcher) {}

template <typename T>
void MsgPackDecoder::setValue(Payload& pl, CJsonBuilder& builder, const T& value, int tagName) {
	int field = tm_->tags2field(tagsPath_.data(), tagsPath_.size());
	if (field > 0) {
		const auto& f = pl.Type().Field(field);
		if (isInArray() && !f.IsArray()) {
			throw Error(errLogic, "Error parsing msgpack field '%s' - got array, expected scalar %s", f.Name(), f.Type().Name());
		}
		Variant val(value);
		pl.Set(field, val, true);
		builder.Ref(tagName, val, field);
	} else {
		builder.Put(tagName, value);
	}
}

void MsgPackDecoder::iterateOverArray(const msgpack_object* begin, const msgpack_object* end, Payload& pl, CJsonBuilder& array) {
	for (const msgpack_object* p = begin; p != end; ++p) {
		decode(pl, array, *p, 0);
	}
}

int MsgPackDecoder::decodeKeyToTag(const msgpack_object_kv& obj) {
	using namespace std::string_view_literals;
	switch (obj.key.type) {
		case MSGPACK_OBJECT_BOOLEAN:
			return tm_->name2tag(obj.key.via.boolean ? "true"sv : "false"sv, true);
		case MSGPACK_OBJECT_POSITIVE_INTEGER:
			return tm_->name2tag(std::to_string(obj.key.via.u64), true);
		case MSGPACK_OBJECT_NEGATIVE_INTEGER:
			return tm_->name2tag(std::to_string(obj.key.via.i64), true);
		case MSGPACK_OBJECT_STR:
			return tm_->name2tag(std::string_view(obj.key.via.str.ptr, obj.key.via.str.size), true);
		case MSGPACK_OBJECT_FLOAT32:
		case MSGPACK_OBJECT_FLOAT64:
		case MSGPACK_OBJECT_NIL:
		case MSGPACK_OBJECT_ARRAY:
		case MSGPACK_OBJECT_MAP:
		case MSGPACK_OBJECT_BIN:
		case MSGPACK_OBJECT_EXT:
		default:
			throw Error(errParams, "Unsupported MsgPack map key type: %s(%d)", ToString(obj.key.type), obj.key.type);
	}
}

void MsgPackDecoder::decode(Payload& pl, CJsonBuilder& builder, const msgpack_object& obj, int tagName) {
	if (tagName) tagsPath_.emplace_back(tagName);
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
			setValue(pl, builder, p_string(reinterpret_cast<const l_msgpack_hdr*>(&obj.via.str)), tagName);
			break;
		case MSGPACK_OBJECT_ARRAY: {
			int count = 0;
			CounterGuardIR32 g(arrayLevel_);
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
				// MsgPack can have non-string type keys: https://github.com/msgpack/msgpack/issues/217
				assertrx(p);
				int tag = decodeKeyToTag(*p);
				decode(pl, object, p->val, tag);
			}
			break;
		}
		case MSGPACK_OBJECT_BIN:
		case MSGPACK_OBJECT_EXT:
		default:
			throw Error(errParams, "Unsupported MsgPack type: %s(%d)", ToString(obj.type), obj.type);
	}
	if (tagName) tagsPath_.pop_back();
}

Error MsgPackDecoder::Decode(std::string_view buf, Payload& pl, WrSerializer& wrser, size_t& offset) {
	try {
		tagsPath_.clear();
		size_t baseOffset = offset;
		MsgPackValue data = parser_.Parse(buf, offset);
		if (!data.p) return Error(errLogic, "Error unpacking msgpack data");
		if (data.p->type != MSGPACK_OBJECT_MAP) {
			std::string_view slice = buf.substr(baseOffset, 16);
			return Error(errNotValid, "Unexpected MsgPack value. Expected %s, but got %s(%d) at %d(~>\"%s\"...)",
						 ToString(MSGPACK_OBJECT_MAP), ToString(data.p->type), data.p->type, baseOffset, slice);
		}

		CJsonBuilder cjsonBuilder(wrser, ObjType::TypePlain, tm_, 0);
		decode(pl, cjsonBuilder, *(data.p), 0);
	} catch (const Error& err) {
		return err;
	} catch (const std::exception& ex) {
		return Error(errNotValid, "%s", ex.what());
	} catch (...) {
		// all internal errors shall be handled and converted to Error
		return Error(errNotValid, "Unexpected exception");
	}

	return errOK;
}

constexpr std::string_view ToString(msgpack_object_type type) {
	using namespace std::string_view_literals;

	switch (type) {
		case MSGPACK_OBJECT_NIL:
			return "NIL"sv;
		case MSGPACK_OBJECT_BOOLEAN:
			return "BOOLEAN"sv;
		case MSGPACK_OBJECT_POSITIVE_INTEGER:
			return "POSITIVE_INTEGER"sv;
		case MSGPACK_OBJECT_NEGATIVE_INTEGER:
			return "NEGATIVE_INTEGER"sv;
		case MSGPACK_OBJECT_FLOAT32:
			return "FLOAT32"sv;
		case MSGPACK_OBJECT_FLOAT64:
			return "FLOAT64"sv;
#if defined(MSGPACK_USE_LEGACY_NAME_AS_FLOAT)
		case MSGPACK_OBJECT_DOUBLE: /* obsolete */
			return "DOUBLE"sv;
#endif /* MSGPACK_USE_LEGACY_NAME_AS_FLOAT */
		case MSGPACK_OBJECT_STR:
			return "STR"sv;
		case MSGPACK_OBJECT_ARRAY:
			return "ARRAY"sv;
		case MSGPACK_OBJECT_MAP:
			return "MAP"sv;
		case MSGPACK_OBJECT_BIN:
			return "BIN"sv;
		case MSGPACK_OBJECT_EXT:
			return "EXT"sv;
		default:
			return "UNKNOWN_TYPE"sv;
	}
}

}  // namespace reindexer
