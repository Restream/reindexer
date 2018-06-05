#include "jsondecoder.h"
#include "tagsmatcher.h"
#include "tools/json2kv.h"
#include "tools/serializer.h"

namespace reindexer {

JsonDecoder::JsonDecoder(TagsMatcher &tagsMatcher) : tagsMatcher_(tagsMatcher) {}

Error JsonDecoder::Decode(Payload *pl, WrSerializer &wrser, JsonValue &v) {
	try {
		wrser.Reset();
		tagsPath_.clear();

		decodeJson(pl, wrser, v, 0);
	}

	catch (const Error &err) {
		return err;
	}
	return 0;
}

void JsonDecoder::decodeJsonObject(Payload *pl, WrSerializer &wrser, JsonValue &v) {
	// int l = jsonPath_.length();
	for (auto elem : v) {
#if 0
		if (l) jsonPath_.append(".");
		jsonPath_.append(elem->key, strlen(elem->key));
		int subtag = tagsMatcher_->name2tag(elem->key, jsonPath_);
		int field = tagsMatcher_->tag2field(subtag);
#else
		int tagName = tagsMatcher_.name2tag(elem->key);
		if (!tagName) {
			tagName = tagsMatcher_.name2tag(elem->key, true);
		}
		assert(tagName);
		tagsPath_.push_back(tagName);
		int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());
#endif
		if (field < 0)
			decodeJson(pl, wrser, elem->value, tagName);
		else {
			// Indexed field. extract it
			KeyRefs kvs;
			int count = 0;
			auto &f = pl->Type().Field(field);
			if (elem->value.getTag() == JSON_ARRAY) {
				if (!f.IsArray()) {
					throw Error(errLogic, "Error parsing json field '%s' - got array, expected scalar %s", f.Name().c_str(),
								KeyValue::TypeName(f.Type()));
				}
				for (auto subelem : elem->value) {
					kvs.push_back(jsonValue2KeyRef(subelem->value, f.Type(), f.Name().c_str()));
					++count;
				}
			} else if (elem->value.getTag() != JSON_NULL) {
				kvs.push_back(jsonValue2KeyRef(elem->value, f.Type(), f.Name().c_str()));
			}
			if (!kvs.empty()) {
				pl->Set(field, kvs, true);
			}

			// Put special tag :link data to indexed field
			switch (elem->value.getTag()) {
				case JSON_NUMBER: {
					double value = elem->value.toNumber(), intpart;
					if (std::modf(value, &intpart) == 0.0) {
						wrser.PutVarUint(ctag(TAG_VARINT, tagName, field));
					} else {
						wrser.PutVarUint(ctag(TAG_DOUBLE, tagName, field));
					}
				} break;
				case JSON_STRING:
					wrser.PutVarUint(ctag(TAG_STRING, tagName, field));
					break;
				case JSON_TRUE:
				case JSON_FALSE:
					wrser.PutVarUint(ctag(TAG_BOOL, tagName, field));
					break;
				case JSON_ARRAY:
					wrser.PutVarUint(ctag(TAG_ARRAY, tagName, field));
					wrser.PutVarUint(count);
					break;
				case JSON_NULL:
					wrser.PutVarUint(ctag(TAG_NULL, tagName));
					break;
				default:
					wrser.PutVarUint(ctag(TAG_NULL, tagName));
					break;
			}
		}
		// jsonPath_.resize(l);
		tagsPath_.pop_back();
	}
}

// Split original JSON into 2 parts:
// 1. PayloadFields - fields from json found by 'jsonPath' tags
// 2. stripped binary packed JSON without fields values found by 'jsonPath' tags
void JsonDecoder::decodeJson(Payload *pl, WrSerializer &wrser, JsonValue &v, int tagName) {
	switch (v.getTag()) {
		case JSON_NUMBER: {
			double value = v.toNumber(), intpart;
			if (std::modf(value, &intpart) == 0.0) {
				wrser.PutVarUint(ctag(TAG_VARINT, tagName));
				wrser.PutVarint(int64_t(value));
			} else {
				wrser.PutVarUint(ctag(TAG_DOUBLE, tagName));
				wrser.PutDouble(value);
			}
		} break;
		case JSON_STRING:
			wrser.PutVarUint(ctag(TAG_STRING, tagName));
			wrser.PutVString(v.toString());
			break;
		case JSON_TRUE:
			wrser.PutVarUint(ctag(TAG_BOOL, tagName));
			wrser.PutBool(true);
			break;
		case JSON_FALSE:
			wrser.PutVarUint(ctag(TAG_BOOL, tagName));
			wrser.PutBool(false);
			break;
		case JSON_NULL:
			wrser.PutVarUint(ctag(TAG_NULL, tagName));
			break;
		case JSON_ARRAY: {
			wrser.PutVarUint(ctag(TAG_ARRAY, tagName));
			unsigned pos = wrser.Len();
			int count = 0;
			// reserve for len
			wrser.PutUInt32(0);
			for (auto elem : v) {
				decodeJson(pl, wrser, elem->value, 0);
				count++;
			}
			*(reinterpret_cast<int *>(wrser.Buf() + pos)) = carraytag(count, TAG_OBJECT);
			break;
		}
		case JSON_OBJECT: {
			wrser.PutVarUint(ctag(TAG_OBJECT, tagName));
			decodeJsonObject(pl, wrser, v);
			wrser.PutVarUint(ctag(TAG_END, tagName));
			break;
		}
	}
}

}  // namespace reindexer
