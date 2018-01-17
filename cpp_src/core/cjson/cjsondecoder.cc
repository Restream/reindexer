#include "cjsondecoder.h"
#include "cjsonencoder.h"

namespace reindexer {

CJsonDecoder::CJsonDecoder(TagsMatcher &tagsMatcher) : tagsMatcher_(tagsMatcher) {}

Error CJsonDecoder::Decode(Payload *pl, Serializer &rdser, WrSerializer &wrser) {
	try {
		wrser.Reset();
		tagsPath_.clear();
		decodeCJson(pl, rdser, wrser);
	}

	catch (const Error &err) {
		return err;
	}
	return 0;
}

KeyRef cjsonValueToKeyRef(int tag, Serializer &rdser, const PayloadFieldType &pt) {
	auto t = pt.Type();
	static string nullstr;
	switch (tag) {
		case TAG_VARINT: {
			auto v = rdser.GetVarint();
			switch (t) {
				case KeyValueDouble:
					return KeyRef(double(v));
				case KeyValueInt:
					return KeyRef(int(v));
				case KeyValueInt64:
					return KeyRef(int64_t(v));
				default:
					break;
			}
			break;
		}
		case TAG_DOUBLE: {
			auto v = rdser.GetDouble();
			switch (t) {
				case KeyValueDouble:
					return KeyRef(double(v));
				case KeyValueInt:
					return KeyRef(int(v));
				case KeyValueInt64:
					return KeyRef(int64_t(v));
				default:
					break;
			}
			break;
		}
		case TAG_STRING:
			switch (t) {
				case KeyValueString:
					return KeyRef(rdser.GetPVString());
				default:
					break;
			}
			break;
		case TAG_BOOL:
			switch (t) {
				case KeyValueInt:
					return KeyRef(rdser.GetVarint() ? 1 : 0);
				default:
					break;
			}
			break;
		case TAG_NULL:
			switch (t) {
				case KeyValueDouble:
					return KeyRef(double(0));
				case KeyValueInt:
					return KeyRef(int(0));
				case KeyValueInt64:
					return KeyRef(int64_t(0));
				case KeyValueString:
					return KeyRef(p_string(static_cast<const char *>(nullptr)));
				default:
					break;
			}
	}

	throw Error(errLogic, "Error parsing cjson field '%s': got '%s', expected %s", pt.Name().c_str(), ctag(tag).TypeName(),
				KeyValue::TypeName(t));
}

bool CJsonDecoder::decodeCJson(Payload *pl, Serializer &rdser, WrSerializer &wrser) {
	ctag tag = rdser.GetVarUint();
	int tagType = tag.Type();

	if (tagType == TAG_END) {
		wrser.PutVarUint(TAG_END);
		return false;
	}

	int tagName = tag.Name();

	if (tagName) tagsPath_.push_back(tagName);

	int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());

	wrser.PutVarUint(ctag(tagType, tagName, field));

	if (field >= 0) {
		KeyRefs kvs;
		if (tagType == TAG_ARRAY) {
			carraytag atag = rdser.GetInt();
			kvs.reserve(atag.Count());
			for (int count = 0; count < atag.Count(); count++) {
				ctag tag = atag.Tag() != TAG_OBJECT ? atag.Tag() : rdser.GetVarUint();
				kvs.push_back(cjsonValueToKeyRef(tag.Type(), rdser, pl->Type().Field(field)));
			}
			wrser.PutVarUint(atag.Count());

		} else if (tagType != TAG_NULL) {
			kvs.push_back(cjsonValueToKeyRef(tagType, rdser, pl->Type().Field(field)));
		}
		if (kvs.size()) pl->Set(field, kvs, true);
	} else {
		switch (tagType) {
			case TAG_OBJECT:
				while (decodeCJson(pl, rdser, wrser)) {
				}
				break;
			case TAG_ARRAY: {
				carraytag atag = rdser.GetInt();
				wrser.PutInt(atag);
				for (int count = 0; count < atag.Count(); count++) {
					switch (atag.Tag()) {
						case TAG_OBJECT:
							decodeCJson(pl, rdser, wrser);
							break;
						default:
							copyCJsonValue(atag.Tag(), rdser, wrser);
							break;
					}
				}
				break;
			}
			default:
				copyCJsonValue(tagType, rdser, wrser);
				break;
		}
	}
	if (tagName) tagsPath_.pop_back();
	return true;
}

}  // namespace reindexer
