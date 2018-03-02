#include "cjsonencoder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

CJsonEncoder::CJsonEncoder(const TagsMatcher &tagsMatcher) : tagsMatcher_(tagsMatcher) {}

void CJsonEncoder::Encode(ConstPayload *pl, WrSerializer &wrser) {
	KeyRefs kref;
	pl->Get(0, kref);

	p_string tuple(kref[0]);
	Serializer rdser(tuple.data(), tuple.size());

	for (int i = 0; i < pl->NumFields(); ++i) fieldsoutcnt_[i] = 0;
	encodeCJson(pl, rdser, wrser);
}

static inline int kvType2TagType(int kvType) {
	switch (kvType) {
		case KeyValueInt:
		case KeyValueInt64:
			return TAG_VARINT;
		case KeyValueString:
			return TAG_STRING;
		case KeyValueDouble:
			return TAG_DOUBLE;
		default:
			abort();
	}
}

void copyCJsonValue(int tagType, Serializer &rdser, WrSerializer &wrser) {
	switch (tagType) {
		case TAG_DOUBLE:
			wrser.PutDouble(rdser.GetDouble());
			break;
		case TAG_VARINT:
			wrser.PutVarint(rdser.GetVarint());
			break;
		case TAG_BOOL:
			wrser.PutBool(rdser.GetBool());
			break;
		case TAG_STRING:
			wrser.PutVString(rdser.GetVString());
			break;
		case TAG_NULL:
			break;
		default:
			throw Error(errParseJson, "Unexpected cjson typeTag '%s' while parsing value", ctag(tagType).TypeName());
	}
}

static void encodeKeyRef(WrSerializer &wrser, KeyRef kr, int tagType) {
	switch (kr.Type()) {
		case KeyValueInt:
			switch (tagType) {
				case TAG_VARINT:
					wrser.PutVarint(int(kr));
					return;
				case TAG_BOOL:
					wrser.PutBool(int(kr));
					return;
				case TAG_DOUBLE:
					wrser.PutDouble(int(kr));
					return;
			}
			break;
		case KeyValueInt64:
			switch (tagType) {
				case TAG_VARINT:
					wrser.PutVarint(int64_t(kr));
					return;
				case TAG_BOOL:
					wrser.PutBool(int64_t(kr));
					return;
				case TAG_DOUBLE:
					wrser.PutDouble(int64_t(kr));
					return;
			}
			break;
		case KeyValueDouble:
			switch (tagType) {
				case TAG_VARINT:
					wrser.PutVarint(double(kr));
					return;
				case TAG_BOOL:
					wrser.PutBool(double(kr));
					return;
				case TAG_DOUBLE:
					wrser.PutDouble(double(kr));
					return;
			}
			break;
		case KeyValueString:
			switch (tagType) {
				case TAG_STRING:
					wrser.PutVString(p_string(kr));
					return;
			}
			break;
		default:
			break;
	}
	throw Error(errParseJson, "Can't convert cjson typeTag '%s' to '%s'", ctag(tagType).TypeName(), KeyRef::TypeName(kr.Type()));
}

bool CJsonEncoder::encodeCJson(ConstPayload *pl, Serializer &rdser, WrSerializer &wrser) {
	ctag tag = rdser.GetVarUint();
	int tagType = tag.Type();
	int tagField = tag.Field();

	wrser.PutVarUint(ctag(tag.Type(), tag.Name()));

	if (tagType == TAG_END) return false;

	if (tagField >= 0) {
		int *cnt = &fieldsoutcnt_[tagField];
		KeyRefs kr;
		if (tagType == TAG_ARRAY) {
			int count = rdser.GetVarUint();
			int subtag = kvType2TagType(pl->Type().Field(tagField).Type());
			wrser.PutUInt32(carraytag(count, subtag));
			pl->Get(tagField, kr);
			while (count--) {
				assertf(*cnt < int(kr.size()), "No data in field '%s.%s', got %d items.", pl->Type().Name().c_str(),
						pl->Type().Field(tagField).Name().c_str(), *cnt);

				encodeKeyRef(wrser, kr[(*cnt)++], subtag);
			}
		} else if (tagType != TAG_NULL) {
			pl->Get(tagField, kr);
			assertf(*cnt < int(kr.size()), "No data in field '%s.%s', got %d items.", pl->Type().Name().c_str(),
					pl->Type().Field(tagField).Name().c_str(), *cnt);

			encodeKeyRef(wrser, kr[(*cnt)++], tagType);
		}
	} else {
		switch (tagType) {
			case TAG_OBJECT:
				while (encodeCJson(pl, rdser, wrser)) {
				}
				break;
			case TAG_ARRAY: {
				carraytag atag = rdser.GetUInt32();
				wrser.PutUInt32(atag);
				for (int count = 0; count < atag.Count(); count++) {
					switch (atag.Tag()) {
						case TAG_OBJECT:
							encodeCJson(pl, rdser, wrser);
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
	return true;
}

}  // namespace reindexer
