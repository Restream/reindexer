#include "core/item.h"
#define __STDC_FORMAT_MACROS 1
#include <inttypes.h>
#include <cmath>
#include "cbinding/serializer.h"

namespace reindexer {

Error ItemImpl::SetField(const string &index, const KeyRefs &krs) {
	try {
		Set(index, krs, true);
	} catch (const Error &err) {
		return err;
	}
	return 0;
}

Slice ItemImpl::Serialize() {
	ser_.Reset();
	for (int field = 0; field < NumFields(); ++field) {
		KeyRefs kvs;
		Get(field, kvs);
		if (t_.Field(field).IsArray()) ser_.PutVarUint(kvs.size());

		for (auto v : kvs) {
			switch (t_.Field(field).Type()) {
				case KeyValueInt:
					ser_.PutVarint((int)v);
					break;
				case KeyValueInt64:
					ser_.PutVarint((int64_t)v);
					break;
				case KeyValueDouble:
					ser_.PutDouble((double)v);
					break;
				case KeyValueString:
					ser_.PutSlice((p_string)v);
					break;
				default:
					assert(0);
			}
		}
	}
	return Slice((const char *)ser_.Buf(), ser_.Len());
}

void ItemImpl::Deserialize(const Slice &b) {
	Serializer ser(b.data(), b.size());
	Reset();
	for (int field = 0; field < NumFields(); ++field) {
		int count = t_.Field(field).IsArray() ? ser.GetVarUint() : 1;
		KeyRefs kvs;
		kvs.reserve(count);
		for (int i = 0; i < count; ++i) {
			switch (t_.Field(field).Type()) {
				case KeyValueInt:
					kvs.push_back(KeyRef((int)ser.GetVarint()));
					break;
				case KeyValueInt64:
					kvs.push_back(KeyRef(ser.GetVarint()));
					break;
				case KeyValueDouble:
					kvs.push_back(KeyRef(ser.GetDouble()));
					break;
				case KeyValueString:
					kvs.push_back(KeyRef(ser.GetPString()));
					break;
				default:
					assert(0);
			}
		}
		Set(field, kvs);
	}
}

static KeyRef jsonValue2KeyRef(JsonValue &v, KeyValueType t) {
	static string nullstr;
	switch (v.getTag()) {
		case JSON_NUMBER:
			switch (t) {
				case KeyValueDouble:
					return KeyRef(v.toNumber());
				case KeyValueInt:
					return KeyRef((int)v.toNumber());
				case KeyValueInt64:
					return KeyRef((int64_t)v.toNumber());
				default:
					throw Error(errLogic, "Error parsing json field - got number, expected %s", KeyValue::TypeName(t));
			}
		case JSON_STRING:
			return KeyRef(p_string(v.toString()));
		case JSON_FALSE:
			return KeyRef((int)0);
		case JSON_TRUE:
			return KeyRef((int)1);
		case JSON_NULL:
			switch (t) {
				case KeyValueDouble:
					return KeyRef((double)0);
				case KeyValueInt:
					return KeyRef((int)0);
				case KeyValueInt64:
					return KeyRef((int64_t)0);
				case KeyValueString:
					return KeyRef(p_string((const char *)nullptr));
				default:
					throw Error(errLogic, "Error parsing json field - got null, expected %s", KeyValue::TypeName(t));
			}
		default:
			throw Error(errLogic, "Error parsing json - invalid tag %d", v.getTag());
	}
	return KeyRef();
}

void ItemImpl::splitJsonObject(JsonValue &v) {
	int l = jsonPath_.length();
	for (auto elem : v) {
		if (l) jsonPath_.append(".");
		jsonPath_.append(elem->key, strlen(elem->key));

		int subtag = tagsMatcher_->name2tag(elem->key, jsonPath_);
		int field = tagsMatcher_->tag2field(subtag);

		if (field < 0)
			splitJson(elem->value, subtag);
		else {
			// Indexed field. extract it
			KeyRefs kvs;
			int count = 0;
			if (elem->value.getTag() == JSON_ARRAY)
				for (auto subelem : elem->value) {
					kvs.push_back(jsonValue2KeyRef(subelem->value, t_.Field(field).Type()));
					count++;
				}
			else if (elem->value.getTag() != JSON_NULL)
				kvs.push_back(jsonValue2KeyRef(elem->value, t_.Field(field).Type()));
			if (kvs.size()) Set(field, kvs, true);

			// Put special tag :link data to indexed field
			switch (elem->value.getTag()) {
				case JSON_NUMBER:
					ser_.PutVarUint(TAG_VARINT | (subtag << 3));
					break;
				case JSON_STRING:
					ser_.PutVarUint(TAG_STRING | (subtag << 3));
					break;
				case JSON_TRUE:
				case JSON_FALSE:
					ser_.PutVarUint(TAG_BOOL | (subtag << 3));
					break;
				case JSON_ARRAY:
					ser_.PutVarUint(TAG_ARRAY | (subtag << 3));
					ser_.PutVarUint(count);
					break;
				case JSON_NULL:
					ser_.PutVarUint(TAG_NULL | ((subtag & 0xFFF) << 3));
					break;
				default:
					ser_.PutVarUint(TAG_NULL | (subtag << 3));
					break;
			}
		}
		jsonPath_.resize(l);
	}
}

// tag is uint in following format: ARRRRRRRNNNNNNNNNNNNNNTTT
// TTT - 3bit  type: one of TAG_XXXX
// NNN - 12bit index+1 of field name in tagsMatcher (0 if no name)
// RRR - 6bit index+1 of field in reindexer Payload (0 if no field)
// A   - 1bit

// Split original JSON into 2 parts:
// 1. PayloadFields - fields from json found by 'jsonPath' tags
// 2. stripped binary packed JSON without fields values found by 'jsonPath' tags
void ItemImpl::splitJson(JsonValue &v, int tag) {
	switch (v.getTag()) {
		case JSON_NUMBER: {
			double value = v.toNumber(), intpart;
			if (std::modf(value, &intpart) == 0.0) {
				ser_.PutVarUint(TAG_VARINT | (tag << 3));
				ser_.PutVarint(int64_t(value));
			} else {
				ser_.PutVarUint(TAG_DOUBLE | (tag << 3));
				ser_.PutDouble(value);
			}
		} break;
		case JSON_STRING:
			ser_.PutVarUint(TAG_STRING | (tag << 3));
			ser_.PutVString(v.toString());
			break;
		case JSON_TRUE:
			ser_.PutVarUint(TAG_BOOL | (tag << 3));
			ser_.PutBool(true);
			break;
		case JSON_FALSE:
			ser_.PutVarUint(TAG_BOOL | (tag << 3));
			ser_.PutBool(false);
			break;
		case JSON_NULL:
			ser_.PutVarUint(TAG_NULL | (tag << 3));
			break;
		case JSON_ARRAY:
			ser_.PutVarUint(TAG_ARRAY | (tag << 3));
			for (auto elem : v) splitJson(elem->value, 0);
			ser_.PutVarUint(TAG_END | (tag << 3));
			break;
		case JSON_OBJECT: {
			ser_.PutVarUint(TAG_OBJECT | (tag << 3));
			splitJsonObject(v);
			ser_.PutVarUint(TAG_END | (tag << 3));
			break;
		}
	}
}

bool ItemImpl::MergeJson(ConstPayload *pl, const ItemTagsMatcher *tagsMatcher, WrSerializer &wrser, Serializer &rdser, bool first,
						 int *fieldsoutcnt) {
	int tag = rdser.GetVarUint();

	if ((tag & 0x7) == TAG_END) return false;

	if (!first) wrser.PutChar(',');
	first = true;

	const char *name = tagsMatcher->tag2name(tag >> 3);

	if (name) {
		wrser.PrintJsonString(Slice(name, strlen(name)));
		wrser.PutChar(':');
	}

	int field = tagsMatcher->tag2field(tag >> 3);

	// get field from indexed field
	if (field >= 0) {
		KeyRefs kr;
		int *cnt = &fieldsoutcnt[field];

		switch (tag & 0x7) {
			case TAG_ARRAY: {
				wrser.PutChar('[');
				int count = rdser.GetVarUint();
				while (count--) {
					pl->Get(field, kr);
					assertf(*cnt < (int)kr.size(), "No data in field '%s.%s', got %d items.", pl->Type().Name().c_str(), name, *cnt);
					wrser.PrintKeyRefToJson(kr[(*cnt)++], tag & 0x7);
					if (count) wrser.PutChar(',');
				}
				wrser.PutChar(']');
				break;
			}
			default:
				pl->Get(field, kr);
				assertf(*cnt < (int)kr.size(), "No data in field '%s.%s', got %d items.", pl->Type().Name().c_str(), name, *cnt);
				wrser.PrintKeyRefToJson(kr[(*cnt)++], tag & 0x7);
				break;
		}
		return true;
	}

	switch (tag & 0x7) {
		case TAG_ARRAY:
			wrser.PutChar('[');
			while (MergeJson(pl, tagsMatcher, wrser, rdser, first, fieldsoutcnt)) first = false;
			wrser.PutChar(']');
			break;
		case TAG_DOUBLE:
			wrser.Printf("%g", rdser.GetDouble());
			break;
		case TAG_VARINT:
			wrser.Print(rdser.GetVarint());
			break;
		case TAG_BOOL:
			wrser.PutChars(rdser.GetBool() ? "true" : "false");
			break;
		case TAG_NULL:
			wrser.PutChars("null");
			break;
		case TAG_STRING:
			wrser.PrintJsonString(rdser.GetVString());
			break;
		case TAG_OBJECT:
			wrser.PutChar('{');
			while (MergeJson(pl, tagsMatcher, wrser, rdser, first, fieldsoutcnt)) first = false;
			wrser.PutChar('}');
			break;
		case TAG_END:
			return false;
	}
	return true;
}

Error ItemImpl::FromJSON(const Slice &slice) {
	const char *json = slice.data();
	try {
		char *endptr;
		JsonValue value;
		int status = jsonParse((char *)json, &endptr, &value, jsonAllocator_);
		if (status != JSON_OK) throw Error(errLogic, "Error parsing json - status %d\n", status);

		// Split parsed json into indexes and tuple
		jsonPath_.clear();
		ser_.Reset();
		splitJson(value, 0);

		// Put tuple to field[0]
		tupleData_ = make_shared<string>((const char *)ser_.Buf(), ser_.Len());
		KeyRefs kr;
		kr.push_back(KeyRef(tupleData_));
		Set(0, kr);

	} catch (const Error &err) {
		return err;
	}
	return 0;
}

Slice ItemImpl::GetJSON() {
	KeyRefs kref;
	Get(0, kref);

	p_string tuple = (p_string)kref[0];
	Serializer rdser(tuple.data(), tuple.size());
	ser_.Reset();
	int fieldsoutcnt[maxIndexes];
	for (int i = 0; i < NumFields(); ++i) fieldsoutcnt[i] = 0;
	ConstPayload pl(Type(), Data());

	MergeJson(&pl, tagsMatcher_, ser_, rdser, true, fieldsoutcnt);

	return Slice((const char *)ser_.Buf(), ser_.Len());
}

PayloadType ItemImpl::invalidType_("<invalid type>");

}  // namespace reindexer
