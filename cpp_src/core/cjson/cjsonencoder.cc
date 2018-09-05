#include "cjsonencoder.h"
#include "cjsondecoder.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

#include "core/payload/payloadtuple.h"

static const int depthLevelInitial = -1;

namespace reindexer {

CJsonEncoder::CJsonEncoder(const TagsMatcher &tagsMatcher, const JsonPrintFilter &filter) : tagsMatcher_(tagsMatcher), filter_(filter) {}

void CJsonEncoder::Encode(ConstPayload *pl, WrSerializer &wrser) {
	KeyRefs kref;
	pl->Get(0, kref);

	p_string tuple(kref[0]);
	key_string pseudo_tuple;

	if (tuple.size() == 0) {
		pseudo_tuple = BuildPayloadTuple(*pl, tagsMatcher_).get();
		tuple = p_string(pseudo_tuple.get());
	}
	Serializer rdser(tuple.data(), tuple.size());

	depthLevel = depthLevelInitial;

	for (int i = 0; i < pl->NumFields(); ++i) fieldsoutcnt_[i] = 0;
	encodeCJson(pl, rdser, wrser);
}

static inline int keyValueType2TagType(int kvType) {
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
static void skipCJsonValue(int tagType, Serializer &rdser) {
	switch (tagType) {
		case TAG_DOUBLE:
			rdser.GetDouble();
			break;
		case TAG_VARINT:
			rdser.GetVarint();
			break;
		case TAG_BOOL:
			rdser.GetBool();
			break;
		case TAG_STRING:
			rdser.GetVString();
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

KeyRefs CJsonEncoder::ExtractFieldValue(const Payload *pl, const string &jsonPath, KeyValueType expectedType) {
	KeyRefs result;
	depthLevel = depthLevelInitial;

	KeyRefs tupleData;
	pl->Get(0, tupleData);
	string_view tuple(tupleData[0]);

	TagsPath fieldTags = tagsMatcher_.path2tag(jsonPath);
	Serializer rdser(tuple.data(), tuple.size());
	getValueFromTuple(rdser, fieldTags, pl, expectedType, result);

	return result;
}

KeyRefs CJsonEncoder::ExtractFieldValue(const Payload *pl, const TagsPath &fieldTags, KeyValueType expectedType) {
	KeyRefs result;
	depthLevel = depthLevelInitial;

	KeyRefs tupleData;
	pl->Get(0, tupleData);
	string_view tuple(tupleData[0]);

	Serializer rdser(tuple.data(), tuple.size());
	getValueFromTuple(rdser, fieldTags, pl, expectedType, result);

	return result;
}

static KeyValueType tagTypeToKvType(int tagType) {
	KeyValueType type = KeyValueEmpty;
	switch (tagType) {
		case TAG_VARINT:
			type = KeyValueInt64;
			break;
		case TAG_DOUBLE:
			type = KeyValueDouble;
			break;
		case TAG_STRING:
			type = KeyValueString;
			break;
		case TAG_BOOL:
			type = KeyValueInt;
			break;
		case TAG_NULL:
			type = KeyValueEmpty;
			break;
		default:
			std::abort();
	}
	return type;
}

static PayloadFieldType fieldTypeFromKvType(KeyValueType type) { return PayloadFieldType(type, std::string(), std::string(), false); }

static KeyValueType identifyConversionType(int supposedType, KeyValueType expectedType) {
	if ((expectedType == KeyValueUndefined) || (expectedType == KeyValueComposite)) {
		return tagTypeToKvType(supposedType);
	}
	KeyValueType supposedTypeKv = tagTypeToKvType(supposedType);
	if ((expectedType == KeyValueString || supposedTypeKv == KeyValueString) && expectedType != supposedTypeKv) {
		throw Error(errLogic, "Conversion is only possible between arithmetic types: expected %d, got %d", expectedType, supposedType);
	}
	return expectedType;
}

bool CJsonEncoder::getValueFromTuple(Serializer &rdser, const TagsPath &fieldTags, const Payload *pl, KeyValueType expectedType,
									 KeyRefs &res, bool arrayElements) {
	if (fieldTags.empty()) return false;

	ctag tag = rdser.GetVarUint();
	int tagType = tag.Type();
	if (tagType == TAG_END) return false;

	int field = tag.Field();
	if (field >= 0) {
		if (tagType == TAG_ARRAY) rdser.GetVarUint();
	} else {
		if (depthLevel >= static_cast<int>(fieldTags.size())) return false;
		arrayElements = arrayElements && (tag.Name() == 0);
		if (tagType == TAG_OBJECT) {
			if ((depthLevel == depthLevelInitial) || (fieldTags[depthLevel] == tag.Name()) || arrayElements) {
				++depthLevel;
				while (getValueFromTuple(rdser, fieldTags, pl, expectedType, res, arrayElements)) {
				}
			} else {
				skipCjsonTag(tag, rdser);
			}
		} else {
			if (arrayElements || (depthLevel != depthLevelInitial && fieldTags[depthLevel] == tag.Name())) {
				if (tagType == TAG_ARRAY) {
					carraytag atag = rdser.GetUInt32();
					for (int count = 0; count < atag.Count(); count++) {
						switch (atag.Tag()) {
							case TAG_OBJECT: {
								int origDepLevel = depthLevel;
								getValueFromTuple(rdser, fieldTags, pl, expectedType, res, true);
								depthLevel = origDepLevel;
								break;
							}
							default: {
								Error err;
								KeyValueType type = identifyConversionType(atag.Tag(), expectedType);
								res.push_back(cjsonValueToKeyRef(atag.Tag(), rdser, fieldTypeFromKvType(type), err));
								return !err.ok();
							}
						}
					}
				} else {
					Error err;
					KeyValueType type = identifyConversionType(tagType, expectedType);
					res.push_back(cjsonValueToKeyRef(tagType, rdser, fieldTypeFromKvType(type), err));
					return !err.ok();
				}
			} else {
				skipCjsonTag(tag, rdser);
			}
		}
	}

	return true;
}

bool CJsonEncoder::encodeCJson(ConstPayload *pl, Serializer &rdser, WrSerializer &wrser, bool match) {
	ctag tag = rdser.GetVarUint();
	int tagType = tag.Type();
	int tagField = tag.Field();

	depthLevel++;

	if (depthLevel == 1) match = match && filter_.Match(tag.Name());

	if (match) {
		wrser.PutVarUint(static_cast<int>(ctag(tag.Type(), tag.Name())));
	}
	if (tagType == TAG_END) {
		depthLevel--;
		return false;
	}

	if (tagField >= 0) {
		int *cnt = &fieldsoutcnt_[tagField];
		KeyRefs kr;
		if (!match) {
			if (tagType == TAG_ARRAY) rdser.GetVarUint();
		} else if (tagType == TAG_ARRAY) {
			int count = rdser.GetVarUint();
			int subtag = keyValueType2TagType(pl->Type().Field(tagField).Type());
			wrser.PutUInt32(static_cast<int>(carraytag(count, subtag)));
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
				while (encodeCJson(pl, rdser, wrser, match)) {
				}
				break;
			case TAG_ARRAY: {
				carraytag atag = rdser.GetUInt32();
				if (match) wrser.PutUInt32(static_cast<int>(atag));
				for (int count = 0; count < atag.Count(); count++) {
					switch (atag.Tag()) {
						case TAG_OBJECT:
							encodeCJson(pl, rdser, wrser, match);
							break;
						default:
							if (match) {
								copyCJsonValue(atag.Tag(), rdser, wrser);
							} else {
								skipCJsonValue(atag.Tag(), rdser);
							}
							break;
					}
				}
				break;
			}
			default:
				if (match) {
					copyCJsonValue(tagType, rdser, wrser);
				} else {
					skipCJsonValue(tagType, rdser);
				}
				break;
		}
	}
	depthLevel--;
	return true;
}

}  // namespace reindexer
