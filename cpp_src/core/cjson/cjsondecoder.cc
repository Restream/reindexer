#include "cjsondecoder.h"
#include "core/keyvalue/p_string.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

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

static void skipCjsonTag(ctag tag, Serializer &rdser) {
	const bool embeddedField = (tag.Field() < 0);
	switch (tag.Type()) {
		case TAG_ARRAY: {
			if (embeddedField) {
				carraytag atag = rdser.GetUInt32();
				for (int i = 0; i < atag.Count(); i++) {
					ctag t = atag.Tag() != TAG_OBJECT ? atag.Tag() : rdser.GetVarUint();
					skipCjsonTag(t, rdser);
				}
			} else {
				rdser.GetVarUint();
			}
		} break;

		case TAG_OBJECT:
			for (ctag otag = rdser.GetVarUint(); otag.Type() != TAG_END; otag = rdser.GetVarUint()) {
				skipCjsonTag(otag, rdser);
			}
			break;
		default:
			if (embeddedField) rdser.GetRawVariant(KeyValueType(tag.Type()));
	}
}

static inline Variant cjsonValueToVariant(int tag, Serializer &rdser, KeyValueType dstType, Error &err) {
	try {
		KeyValueType srcType = KeyValueType(tag);
		if (dstType == KeyValueInt && srcType == KeyValueInt64) srcType = KeyValueInt;
		return rdser.GetRawVariant(KeyValueType(srcType)).convert(dstType);
	} catch (const Error &e) {
		err = e;
	}

	return Variant();
}

CJsonDecoder::CJsonDecoder(TagsMatcher &tagsMatcher) : tagsMatcher_(tagsMatcher), filter_(nullptr), lastErr_(errOK) {}
CJsonDecoder::CJsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter)
	: tagsMatcher_(tagsMatcher), filter_(filter), lastErr_(errOK) {}

Error CJsonDecoder::Decode(Payload *pl, Serializer &rdser, WrSerializer &wrser) {
	try {
		wrser.Reset();
		decodeCJson(pl, rdser, wrser, true);
	}

	catch (const Error &err) {
		return err;
	}
	return lastErr_;
}

bool CJsonDecoder::decodeCJson(Payload *pl, Serializer &rdser, WrSerializer &wrser, bool match) {
	ctag tag = rdser.GetVarUint();
	int tagType = tag.Type();

	if (tagType == TAG_END) {
		wrser.PutVarUint(TAG_END);
		return false;
	}

	int tagName = tag.Name();

	if (tagName) {
		// Check
		(void)tagsMatcher_.tag2name(tagName);
		tagsPath_.push_back(tagName);
	}

	int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());

	if (filter_) {
		if (field >= 0)
			match = filter_->contains(field);
		else
			match = match && filter_->match(tagsPath_);
	}
	if (field >= 0) {
		if (match) {
			Error err = errOK;
			size_t savePos = rdser.Pos();
			KeyValueType fieldType = pl->Type().Field(field).Type();
			if (tagType == TAG_ARRAY) {
				carraytag atag = rdser.GetUInt32();
				int ofs = pl->ResizeArray(field, atag.Count(), true);
				for (int count = 0; count < atag.Count() && err.ok(); count++) {
					ctag tag = atag.Tag() != TAG_OBJECT ? atag.Tag() : rdser.GetVarUint();
					pl->Set(field, ofs + count, cjsonValueToVariant(tag.Type(), rdser, fieldType, err));
				}
				if (err.ok()) {
					wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, field)));
					wrser.PutVarUint(atag.Count());
				}
			} else if (tagType != TAG_NULL) {
				pl->Set(field, {cjsonValueToVariant(tagType, rdser, fieldType, err)}, true);
				if (err.ok()) {
					wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, field)));
				}
			}
			if (!err.ok()) {
				// Type error occuried. Just store field, and do not put it to index
				// rewind serializer, and set lastErr_ code
				field = -1;
				lastErr_ = err;
				rdser.SetPos(savePos);
			}
		} else {
			skipCjsonTag(tag, rdser);
		}
	}

	if (field < 0) {
		wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName, field)));

		if (tagType == TAG_OBJECT) {
			while (decodeCJson(pl, rdser, wrser, match)) {
			}
		} else if (!match) {
			skipCjsonTag(tag, rdser);
		} else if (tagType == TAG_ARRAY) {
			carraytag atag = rdser.GetUInt32();
			wrser.PutUInt32(static_cast<int>(atag));
			for (int count = 0; count < atag.Count(); count++) {
				switch (atag.Tag()) {
					case TAG_OBJECT:
						decodeCJson(pl, rdser, wrser, match);
						break;
					default:
						copyCJsonValue(atag.Tag(), rdser, wrser);
						break;
				}
			}
		} else {
			copyCJsonValue(tagType, rdser, wrser);
		}
	}

	if (tagName) tagsPath_.pop_back();
	return true;
}

}  // namespace reindexer
