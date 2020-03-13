#include "cjsondecoder.h"
#include "cjsontools.h"
#include "core/keyvalue/p_string.h"
#include "tagsmatcher.h"
#include "tools/serializer.h"

namespace reindexer {

CJsonDecoder::CJsonDecoder(TagsMatcher &tagsMatcher) : tagsMatcher_(tagsMatcher), filter_(nullptr), lastErr_(errOK) {}
CJsonDecoder::CJsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter)
	: tagsMatcher_(tagsMatcher), filter_(filter), lastErr_(errOK) {}

Error CJsonDecoder::Decode(Payload *pl, Serializer &rdser, WrSerializer &wrser) {
	try {
		decodeCJson(pl, rdser, wrser, true);
	} catch (const Error &err) {
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
		tagsPath_.emplace_back(tagName);
	}

	int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());
	if (tag.Field() >= 0) {
		throw Error(errLogic, "Reference tag was found in transport CJSON for field %d[%s] in ns [%s]", tag.Field(),
					tagsMatcher_.tag2name(tagName), pl->Type().Name());
	}

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
					// TODO: remove hardcoded conversion from KeyType to TAG

					wrser.PutVarUint(
						static_cast<int>(ctag(fieldType == KeyValueInt ? KeyValueType(TAG_VARINT) : fieldType, tagName, field)));
				}
			} else {
				wrser.PutVarUint(static_cast<int>(ctag(tagType, tagName)));
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
			while (decodeCJson(pl, rdser, wrser, match))
				;
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
