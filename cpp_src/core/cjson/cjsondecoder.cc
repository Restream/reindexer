#include "cjsondecoder.h"
#include "cjsontools.h"
#include "core/keyvalue/p_string.h"
#include "tagsmatcher.h"
#include "tools/flagguard.h"
#include "tools/serializer.h"

namespace reindexer {

CJsonDecoder::CJsonDecoder(TagsMatcher &tagsMatcher) : tagsMatcher_(tagsMatcher), filter_(nullptr) {}
CJsonDecoder::CJsonDecoder(TagsMatcher &tagsMatcher, const FieldsSet *filter, Recoder *recoder)
	: tagsMatcher_(tagsMatcher), filter_(filter), recoder_{recoder} {}

bool CJsonDecoder::decodeCJson(Payload &pl, Serializer &rdser, WrSerializer &wrser, bool match) {
	const ctag tag = rdser.GetCTag();
	TagType tagType = tag.Type();
	if (tagType == TAG_END) {
		wrser.PutCTag(kCTagEnd);
		return false;
	}
	const int tagName = tag.Name();
	if (tagName) {
		// Check
		(void)tagsMatcher_.tag2name(tagName);
		tagsPath_.emplace_back(tagName);
	}
	if (tag.Field() >= 0) {
		throw Error(errLogic, "Reference tag was found in transport CJSON for field %d[%s] in ns [%s]", tag.Field(),
					tagsMatcher_.tag2name(tagName), pl.Type().Name());
	}

	const int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());

	if (filter_) {
		if (field >= 0) {
			match = filter_->contains(field);
		} else {
			match = match && filter_->match(tagsPath_);
		}
	}
	Recoder *recoder{nullptr};
	if (recoder_) {
		if (field >= 0) {
			if (recoder_->Match(field)) {
				recoder = recoder_;
			}
		} else {
			if (recoder_->Match(tagsPath_)) {
				recoder = recoder_;
			}
		}
	}
	if (recoder) {
		tagType = recoder->Type(tagType);
	}
	if (field >= 0) {
		if (match) {
			if (tagType == TAG_NULL) {
				wrser.PutCTag(ctag{TAG_NULL, tagName});
			} else if (recoder) {
				recoder->Recode(rdser, pl, tagName, wrser);
			} else {
				const auto &fieldRef{pl.Type().Field(field)};
				const KeyValueType fieldType{fieldRef.Type()};
				if (tagType == TAG_ARRAY) {
					if (!fieldRef.IsArray()) {
						throw Error(errLogic, "Error parsing cjson field '%s' - got array, expected scalar %s", fieldRef.Name(),
									fieldType.Name());
					}
					const carraytag atag = rdser.GetCArrayTag();
					const auto count = atag.Count();
					const int ofs = pl.ResizeArray(field, count, true);
					const TagType atagType = atag.Type();
					for (size_t i = 0; i < count; ++i) {
						const TagType type = atagType != TAG_OBJECT ? atagType : rdser.GetCTag().Type();
						pl.Set(field, ofs + i, cjsonValueToVariant(type, rdser, fieldType));
					}
					wrser.PutCTag(ctag{TAG_ARRAY, tagName, field});
					wrser.PutVarUint(count);
				} else if (isInArray() && !fieldRef.IsArray()) {
					throw Error(errLogic, "Error parsing cjson field '%s' - got value in the nested array, but expected scalar %s",
								fieldRef.Name(), fieldType.Name());
				} else {
					pl.Set(field, cjsonValueToVariant(tagType, rdser, fieldType), true);
					fieldType.EvaluateOneOf(
						[&](OneOf<KeyValueType::Int, KeyValueType::Int64>) {
							wrser.PutCTag(ctag{TAG_VARINT, tagName, field});
						},
						[&](OneOf<KeyValueType::Double, KeyValueType::String, KeyValueType::Bool, KeyValueType::Null, KeyValueType::Uuid>) {
							wrser.PutCTag(ctag{fieldType.ToTagType(), tagName, field});
						},
						[&](OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite>) { assertrx(0); });
				}
			}
		} else {
			skipCjsonTag(tag, rdser);
		}
	} else {
		wrser.PutCTag(ctag{tagType, tagName, field});
		if (tagType == TAG_OBJECT) {
			while (decodeCJson(pl, rdser, wrser, match))
				;
		} else if (!match) {
			skipCjsonTag(tag, rdser);
		} else if (recoder) {
			recoder->Recode(rdser, wrser);
		} else if (tagType == TAG_ARRAY) {
			const carraytag atag = rdser.GetCArrayTag();
			wrser.PutCArrayTag(atag);
			const auto count = atag.Count();
			const TagType atagType = atag.Type();
			CounterGuardIR32 g(arrayLevel_);
			for (size_t i = 0; i < count; ++i) {
				switch (atagType) {
					case TAG_OBJECT:
						decodeCJson(pl, rdser, wrser, match);
						break;
					case TAG_VARINT:
					case TAG_NULL:
					case TAG_BOOL:
					case TAG_STRING:
					case TAG_END:
					case TAG_DOUBLE:
					case TAG_ARRAY:
					case TAG_UUID:
						copyCJsonValue(atagType, rdser, wrser);
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
