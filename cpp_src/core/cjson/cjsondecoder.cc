#include "cjsondecoder.h"
#include "cjsontools.h"
#include "core/keyvalue/p_string.h"
#include "tagsmatcher.h"
#include "tools/flagguard.h"
#include "tools/serializer.h"

namespace reindexer {

template <typename FilterT, typename RecoderT, typename TagOptT>
bool CJsonDecoder::decodeCJson(Payload &pl, Serializer &rdser, WrSerializer &wrser, FilterT filter, RecoderT recoder, TagOptT) {
	const ctag tag = rdser.GetCTag();
	TagType tagType = tag.Type();
	if (tag == kCTagEnd) {
		wrser.PutCTag(kCTagEnd);
		return false;
	}
	int tagName = 0;
	if constexpr (std::is_same_v<TagOptT, NamedTagOpt>) {
		tagName = tag.Name();
		assertrx_dbg(tagName);
		// Check if tag exists
		(void)tagsMatcher_.tag2name(tagName);
		tagsPath_.emplace_back(tagName);
	}

	if rx_unlikely (tag.Field() >= 0) {
		throwTagReferenceError(tag, pl);
	}

	const int field = tagsMatcher_.tags2field(tagsPath_.data(), tagsPath_.size());
	if (field >= 0) {
		const bool match = filter.contains(field);
		if (match) {
			tagType = recoder.RegisterTagType(tagType, field);
			if (tagType == TAG_NULL) {
				wrser.PutCTag(ctag{TAG_NULL, tagName});
			} else if (recoder.Recode(rdser, pl, tagName, wrser)) {
				// No more actions needed after recoding
			} else {
				const auto &fieldRef{pl.Type().Field(field)};
				const KeyValueType fieldType{fieldRef.Type()};
				if (tagType == TAG_ARRAY) {
					if rx_unlikely (!fieldRef.IsArray()) {
						throwUnexpectedArrayError(fieldRef);
					}
					const carraytag atag = rdser.GetCArrayTag();
					const auto count = atag.Count();
					const int ofs = pl.ResizeArray(field, count, true);
					const TagType atagType = atag.Type();
					if (atagType != TAG_OBJECT) {
						for (size_t i = 0; i < count; ++i) {
							pl.Set(field, ofs + i, cjsonValueToVariant(atagType, rdser, fieldType));
						}
					} else {
						for (size_t i = 0; i < count; ++i) {
							pl.Set(field, ofs + i, cjsonValueToVariant(rdser.GetCTag().Type(), rdser, fieldType));
						}
					}

					wrser.PutCTag(ctag{TAG_ARRAY, tagName, field});
					wrser.PutVarUint(count);
				} else {
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, fieldRef, field, isInArray(), "cjson");
					objectScalarIndexes_.set(field);
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
			// objectScalarIndexes_.set(field); - do not change objectScalarIndexes_ value for the filtered out fields
			skipCjsonTag(tag, rdser);
		}
	} else {
		const bool match = filter.match(tagsPath_);
		if (match) {
			tagType = recoder.RegisterTagType(tagType, tagsPath_);
			wrser.PutCTag(ctag{tagType, tagName, field});
			if (tagType == TAG_OBJECT) {
				while (decodeCJson(pl, rdser, wrser, filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamedTagOpt{}))
					;
			} else if (recoder.Recode(rdser, wrser)) {
				// No more actions needed after recoding
			} else if (tagType == TAG_ARRAY) {
				const carraytag atag = rdser.GetCArrayTag();
				wrser.PutCArrayTag(atag);
				const auto count = atag.Count();
				const TagType atagType = atag.Type();
				CounterGuardIR32 g(arrayLevel_);
				if (atagType == TAG_OBJECT) {
					for (size_t i = 0; i < count; ++i) {
						decodeCJson(pl, rdser, wrser, filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamelessTagOpt{});
					}
				} else {
					for (size_t i = 0; i < count; ++i) {
						copyCJsonValue(atagType, rdser, wrser);
					}
				}
			} else {
				copyCJsonValue(tagType, rdser, wrser);
			}
		} else if (tagType != TAG_OBJECT) {
			// !match
			skipCjsonTag(tag, rdser);
		} else {
			// !match
			wrser.PutCTag(ctag{tagType, tagName, field});
			while (decodeCJson(pl, rdser, wrser, filter.MakeSkipFilter(), recoder.MakeCleanCopy(), NamedTagOpt{}))
				;
		}
	}

	if constexpr (std::is_same_v<TagOptT, NamedTagOpt>) {
		tagsPath_.pop_back();
	}

	return true;
}

[[nodiscard]] Variant CJsonDecoder::cjsonValueToVariant(TagType tagType, Serializer &rdser, KeyValueType fieldType) {
	if (fieldType.Is<KeyValueType::String>() && tagType != TagType::TAG_STRING) {
		storage_.emplace_back(rdser.GetRawVariant(KeyValueType{tagType}).As<std::string>());
		return Variant(p_string(&storage_.back()), Variant::no_hold_t{});
	} else {
		return reindexer::cjsonValueToVariant(tagType, rdser, fieldType);
	}
}

RX_NO_INLINE void CJsonDecoder::throwTagReferenceError(ctag tag, const Payload &pl) {
	throw Error(errLogic, "Reference tag was found in transport CJSON for field %d[%s] in ns [%s]", tag.Field(),
				tagsMatcher_.tag2name(tag.Name()), pl.Type().Name());
}

RX_NO_INLINE void CJsonDecoder::throwUnexpectedArrayError(const PayloadFieldType &fieldRef) {
	throw Error(errLogic, "Error parsing cjson field '%s' - got array, expected scalar %s", fieldRef.Name(), fieldRef.Type().Name());
}

template bool CJsonDecoder::decodeCJson<CJsonDecoder::DummyFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::DummyFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::DummyFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::DummyFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::RestrictingFilter, CJsonDecoder::DummyRecoder, CJsonDecoder::NamelessTagOpt);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload &, Serializer &, WrSerializer &, CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt);

}  // namespace reindexer
