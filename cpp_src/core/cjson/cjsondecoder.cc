#include "cjsondecoder.h"
#include "cjsontools.h"
#include "tagsmatcher.h"
#include "tools/flagguard.h"
#include "tools/serializer.h"

namespace reindexer {

template <typename FilterT, typename RecoderT, typename TagOptT>
bool CJsonDecoder::decodeCJson(Payload& pl, Serializer& rdser, WrSerializer& wrser, FilterT filter, RecoderT recoder, TagOptT,
							   FloatVectorsHolderVector& floatVectorsHolder) {
	using namespace std::string_view_literals;
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
				const auto& fieldRef{pl.Type().Field(field)};
				const KeyValueType fieldType{fieldRef.Type()};
				if (tagType == TAG_ARRAY) {
					const carraytag atag = rdser.GetCArrayTag();
					const auto count = atag.Count();
					const TagType atagType = atag.Type();
					if (fieldRef.IsFloatVector()) {
						ConstFloatVectorView vectView;
						if (count != 0) {
							if (atagType != TAG_DOUBLE && atagType != TAG_FLOAT && atagType != TAG_VARINT) {
								throwUnexpectedArrayTypeForFloatVectorError("cjson"sv, fieldRef);
							}
							if (count != fieldRef.FloatVectorDimension().Value()) {
								throwUnexpectedArraySizeForFloatVectorError("cjson"sv, fieldRef, count);
							}
							auto vect = FloatVector::CreateNotInitialized(fieldRef.FloatVectorDimension());
							if (atagType == TAG_DOUBLE) {
								for (size_t i = 0; i < count; ++i) {
									vect.RawData()[i] = rdser.GetDouble();
								}
							} else if (atagType == TAG_FLOAT) {
								for (size_t i = 0; i < count; ++i) {
									vect.RawData()[i] = rdser.GetFloat();
								}
							} else if (atagType == TAG_VARINT) {
								for (size_t i = 0; i < count; ++i) {
									vect.RawData()[i] = rdser.GetVarint();
								}
							}
							floatVectorsHolder.Add(std::move(vect));
							vectView = floatVectorsHolder.Back();
						}
						objectScalarIndexes_.set(field);  // Indexed float vector is treated as scalar value
						pl.Set(field, Variant{vectView});
						wrser.PutCTag(ctag{TAG_ARRAY, tagName, field});
						wrser.PutVarUint(count);
					} else if rx_likely (fieldRef.IsArray()) {
						validateArrayFieldRestrictions(fieldRef, count, "cjson");
						const int ofs = pl.ResizeArray(field, count, true);
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
						throwUnexpectedArrayError("cjson"sv, fieldRef);
					}
				} else {
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, fieldRef, field, isInArray(), "cjson");
					validateArrayFieldRestrictions(fieldRef, 1, "cjson");
					objectScalarIndexes_.set(field);
					pl.Set(field, cjsonValueToVariant(tagType, rdser, fieldType), true);
					fieldType.EvaluateOneOf(
						[&](OneOf<KeyValueType::Int, KeyValueType::Int64>) { wrser.PutCTag(ctag{TAG_VARINT, tagName, field}); },
						[&](OneOf<KeyValueType::Double, KeyValueType::Float, KeyValueType::String, KeyValueType::Bool, KeyValueType::Null,
								  KeyValueType::Uuid>) { wrser.PutCTag(ctag{fieldType.ToTagType(), tagName, field}); },
						[&](OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::FloatVector>) {
							assertrx(false);
						});
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
				while (decodeCJson(pl, rdser, wrser, filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamedTagOpt{}, floatVectorsHolder));
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
						decodeCJson(pl, rdser, wrser, filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamelessTagOpt{},
									floatVectorsHolder);
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
			while (decodeCJson(pl, rdser, wrser, filter.MakeSkipFilter(), recoder.MakeCleanCopy(), NamedTagOpt{}, floatVectorsHolder));
		}
	}

	if constexpr (std::is_same_v<TagOptT, NamedTagOpt>) {
		tagsPath_.pop_back();
	}

	return true;
}

[[nodiscard]] Variant CJsonDecoder::cjsonValueToVariant(TagType tagType, Serializer& rdser, KeyValueType fieldType) {
	if (fieldType.Is<KeyValueType::String>() && tagType != TagType::TAG_STRING) {
		auto& back = storage_.emplace_back(rdser.GetRawVariant(KeyValueType{tagType}).As<key_string>());
		return Variant(p_string(back), Variant::noHold);
	} else {
		return reindexer::cjsonValueToVariant(tagType, rdser, fieldType);
	}
}

RX_NO_INLINE void CJsonDecoder::throwTagReferenceError(ctag tag, const Payload& pl) {
	throw Error(errLogic, "Reference tag was found in transport CJSON for field {}[{}] in ns [{}]", tag.Field(),
				tagsMatcher_.tag2name(tag.Name()), pl.Type().Name());
}

template bool CJsonDecoder::decodeCJson<CJsonDecoder::DefaultFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload&, Serializer&, WrSerializer&, CJsonDecoder::DefaultFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt,
	FloatVectorsHolderVector&);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::DefaultFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload&, Serializer&, WrSerializer&, CJsonDecoder::DefaultFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt,
	FloatVectorsHolderVector&);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload&, Serializer&, WrSerializer&, CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt,
	FloatVectorsHolderVector&);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt>(
	Payload&, Serializer&, WrSerializer&, CJsonDecoder::RestrictingFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt,
	FloatVectorsHolderVector&);

}  // namespace reindexer
