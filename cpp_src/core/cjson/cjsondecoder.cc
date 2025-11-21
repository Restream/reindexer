#include "cjsondecoder.h"
#include "cjsontools.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "sparse_validator.h"
#include "tools/flagguard.h"
#include "tools/serializer.h"

namespace reindexer {

using namespace item_fields_validator;

template <typename Filter, typename Recoder, typename TagOptT>
bool CJsonDecoder::decodeCJson(Payload& pl, Serializer& rdser, WrSerializer& wrser, Filter filter, Recoder recoder, TagOptT,
							   FloatVectorsHolderVector& floatVectorsHolder) {
	using namespace std::string_view_literals;
	const ctag tag = rdser.GetCTag();
	TagType tagType = tag.Type();
	if (tag == kCTagEnd) {
		wrser.PutCTag(kCTagEnd);
		return false;
	}
	TagName tagName = TagName::Empty();
	if constexpr (std::is_same_v<TagOptT, NamedTagOpt>) {
		tagName = tag.Name();
		assertrx_dbg(!tagName.IsEmpty());
		// Check if tag exists
		std::ignore = tagsMatcher_.tag2name(tagName);
		tagsPath_.emplace_back(tagName);
	}

	if (tag.Field() >= 0) [[unlikely]] {
		throwTagReferenceError(tag, pl);
	}

	const auto field = tagsMatcher_.tags2field(tagsPath_);
	if (field.IsRegularIndex()) {
		const auto indexNumber = field.IndexNumber();
		if (filter.contains(indexNumber)) {
			tagType = recoder.RegisterTagType(tagType, indexNumber);
			if (tagType == TAG_NULL) {
				wrser.PutCTag(ctag{TAG_NULL, tagName});
			} else if (recoder.Recode(rdser, pl, tagName, wrser)) {
				// No more actions needed after recoding
			} else {
				const auto& fieldRef{pl.Type().Field(indexNumber)};
				const KeyValueType fieldType{fieldRef.Type()};
				if (tagType == TAG_ARRAY) {
					const carraytag atag = rdser.GetCArrayTag();
					const auto count = atag.Count();
					const TagType atagType = atag.Type();
					if (fieldRef.IsFloatVector()) {
						ConstFloatVectorView vectView;
						if (count != 0) {
							if (atagType != TAG_DOUBLE && atagType != TAG_FLOAT && atagType != TAG_VARINT) {
								throwUnexpectedArrayTypeForFloatVectorError(kCJSONFmt, fieldRef);
							}
							if (count != fieldRef.FloatVectorDimension().Value()) {
								throwUnexpectedArraySizeForFloatVectorError(kCJSONFmt, fieldRef, count);
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
							if (floatVectorsHolder.Add(std::move(vect))) {
								vectView = floatVectorsHolder.Back();
							}
						}
						objectScalarIndexes_.set(indexNumber);	// Indexed float vector is treated as scalar value
						pl.Set(indexNumber, Variant{vectView});
						wrser.PutCTag(ctag{TAG_ARRAY, tagName, indexNumber});
						wrser.PutVarUint(count);
					} else if (fieldRef.IsArray()) [[likely]] {
						if (atagType == TAG_OBJECT) {
							Serializer rdserCopy{rdser};
							const auto [size, isNested] = analizeNestedArray(count, rdserCopy);
							validateArrayFieldRestrictions(fieldRef.Name(), fieldRef.IsArray(), fieldRef.ArrayDims(), size, kCJSONFmt);
							const int ofs = pl.ResizeArray(indexNumber, size, Append_True);
							if (isNested) {
								wrser.PutCTag(ctag{TAG_ARRAY, tagName});
								wrser.PutCArrayTag(carraytag{count, TAG_OBJECT});
								[[maybe_unused]] const auto decodedValuesCount =
									decodeNestedArray(pl, rdser, wrser, indexNumber, count, fieldType, ofs);
								assertrx_dbg(size == decodedValuesCount);
							} else {
								assertrx_dbg(size == count);
								for (size_t i = 0; i < count; ++i) {
									pl.Set(indexNumber, ofs + i, cjsonValueToVariant(rdser.GetCTag().Type(), rdser, fieldType));
								}
								wrser.PutCTag(ctag{TAG_ARRAY, tagName, indexNumber});
								wrser.PutVarUint(count);
							}
						} else {
							validateArrayFieldRestrictions(fieldRef.Name(), fieldRef.IsArray(), fieldRef.ArrayDims(), count, "cjson"sv);
							const int ofs = pl.ResizeArray(indexNumber, count, Append_True);
							for (size_t i = 0; i < count; ++i) {
								pl.Set(indexNumber, ofs + i, cjsonValueToVariant(atagType, rdser, fieldType));
							}
							wrser.PutCTag(ctag{TAG_ARRAY, tagName, indexNumber});
							wrser.PutVarUint(count);
						}
					} else {
						throwUnexpectedArrayError(fieldRef.Name(), fieldRef.Type(), kCJSONFmt);
					}
				} else {
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, fieldRef, indexNumber, isInArray(), kCJSONFmt);
					validateArrayFieldRestrictions(fieldRef.Name(), fieldRef.IsArray(), fieldRef.ArrayDims(), 1, kCJSONFmt);
					objectScalarIndexes_.set(indexNumber);
					pl.Set(indexNumber, cjsonValueToVariant(tagType, rdser, fieldType), Append_True);
					wrser.PutCTag(fieldType, tagName, indexNumber);
				}
			}
		} else {
			// objectScalarIndexes_.set(indexNumber); - do not change objectScalarIndexes_ value for the filtered out fields
			skipCjsonTag(tag, rdser);
		}
	} else if (field.IsIndexed()) {	 // sparse index
		decodeCJson(pl, rdser, wrser, filter, recoder, tagType, tagName, tag, floatVectorsHolder,
					SparseValidator{field.ValueType(), field.IsArray(), field.ArrayDim(), field.SparseNumber(), tagsMatcher_, isInArray(),
									kCJSONFmt});
	} else {
		decodeCJson(pl, rdser, wrser, filter, recoder, tagType, tagName, tag, floatVectorsHolder, kNoValidation);
	}

	if constexpr (std::is_same_v<TagOptT, NamedTagOpt>) {
		tagsPath_.pop_back();
	}

	return true;
}

size_t CJsonDecoder::decodeNestedArray(Payload& pl, Serializer& rdser, WrSerializer& wrser, int indexNumber, size_t count,
									   KeyValueType fieldType, size_t offset) const {
	size_t decodedValuesCount = 0;
	for (size_t i = 0; i < count; ++i) {
		const auto tag = rdser.GetCTag();
		const auto tagType = tag.Type();
		if (tagType == TAG_ARRAY) {
			const auto nestedArr = rdser.GetCArrayTag();
			const auto nestedArrCount = nestedArr.Count();
			const auto nestedArrType = nestedArr.Type();
			if (nestedArrType == TAG_OBJECT) {
				wrser.PutCTag(ctag{TAG_ARRAY});
				wrser.PutCArrayTag(carraytag{nestedArrCount, TAG_OBJECT});
				const auto decodedValuesCountInNested = decodeNestedArray(pl, rdser, wrser, indexNumber, nestedArrCount, fieldType, offset);
				offset += decodedValuesCountInNested;
				decodedValuesCount += decodedValuesCountInNested;
			} else {
				for (size_t j = 0; j < nestedArrCount; ++j) {
					pl.Set(indexNumber, offset, cjsonValueToVariant(nestedArrType, rdser, fieldType));
					++offset;
				}
				decodedValuesCount += nestedArrCount;
				wrser.PutCTag(ctag{TAG_ARRAY, TagName::Empty(), indexNumber});
				wrser.PutVarUint(nestedArrCount);
			}
		} else {
			pl.Set(indexNumber, offset, cjsonValueToVariant(tagType, rdser, fieldType));
			wrser.PutCTag(fieldType, TagName::Empty(), indexNumber);
			++decodedValuesCount;
			++offset;
		}
	}
	return decodedValuesCount;
}

template <typename Filter, typename Recoder, typename Validator>
void CJsonDecoder::decodeCJson(Payload& pl, Serializer& rdser, WrSerializer& wrser, Filter filter, Recoder recoder, TagType tagType,
							   TagName tagName, ctag tag, FloatVectorsHolderVector& floatVectorsHolder, const Validator& validator) {
	const bool match = filter.match(tagsPath_);
	if (match) {
		tagType = recoder.RegisterTagType(tagType, tagsPath_);
		wrser.PutCTag(ctag{tagType, tagName});
		if (tagType == TAG_OBJECT) {
			while (decodeCJson(pl, rdser, wrser, filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamedTagOpt{}, floatVectorsHolder));
		} else if (recoder.Recode(rdser, wrser)) {
			// No more actions needed after recoding
		} else if (tagType == TAG_ARRAY) {
			auto val = validator.Array();
			const carraytag atag = rdser.GetCArrayTag();
			wrser.PutCArrayTag(atag);
			const auto count = atag.Count();
			const TagType atagType = atag.Type();
			CounterGuardIR32 g(arrayLevel_);
			if (atagType == TAG_OBJECT) {
				for (size_t i = 0; i < count; ++i) {
					decodeCJson(pl, rdser, wrser, filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamelessTagOpt{}, floatVectorsHolder);
				}
			} else {
				for (size_t i = 0; i < count; ++i) {
					copyCJsonValue(atagType, rdser, wrser, val.Elem());
				}
			}
		} else {
			copyCJsonValue(tagType, rdser, wrser, validator);
		}
	} else if (tagType != TAG_OBJECT) {
		// !match
		skipCjsonTag(tag, rdser);
	} else {
		// !match
		wrser.PutCTag(ctag{tagType, tagName});
		while (decodeCJson(pl, rdser, wrser, filter.MakeSkipFilter(), recoder.MakeCleanCopy(), NamedTagOpt{}, floatVectorsHolder));
	}
}

Variant CJsonDecoder::cjsonValueToVariant(TagType tagType, Serializer& rdser, KeyValueType fieldType) const {
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
