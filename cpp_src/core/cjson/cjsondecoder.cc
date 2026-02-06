#include "cjsondecoder.h"
#include "cjsontools.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "sparse_validator.h"
#include "tools/flagguard.h"
#include "tools/serializer.h"

namespace reindexer {

using namespace item_fields_validator;

template <typename Filter, typename Recoder, typename TagOptT>
bool CJsonDecoder::decodeCJson(Filter filter, Recoder recoder, TagOptT) {
	using namespace std::string_view_literals;
	const ctag tag = rdSer_.GetCTag();
	TagType tagType = tag.Type();
	if (tag == kCTagEnd) {
		wrSer_.PutCTag(kCTagEnd);
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
		throwTagReferenceError(tag, pl_);
	}

	const auto field = tagsMatcher_.tags2field(tagsPath_);
	if (field.IsRegularIndex()) {
		const auto indexNumber = field.IndexNumber();
		if (filter.contains(indexNumber)) {
			tagType = recoder.RegisterTagType(tagType, indexNumber);
			if (tagType == TAG_NULL) {
				wrSer_.PutCTag(ctag{TAG_NULL, tagName});
			} else if (recoder.Recode(rdSer_, pl_, tagName, wrSer_)) {
				// No more actions needed after recoding
			} else {
				const auto& fieldRef{pl_.Type().Field(indexNumber)};
				const KeyValueType fieldType{fieldRef.Type()};
				if (tagType == TAG_ARRAY) {
					const carraytag atag = rdSer_.GetCArrayTag();
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
									vect.RawData()[i] = rdSer_.GetDouble();
								}
							} else if (atagType == TAG_FLOAT) {
								for (size_t i = 0; i < count; ++i) {
									vect.RawData()[i] = rdSer_.GetFloat();
								}
							} else if (atagType == TAG_VARINT) {
								for (size_t i = 0; i < count; ++i) {
									vect.RawData()[i] = rdSer_.GetVarint();
								}
							}
							if (floatVectorsHolder_.Add(std::move(vect))) {
								vectView = floatVectorsHolder_.Back();
							}
						}
						objectScalarIndexes_.set(indexNumber);	// Indexed float vector is treated as scalar value
						pl_.Set(indexNumber, Variant{vectView});
						wrSer_.PutCTag(ctag{TAG_ARRAY, tagName, indexNumber});
						wrSer_.PutVarUint(count);
					} else if (fieldRef.IsArray()) [[likely]] {
						if (atagType == TAG_OBJECT) {
							Serializer rdserCopy{rdSer_};
							const auto [size, isNested] = analizeNestedArray(count, rdserCopy);
							validateArrayFieldRestrictions(fieldRef.Name(), fieldRef.IsArray(), fieldRef.ArrayDims(), size, kCJSONFmt);
							const int ofs = pl_.ResizeArray(indexNumber, size, Append_True);
							if (isNested) {
								wrSer_.PutCTag(ctag{TAG_ARRAY, tagName});
								wrSer_.PutCArrayTag(carraytag{count, TAG_OBJECT});
								[[maybe_unused]] const auto decodedValuesCount = decodeNestedArray(indexNumber, count, fieldType, ofs);
								assertrx_dbg(size == decodedValuesCount);
							} else {
								assertrx_dbg(size == count);
								for (size_t i = 0; i < count; ++i) {
									pl_.Set(indexNumber, ofs + i, cjsonValueToVariant(rdSer_.GetCTag().Type(), fieldType));
								}
								wrSer_.PutCTag(ctag{TAG_ARRAY, tagName, indexNumber});
								wrSer_.PutVarUint(count);
							}
						} else {
							validateArrayFieldRestrictions(fieldRef.Name(), fieldRef.IsArray(), fieldRef.ArrayDims(), count, "cjson"sv);
							const int ofs = pl_.ResizeArray(indexNumber, count, Append_True);
							for (size_t i = 0; i < count; ++i) {
								pl_.Set(indexNumber, ofs + i, cjsonValueToVariant(atagType, fieldType));
							}
							wrSer_.PutCTag(ctag{TAG_ARRAY, tagName, indexNumber});
							wrSer_.PutVarUint(count);
						}
					} else {
						throwUnexpectedArrayError(fieldRef.Name(), fieldRef.Type(), kCJSONFmt);
					}
				} else {
					validateNonArrayFieldRestrictions(objectScalarIndexes_, pl_, fieldRef, indexNumber, isInArray(), kCJSONFmt);
					validateArrayFieldRestrictions(fieldRef.Name(), fieldRef.IsArray(), fieldRef.ArrayDims(), 1, kCJSONFmt);
					objectScalarIndexes_.set(indexNumber);
					pl_.Set(indexNumber, cjsonValueToVariant(tagType, fieldType), Append_True);
					wrSer_.PutCTag(fieldType, tagName, indexNumber);
				}
			}
		} else {
			// objectScalarIndexes_.set(indexNumber); - do not change objectScalarIndexes_ value for the filtered out fields
			skipCjsonTag(tag, rdSer_);
		}
	} else if (field.IsIndexed()) {	 // sparse index
		decodeCJson(filter, recoder, tagType, tagName, tag,
					SparseValidator{field.ValueType(), field.IsArray(), field.ArrayDim(), field.SparseNumber(), tagsMatcher_, isInArray(),
									kCJSONFmt});
	} else {
		decodeCJson(filter, recoder, tagType, tagName, tag, kNoValidation);
	}

	if constexpr (std::is_same_v<TagOptT, NamedTagOpt>) {
		tagsPath_.pop_back();
	}

	return true;
}

size_t CJsonDecoder::decodeNestedArray(int indexNumber, size_t count, KeyValueType fieldType, size_t offset) const {
	size_t decodedValuesCount = 0;
	for (size_t i = 0; i < count; ++i) {
		const auto tag = rdSer_.GetCTag();
		const auto tagType = tag.Type();
		if (tagType == TAG_ARRAY) {
			const auto nestedArr = rdSer_.GetCArrayTag();
			const auto nestedArrCount = nestedArr.Count();
			const auto nestedArrType = nestedArr.Type();
			if (nestedArrType == TAG_OBJECT) {
				wrSer_.PutCTag(ctag{TAG_ARRAY});
				wrSer_.PutCArrayTag(carraytag{nestedArrCount, TAG_OBJECT});
				const auto decodedValuesCountInNested = decodeNestedArray(indexNumber, nestedArrCount, fieldType, offset);
				offset += decodedValuesCountInNested;
				decodedValuesCount += decodedValuesCountInNested;
			} else {
				for (size_t j = 0; j < nestedArrCount; ++j) {
					pl_.Set(indexNumber, offset, cjsonValueToVariant(nestedArrType, fieldType));
					++offset;
				}
				decodedValuesCount += nestedArrCount;
				wrSer_.PutCTag(ctag{TAG_ARRAY, TagName::Empty(), indexNumber});
				wrSer_.PutVarUint(nestedArrCount);
			}
		} else {
			pl_.Set(indexNumber, offset, cjsonValueToVariant(tagType, fieldType));
			wrSer_.PutCTag(fieldType, TagName::Empty(), indexNumber);
			++decodedValuesCount;
			++offset;
		}
	}
	return decodedValuesCount;
}

template <typename Filter, typename Recoder, typename Validator>
void CJsonDecoder::decodeCJson(Filter filter, Recoder recoder, TagType tagType, TagName tagName, ctag tag, const Validator& validator) {
	const bool match = filter.match(tagsPath_);
	if (match) {
		tagType = recoder.RegisterTagType(tagType, tagsPath_);
		wrSer_.PutCTag(ctag{tagType, tagName});
		if (tagType == TAG_OBJECT) {
			while (decodeCJson(filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamedTagOpt{}));
		} else if (recoder.Recode(rdSer_, wrSer_)) {
			// No more actions needed after recoding
		} else if (tagType == TAG_ARRAY) {
			auto val = validator.Array();
			const carraytag atag = rdSer_.GetCArrayTag();
			wrSer_.PutCArrayTag(atag);
			const auto count = atag.Count();
			const TagType atagType = atag.Type();
			CounterGuardIR32 g(arrayLevel_);
			if (atagType == TAG_OBJECT) {
				for (size_t i = 0; i < count; ++i) {
					decodeCJson(filter.MakeCleanCopy(), recoder.MakeCleanCopy(), NamelessTagOpt{});
				}
			} else {
				for (size_t i = 0; i < count; ++i) {
					copyCJsonValue(atagType, rdSer_, wrSer_, val.Elem());
				}
			}
		} else {
			copyCJsonValue(tagType, rdSer_, wrSer_, validator);
		}
	} else if (tagType != TAG_OBJECT) {
		// !match
		skipCjsonTag(tag, rdSer_);
	} else {
		// !match
		wrSer_.PutCTag(ctag{tagType, tagName});
		while (decodeCJson(filter.MakeSkipFilter(), recoder.MakeCleanCopy(), NamedTagOpt{}));
	}
}

Variant CJsonDecoder::cjsonValueToVariant(TagType tagType, KeyValueType fieldType) const {
	if (fieldType.Is<KeyValueType::String>() && tagType != TagType::TAG_STRING) {
		auto& back = storage_.emplace_back(rdSer_.GetRawVariant(KeyValueType{tagType}).As<key_string>());
		return Variant(p_string(back), Variant::noHold);
	} else {
		return reindexer::cjsonValueToVariant(tagType, rdSer_, fieldType);
	}
}

RX_NO_INLINE void CJsonDecoder::throwTagReferenceError(ctag tag, const Payload& pl) {
	throw Error(errLogic, "Reference tag was found in transport CJSON for field {}[{}] in ns [{}]", tag.Field(),
				tagsMatcher_.tag2name(tag.Name()), pl.Type().Name());
}

template bool CJsonDecoder::decodeCJson<CJsonDecoder::DefaultFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	CJsonDecoder::DefaultFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::DefaultFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt>(
	CJsonDecoder::DefaultFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt>(
	CJsonDecoder::RestrictingFilter, CJsonDecoder::DefaultRecoder, CJsonDecoder::NamelessTagOpt);
template bool CJsonDecoder::decodeCJson<CJsonDecoder::RestrictingFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt>(
	CJsonDecoder::RestrictingFilter, CJsonDecoder::CustomRecoder, CJsonDecoder::NamelessTagOpt);

}  // namespace reindexer
