#include "msgpackdecoder.h"

#include "core/cjson/cjsonbuilder.h"
#include "core/cjson/cjsontools.h"
#include "core/cjson/tagsmatcher.h"
#include "core/enums.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "sparse_validator.h"
#include "tools/catch_and_return.h"
#include "tools/flagguard.h"

namespace reindexer {

using namespace item_fields_validator;

template <typename T, typename Validator>
void MsgPackDecoder::setValue(Payload& pl, CJsonBuilder& builder, const T& value, TagName tagName, const Validator& validator) {
	using namespace std::string_view_literals;
	const auto field = tm_.tags2field(tagsPath_);
	if (field.IsRegularIndex()) {
		const auto indexNumber = field.IndexNumber();
		const auto& f = pl.Type().Field(indexNumber);
		validateNonArrayFieldRestrictions(objectScalarIndexes_, pl, f, indexNumber, isInArray(), kMsgPackFmt);
		if (!isInArray()) {
			validateArrayFieldRestrictions(f.Name(), f.IsArray(), f.ArrayDims(), 1, kMsgPackFmt);
		}
		Variant val(value);
		convertValueForIndexField(f.Type(), f.Name(), val, kMsgPackFmt, ConvertToString_False, ConvertNull_True);
		builder.Ref(tagName, val.Type(), indexNumber);
		pl.Set(indexNumber, std::move(val), Append_True);
		objectScalarIndexes_.set(indexNumber);
	} else if (field.IsIndexed()) {	 // sparse index
		Variant val(value);
		convertValueForIndexField(validator.Type(), validator.Name(), val, kMsgPackFmt, ConvertToString_True, ConvertNull_False);
		validator(val);
		builder.Put(tagName, val);
	} else {
		validator(value);
		builder.Put(tagName, value);
	}
}

TagName MsgPackDecoder::decodeKeyToTag(const msgpack_object_kv& obj) {
	using namespace std::string_view_literals;
	switch (obj.key.type) {
		case MSGPACK_OBJECT_BOOLEAN:
			return tm_.name2tag(obj.key.via.boolean ? "true"sv : "false"sv, CanAddField_True);
		case MSGPACK_OBJECT_POSITIVE_INTEGER:
			return tm_.name2tag(std::to_string(obj.key.via.u64), CanAddField_True);
		case MSGPACK_OBJECT_NEGATIVE_INTEGER:
			return tm_.name2tag(std::to_string(obj.key.via.i64), CanAddField_True);
		case MSGPACK_OBJECT_STR:
			return tm_.name2tag(std::string_view(obj.key.via.str.ptr, obj.key.via.str.size), CanAddField_True);
		case MSGPACK_OBJECT_FLOAT32:
		case MSGPACK_OBJECT_FLOAT64:
		case MSGPACK_OBJECT_NIL:
		case MSGPACK_OBJECT_ARRAY:
		case MSGPACK_OBJECT_MAP:
		case MSGPACK_OBJECT_BIN:
		case MSGPACK_OBJECT_EXT:
			break;
	}
	throw Error(errParams, "Unsupported MsgPack map key type: {}({})", ToString(obj.key.type), int(obj.key.type));
}

void MsgPackDecoder::decode(Payload& pl, CJsonBuilder& builder, const msgpack_object& obj, TagName tagName,
							FloatVectorsHolderVector& floatVectorsHolder) {
	using namespace std::string_view_literals;
	if (!tagName.IsEmpty()) {
		tagsPath_.emplace_back(tagName);
		if (const auto field = tm_.tags2field(tagsPath_); field.IsSparseIndex()) {
			SparseValidator validator{field.ValueType(), field.IsArray(), field.ArrayDim(), field.SparseNumber(), tm_,
									  isInArray(),		 kMsgPackFmt};
			decode(pl, builder, obj, tagName, floatVectorsHolder, validator);
		} else {
			if (field.IsIndexed() && obj.type == MSGPACK_OBJECT_MAP) [[unlikely]] {
				throwUnexpectedObjectInIndex(tm_.Path2Name(tagsPath_), kMsgPackFmt);
			}
			decode(pl, builder, obj, tagName, floatVectorsHolder, kNoValidation);
		}
		tagsPath_.pop_back();
	} else {
		decode(pl, builder, obj, tagName, floatVectorsHolder, kNoValidation);
	}
}

template <typename Validator>
void MsgPackDecoder::decode(Payload& pl, CJsonBuilder& builder, const msgpack_object& obj, TagName tagName,
							FloatVectorsHolderVector& floatVectorsHolder, const Validator& validator) {
	using namespace std::string_view_literals;
	switch (obj.type) {
		case MSGPACK_OBJECT_NIL: {
			const auto field = tm_.tags2field(tagsPath_);
			if (field.IsRegularIndex() && field.ValueType().Is<KeyValueType::FloatVector>()) {
				pl.Set(field.IndexNumber(), Variant{ConstFloatVectorView{}});
				builder.ArrayRef(tagName, field.IndexNumber(), 0);
			} else {
				setValue(pl, builder, Variant{}, tagName, validator);
			}
			break;
		}
		case MSGPACK_OBJECT_BOOLEAN:
			setValue(pl, builder, obj.via.boolean, tagName, validator);
			break;
		case MSGPACK_OBJECT_POSITIVE_INTEGER:
			setValue(pl, builder, int64_t(obj.via.u64), tagName, validator);
			break;
		case MSGPACK_OBJECT_NEGATIVE_INTEGER:
			setValue(pl, builder, obj.via.i64, tagName, validator);
			break;
		case MSGPACK_OBJECT_FLOAT32:
		case MSGPACK_OBJECT_FLOAT64:
			setValue(pl, builder, double(obj.via.f64), tagName, validator);
			break;
		case MSGPACK_OBJECT_STR:
			setValue(pl, builder, p_string(reinterpret_cast<const l_msgpack_hdr*>(&obj.via.str)), tagName, validator);
			break;
		case MSGPACK_OBJECT_ARRAY: {
			CounterGuardIR32 g(arrayLevel_);
			const msgpack_object* const begin = obj.via.array.ptr;
			const msgpack_object* const end = begin + obj.via.array.size;
			const auto [arrayType, isNested, outterSize, fullSize] = analizeArray(begin, end);
			const auto field = tm_.tags2field(tagsPath_);
			if (field.IsRegularIndex()) {
				const auto indexNumber = field.IndexNumber();
				const auto& f = pl.Type().Field(indexNumber);
				if (f.IsFloatVector()) {
					decodeFloatVector(pl, f, indexNumber, builder, tagName, begin, end, fullSize, floatVectorsHolder);
				} else {
					if (!f.IsArray()) [[unlikely]] {
						throwUnexpectedArrayError(f.Name(), f.Type(), kMsgPackFmt);
					}
					validateArrayFieldRestrictions(f.Name(), f.IsArray(), f.ArrayDims(), fullSize, kMsgPackFmt);
					int pos = pl.ResizeArray(indexNumber, fullSize, Append_True);
					if (isNested) {
						builder.HeteroArray(tagName, outterSize);
						decodeNestedArray(pl, f, pos, indexNumber, builder, begin, end);
					} else {
						decodeArray(pl, f, pos, indexNumber, builder, tagName, begin, end, fullSize);
					}
				}
			} else {
				auto array = builder.Array(tagName, arrayType);
				auto arrayValidator = validator.Array();
				for (const msgpack_object* p = begin; p != end; ++p) {
					decode(pl, array, *p, TagName::Empty(), floatVectorsHolder, arrayValidator.Elem());
				}
			}
			break;
		}
		case MSGPACK_OBJECT_MAP: {
			const msgpack_object_kv* begin = obj.via.map.ptr;
			const msgpack_object_kv* end = begin + obj.via.map.size;
			auto object = builder.Object(tagName);
			for (const msgpack_object_kv* p = begin; p != end; ++p) {
				// MsgPack can have non-string type keys: https://github.com/msgpack/msgpack/issues/217
				assertrx(p);
				const TagName tag = decodeKeyToTag(*p);
				decode(pl, object, p->val, tag, floatVectorsHolder);
			}
			break;
		}
		case MSGPACK_OBJECT_BIN:
		case MSGPACK_OBJECT_EXT:
		default:
			throw Error(errParams, "Unsupported MsgPack type: {}({})", ToString(obj.type), int(obj.type));
	}
}

void MsgPackDecoder::decodeFloatVector(Payload& pl, const PayloadFieldType& plFieldType, int indexNumber, CJsonBuilder& builder,
									   TagName tagName, const msgpack_object* const begin, const msgpack_object* const end,
									   size_t arraySize, FloatVectorsHolderVector& floatVectorsHolder) {
	ConstFloatVectorView vectView;
	if (arraySize != 0) {
		if (arraySize != plFieldType.FloatVectorDimension().Value()) {
			throwUnexpectedArraySizeForFloatVectorError(kMsgPackFmt, plFieldType, arraySize);
		}
		auto vect = FloatVector::CreateNotInitialized(plFieldType.FloatVectorDimension());
		size_t pos = 0;
		for (const msgpack_object* p = begin; p != end; ++p, ++pos) {
			assertrx(pos < plFieldType.FloatVectorDimension().Value());
			switch (p->type) {
				case MSGPACK_OBJECT_FLOAT32:
				case MSGPACK_OBJECT_FLOAT64:
					vect.RawData()[pos] = p->via.f64;
					break;
				case MSGPACK_OBJECT_BOOLEAN:
				case MSGPACK_OBJECT_POSITIVE_INTEGER:
				case MSGPACK_OBJECT_NEGATIVE_INTEGER:
				case MSGPACK_OBJECT_STR:
				case MSGPACK_OBJECT_NIL:
				case MSGPACK_OBJECT_ARRAY:
				case MSGPACK_OBJECT_MAP:
				case MSGPACK_OBJECT_BIN:
				case MSGPACK_OBJECT_EXT:
				default:
					throwUnexpectedArrayTypeForFloatVectorError(kMsgPackFmt, plFieldType);
			}
		}
		if (floatVectorsHolder.Add(std::move(vect))) {
			vectView = floatVectorsHolder.Back();
		}
	}
	pl.Set(indexNumber, Variant{vectView});
	builder.ArrayRef(tagName, indexNumber, arraySize);
}

Variant MsgPackDecoder::msgpackValue2Variant(const PayloadFieldType& plFieldType, const msgpack_object& obj) {
	Variant result = [&] {
		switch (obj.type) {
			case MSGPACK_OBJECT_BOOLEAN:
				return Variant{obj.via.boolean};
			case MSGPACK_OBJECT_POSITIVE_INTEGER:
				return Variant{int64_t(obj.via.u64)};
			case MSGPACK_OBJECT_NEGATIVE_INTEGER:
				return Variant{obj.via.i64};
			case MSGPACK_OBJECT_FLOAT32:
			case MSGPACK_OBJECT_FLOAT64:
				return Variant{obj.via.f64};
			case MSGPACK_OBJECT_STR:
				return Variant{p_string(reinterpret_cast<const l_msgpack_hdr*>(&obj.via.str)), Variant::HoldT{}};
			case MSGPACK_OBJECT_NIL:
				return Variant{};
			case MSGPACK_OBJECT_MAP:
				throwUnexpectedObjectInIndex(tm_.Path2Name(tagsPath_), kMsgPackFmt);
			case MSGPACK_OBJECT_ARRAY:
			case MSGPACK_OBJECT_BIN:
			case MSGPACK_OBJECT_EXT:
			default:
				throw Error(errLogic, "Unsupported MsgPack array field type: {}({})", ToString(obj.type), int(obj.type));
		}
	}();
	convertValueForIndexField(plFieldType.Type(), plFieldType.Name(), result, kMsgPackFmt, ConvertToString_False, ConvertNull_True);
	return result;
}

void MsgPackDecoder::decodeArray(Payload& pl, const PayloadFieldType& plFieldType, int& plArrayPos, int indexNumber, CJsonBuilder& builder,
								 TagName tagName, const msgpack_object* const begin, const msgpack_object* const end, size_t arraySize) {
	for (const msgpack_object* p = begin; p != end; ++p) {
		pl.Set(indexNumber, plArrayPos++, msgpackValue2Variant(plFieldType, *p));
	}
	builder.ArrayRef(tagName, indexNumber, arraySize);
}

void MsgPackDecoder::decodeNestedArray(Payload& pl, const PayloadFieldType& plFieldType, int& plArrayPos, int indexNumber,
									   CJsonBuilder& builder, const msgpack_object* const begin, const msgpack_object* const end) {
	for (const msgpack_object* p = begin; p != end; ++p) {
		if (p->type == MSGPACK_OBJECT_ARRAY) {
			const msgpack_object* const nestedBegin = p->via.array.ptr;
			const msgpack_object* const nestedEnd = nestedBegin + p->via.array.size;
			const auto [isNested, nestedSize] = fastAnalizeArray(nestedBegin, nestedEnd);
			if (isNested) {
				builder.HeteroArray(TagName::Empty(), nestedSize);
				decodeNestedArray(pl, plFieldType, plArrayPos, indexNumber, builder, nestedBegin, nestedEnd);
			} else {
				decodeArray(pl, plFieldType, plArrayPos, indexNumber, builder, TagName::Empty(), nestedBegin, nestedEnd, nestedSize);
			}
		} else {
			pl.Set(indexNumber, plArrayPos++, msgpackValue2Variant(plFieldType, *p));
			builder.Ref(TagName::Empty(), plFieldType.Type(), indexNumber);
		}
	}
}

MsgPackDecoder::FastArrayAnalizeResult MsgPackDecoder::fastAnalizeArray(const msgpack_object* const begin,
																		const msgpack_object* const end) {
	FastArrayAnalizeResult result;
	for (const msgpack_object* p = begin; p != end; ++p, ++result.outterSize_) {
		if (p->type == MSGPACK_OBJECT_ARRAY) {
			result.isNested_ = true;
		}
	}
	return result;
}

MsgPackDecoder::ArrayAnalizeResult MsgPackDecoder::analizeArray(const msgpack_object* const begin, const msgpack_object* const end) {
	ArrayAnalizeResult result;
	for (const msgpack_object* p = begin; p != end; ++p, ++result.outterSize_) {
		if (p->type == MSGPACK_OBJECT_ARRAY) {
			result.arrayType_ = ObjType::TypeObjectArray;
			result.isNested_ = true;
			const auto& nestedArray = p->via.array;
			result.fullSize_ += analizeArray(nestedArray.ptr, nestedArray.ptr + nestedArray.size).fullSize_;
		} else {
			++result.fullSize_;
			if (begin->type != p->type) {
				result.arrayType_ = ObjType::TypeObjectArray;
			}
		}
	}
	return result;
}

Error MsgPackDecoder::Decode(std::string_view buf, Payload& pl, WrSerializer& wrser, size_t& offset,
							 FloatVectorsHolderVector& floatVectorsHolder) noexcept {
	try {
		objectScalarIndexes_.reset();
		tagsPath_.clear();
		size_t baseOffset = offset;
		MsgPackValue data = parser_.Parse(buf, offset);
		if (!data.p) [[unlikely]] {
			return Error(errLogic, "Error unpacking msgpack data");
		}
		if (data.p->type != MSGPACK_OBJECT_MAP) [[unlikely]] {
			std::string_view slice = buf.substr(baseOffset, 16);
			return Error(errNotValid, "Unexpected MsgPack value. Expected {}, but got {}({}) at {}(~>\"{}\"...)",
						 ToString(MSGPACK_OBJECT_MAP), ToString(data.p->type), int(data.p->type), baseOffset, slice);
		}
		CJsonBuilder cjsonBuilder(wrser, ObjType::TypePlain, &tm_, TagName::Empty());
		decode(pl, cjsonBuilder, *(data.p), TagName::Empty(), floatVectorsHolder);
	}
	CATCH_AND_RETURN
	return {};
}

constexpr std::string_view ToString(msgpack_object_type type) noexcept {
	using namespace std::string_view_literals;

	switch (type) {
		case MSGPACK_OBJECT_NIL:
			return "NIL"sv;
		case MSGPACK_OBJECT_BOOLEAN:
			return "BOOLEAN"sv;
		case MSGPACK_OBJECT_POSITIVE_INTEGER:
			return "POSITIVE_INTEGER"sv;
		case MSGPACK_OBJECT_NEGATIVE_INTEGER:
			return "NEGATIVE_INTEGER"sv;
		case MSGPACK_OBJECT_FLOAT32:
			return "FLOAT32"sv;
		case MSGPACK_OBJECT_FLOAT64:
			return "FLOAT64"sv;
#if defined(MSGPACK_USE_LEGACY_NAME_AS_FLOAT)
		case MSGPACK_OBJECT_DOUBLE: /* obsolete */
			return "DOUBLE"sv;
#endif /* MSGPACK_USE_LEGACY_NAME_AS_FLOAT */
		case MSGPACK_OBJECT_STR:
			return "STR"sv;
		case MSGPACK_OBJECT_ARRAY:
			return "ARRAY"sv;
		case MSGPACK_OBJECT_MAP:
			return "MAP"sv;
		case MSGPACK_OBJECT_BIN:
			return "BIN"sv;
		case MSGPACK_OBJECT_EXT:
			return "EXT"sv;
	}
	return "UNKNOWN_TYPE"sv;
}

}  // namespace reindexer
