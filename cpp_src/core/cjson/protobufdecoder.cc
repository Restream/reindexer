#include "protobufdecoder.h"
#include "core/cjson/cjsontools.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "core/schema.h"
#include "estl/protobufparser.h"
#include "sparse_validator.h"
#include "tools/catch_and_return.h"
#include "tools/flagguard.h"

namespace reindexer {

using namespace item_fields_validator;

void ArraysStorage::UpdateArraySize(TagName tagName, int field) { std::ignore = GetArray(tagName, field); }

CJsonBuilder& ArraysStorage::GetArray(TagName tagName, int field) {
	assertrx(!indexes_.empty());
	auto it = data_.find(tagName);
	if (it == data_.end()) {
		indexes_.back().emplace_back(tagName);
		auto itArrayData =
			data_.emplace(std::piecewise_construct, std::forward_as_tuple(tagName), std::forward_as_tuple(&tm_, tagName, field));
		itArrayData.first->second.size = 1;
		return itArrayData.first->second.builder;
	} else {
		++(it->second.size);
		return it->second.builder;
	}
}

void ArraysStorage::onAddObject() { indexes_.emplace_back(); }

void ArraysStorage::onObjectBuilt(CJsonBuilder& parent) {
	assertrx(!indexes_.empty());
	for (TagName tagName : indexes_.back()) {
		auto it = data_.find(tagName);
		assertrx(it != data_.end());
		ArrayData& arrayData = it->second;
		if (arrayData.field == IndexValueType::NotSet) {
			arrayData.builder.End();
			parent.Write(arrayData.ser.Slice());
		} else {
			parent.ArrayRef(it->first, arrayData.field, arrayData.size);
		}
		data_.erase(it);
	}
	indexes_.pop_back();
}

template <typename Validator>
void ProtobufDecoder::setValue(Payload& pl, CJsonBuilder& builder, ProtobufValue item, const Validator& validator) {
	using namespace std::string_view_literals;
	const auto field = tm_.tags2field(tagsPath_);
	if (item.itemType.Is<KeyValueType::Composite>()) [[unlikely]] {
		throwUnexpectedObjectInIndex(tm_.Path2Name(tagsPath_), kProtobufFmt);
	}
	auto value = item.value.convert(item.itemType);
	if (field.IsRegularIndex()) {
		const auto indexNumber = field.IndexNumber();
		const auto& f = pl.Type().Field(indexNumber);
		if (!f.IsArray() && objectScalarIndexes_.test(indexNumber)) [[unlikely]] {
			throw Error(errLogic, "Non-array field '{}' [{}] from '{}' can only be encoded once.", f.Name(), indexNumber, pl.Type().Name());
		}
		if (item.isArray) {
			arraysStorage_.UpdateArraySize(item.tagName, indexNumber);
		} else {
			validateArrayFieldRestrictions(f.Name(), f.IsArray(), f.ArrayDims(), 1, kProtobufFmt);
			builder.Ref(item.tagName, value.Type(), indexNumber);
		}
		convertValueForIndexField(pl.Type().Field(indexNumber).Type(), pl.Type().Field(indexNumber).Name(), value, kProtobufFmt,
								  ConvertToString_False, ConvertNull_True);
		pl.Set(indexNumber, std::move(value), Append_True);
		objectScalarIndexes_.set(indexNumber);
	} else {
		convertValueForIndexField(validator.Type(), validator.Name(), value, kProtobufFmt, ConvertToString_True, ConvertNull_False);
		validator(value);
		if (item.isArray) {
			auto& array = arraysStorage_.GetArray(item.tagName);
			array.Put(TagName::Empty(), value);
		} else {
			builder.Put(item.tagName, value);
		}
	}
}

template <typename Validator>
void ProtobufDecoder::decodeArray(Payload& pl, CJsonBuilder& builder, const ProtobufValue& item,
								  FloatVectorsHolderVector& floatVectorsHolder, Validator&& validator) {
	using namespace std::string_view_literals;
	CounterGuardIR32 g(arrayLevel_);
	ProtobufObject object(item.As<std::string_view>(), *schema_, tagsPath_, tm_);
	ProtobufParser parser(object);
	const bool packed = item.IsOfPrimitiveType();
	const auto field = tm_.tags2field(tagsPath_);
	if (field.IsRegularIndex()) {
		const auto indexNumber = field.IndexNumber();
		const auto& f = pl.Type().Field(indexNumber);
		if (f.IsFloatVector()) {
			if (!item.itemType.IsNumeric() || item.itemType.Is<KeyValueType::Bool>()) {
				throwUnexpectedArrayTypeForFloatVectorError(kProtobufFmt, f);
			}
			ConstFloatVectorView vectView;
			size_t count = 0;
			if (!parser.IsEof()) {
				auto vect = FloatVector::CreateNotInitialized(f.FloatVectorDimension());
				while (!parser.IsEof()) {
					if (count >= f.FloatVectorDimension().Value()) {
						throwUnexpectedArraySizeForFloatVectorError(kProtobufFmt, f, count);
					}
					const Variant value = parser.ReadArrayItem(item.itemType);
					vect.RawData()[count] = value.As<float>();
					++count;
				}
				if (count != f.FloatVectorDimension().Value()) {
					throwUnexpectedArraySizeForFloatVectorError(kProtobufFmt, f, count);
				}
				if (floatVectorsHolder.Add(std::move(vect))) {
					vectView = floatVectorsHolder.Back();
				}
			}
			pl.Set(indexNumber, Variant{vectView});
			builder.ArrayRef(item.tagName, indexNumber, count);
		} else {
			if (!f.IsArray()) [[unlikely]] {
				throwUnexpectedArrayError(f.Name(), f.Type(), kProtobufFmt);
			}
			if (packed) {
				int count = 0;
				while (!parser.IsEof()) {
					Variant val = parser.ReadArrayItem(item.itemType);
					convertValueForIndexField(pl.Type().Field(indexNumber).Type(), pl.Type().Field(indexNumber).Name(), val, kProtobufFmt,
											  ConvertToString_False, ConvertNull_True);
					pl.Set(indexNumber, std::move(val), Append_True);
					++count;
				}
				builder.ArrayRef(item.tagName, indexNumber, count);
			} else {
				setValue(pl, builder, item, kNoValidation);
			}
			validateArrayFieldRestrictions(f.Name(), f.IsArray(), f.ArrayDims(),
										   reinterpret_cast<PayloadFieldValue::Array*>(pl.Field(indexNumber).p_)->len, kProtobufFmt);
		}
	} else {
		CJsonBuilder& array = arraysStorage_.GetArray(item.tagName);
		if (packed) {
			while (!parser.IsEof()) {
				Variant value = parser.ReadArrayItem(item.itemType);
				convertValueForIndexField(validator.Type(), validator.Name(), value, kProtobufFmt, ConvertToString_True, ConvertNull_False);
				validator.Elem()(value);
				array.Put(TagName::Empty(), value);
			}
		} else {
			if (item.itemType.Is<KeyValueType::Composite>()) {
				CJsonProtobufObjectBuilder obj(array, TagName::Empty(), arraysStorage_);
				while (!parser.IsEof()) {
					decode(pl, obj, parser.ReadValue(), floatVectorsHolder, validator.Elem());
				}
			} else {
				setValue(pl, array, item, validator.Elem());
			}
		}
	}
}

void ProtobufDecoder::decode(Payload& pl, CJsonBuilder& builder, const ProtobufValue& item, FloatVectorsHolderVector& floatVectorsHolder) {
	using namespace std::string_view_literals;
	if (!item.tagName.IsEmpty()) {
		TagsPathScope<TagsPath> tagScope(tagsPath_, item.tagName);
		if (const auto field = tm_.tags2field(tagsPath_); field.IsSparseIndex()) {
			SparseValidator validator{field.ValueType(), field.IsArray(), field.ArrayDim(), field.SparseNumber(), tm_,
									  isInArray(),		 kProtobufFmt};
			decode(pl, builder, item, floatVectorsHolder, validator);
		} else {
			if (field.IsIndexed() && item.itemType.Is<KeyValueType::Composite>()) [[unlikely]] {
				throwUnexpectedObjectInIndex(tm_.Path2Name(tagsPath_), kProtobufFmt);
			}
			decode(pl, builder, item, floatVectorsHolder, kNoValidation);
		}
	} else {
		decode(pl, builder, item, floatVectorsHolder, kNoValidation);
	}
}

template <typename Validator>
void ProtobufDecoder::decode(Payload& pl, CJsonBuilder& builder, const ProtobufValue& item, FloatVectorsHolderVector& floatVectorsHolder,
							 const Validator& validator) {
	item.value.Type().EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::Bool> auto) {
			setValue(pl, builder, item, validator);
		},
		[&](KeyValueType::String) {
			if (item.isArray) {
				decodeArray(pl, builder, item, floatVectorsHolder, validator.Array());
			} else {
				item.itemType.EvaluateOneOf(
					[&](KeyValueType::String) { setValue(pl, builder, item, validator); },
					[&](KeyValueType::Composite) {
						CJsonProtobufObjectBuilder objBuilder(builder, item.tagName, arraysStorage_);
						ProtobufObject object(item.As<std::string_view>(), *schema_, tagsPath_, tm_);
						decodeObject(pl, objBuilder, object, floatVectorsHolder);
					},
					[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double,
										KeyValueType::Float, KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined,
										KeyValueType::Uuid, KeyValueType::FloatVector> auto) {
						throw Error(errParseProtobuf, "Error parsing length-encoded type: [{}] for field [{}]", item.itemType.Name(),
									tm_.tag2name(item.tagName));
					});
			}
		},
		[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Uuid,
							KeyValueType::FloatVector> auto) {
			throw Error(errParseProtobuf, "Unknown field type [{}] while parsing Protobuf", item.value.Type().Name());
		});
}

void ProtobufDecoder::decodeObject(Payload& pl, CJsonBuilder& builder, ProtobufObject& object,
								   FloatVectorsHolderVector& floatVectorsHolder) {
	ProtobufParser parser(object);
	while (!parser.IsEof()) {
		decode(pl, builder, parser.ReadValue(), floatVectorsHolder);
	}
}

Error ProtobufDecoder::Decode(std::string_view buf, Payload& pl, WrSerializer& wrser,
							  FloatVectorsHolderVector& floatVectorsHolder) noexcept {
	try {
		if (!schema_) [[unlikely]] {
			return Error(errParams, "Namespace JSON schema is not set. Unable to use protobuf decoder without schema");
		}
		tagsPath_.clear();
		objectScalarIndexes_.reset();
		CJsonProtobufObjectBuilder cjsonBuilder(arraysStorage_, wrser, &tm_, TagName::Empty());
		ProtobufObject object(buf, *schema_, tagsPath_, tm_);
		decodeObject(pl, cjsonBuilder, object, floatVectorsHolder);
	}
	CATCH_AND_RETURN
	return {};
}

}  // namespace reindexer
