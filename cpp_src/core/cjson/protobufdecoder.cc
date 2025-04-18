#include "protobufdecoder.h"
#include "core/cjson/cjsontools.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "core/schema.h"
#include "estl/protobufparser.h"

namespace reindexer {

void ArraysStorage::UpdateArraySize(TagName tagName, int field) { GetArray(tagName, field); }

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

void ArraysStorage::onAddObject() { indexes_.emplace_back(h_vector<TagName, 1>()); }

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

void ProtobufDecoder::setValue(Payload& pl, CJsonBuilder& builder, ProtobufValue item) {
	using namespace std::string_view_literals;
	const int field = tm_.tags2field(tagsPath_);
	auto value = item.value.convert(item.itemType);
	if (field > 0) {
		const auto& f = pl.Type().Field(field);
		if rx_unlikely (!f.IsArray() && objectScalarIndexes_.test(field)) {
			throw Error(errLogic, "Non-array field '{}' [{}] from '{}' can only be encoded once.", f.Name(), field, pl.Type().Name());
		}
		if (item.isArray) {
			arraysStorage_.UpdateArraySize(item.tagName, field);
		} else {
			validateArrayFieldRestrictions(f, 1, "protobuf"sv);
			builder.Ref(item.tagName, value.Type(), field);
		}
		pl.Set(field, convertValueForPayload(pl, field, std::move(value), "protobuf"sv), true);
		objectScalarIndexes_.set(field);
	} else {
		if (item.isArray) {
			auto& array = arraysStorage_.GetArray(item.tagName);
			array.Put(TagName::Empty(), value);
		} else {
			builder.Put(item.tagName, value);
		}
	}
}

Error ProtobufDecoder::decodeArray(Payload& pl, CJsonBuilder& builder, const ProtobufValue& item,
								   FloatVectorsHolderVector& floatVectorsHolder) {
	using namespace std::string_view_literals;
	ProtobufObject object(item.As<std::string_view>(), *schema_, tagsPath_, tm_);
	ProtobufParser parser(object);
	const bool packed = item.IsOfPrimitiveType();
	const int field = tm_.tags2field(tagsPath_);
	if (field > 0) {
		const auto& f = pl.Type().Field(field);
		if (f.IsFloatVector()) {
			if (!item.itemType.IsNumeric() || item.itemType.Is<KeyValueType::Bool>()) {
				throwUnexpectedArrayTypeForFloatVectorError("protobuf"sv, f);
			}
			ConstFloatVectorView vectView;
			size_t count = 0;
			if (!parser.IsEof()) {
				auto vect = FloatVector::CreateNotInitialized(f.FloatVectorDimension());
				while (!parser.IsEof()) {
					if (count >= f.FloatVectorDimension().Value()) {
						throwUnexpectedArraySizeForFloatVectorError("protobuf"sv, f, count);
					}
					const Variant value = parser.ReadArrayItem(item.itemType);
					vect.RawData()[count] = value.As<float>();
					++count;
				}
				if (count != f.FloatVectorDimension().Value()) {
					throwUnexpectedArraySizeForFloatVectorError("protobuf"sv, f, count);
				}
				floatVectorsHolder.Add(std::move(vect));
				vectView = floatVectorsHolder.Back();
			}
			pl.Set(field, Variant{vectView});
			builder.ArrayRef(item.tagName, field, count);
		} else {
			if rx_unlikely (!f.IsArray()) {
				throwUnexpectedArrayError("protobuf"sv, f);
			}
			if (packed) {
				int count = 0;
				while (!parser.IsEof()) {
					pl.Set(field, convertValueForPayload(pl, field, parser.ReadArrayItem(item.itemType), "protobuf"sv), true);
					++count;
				}
				builder.ArrayRef(item.tagName, field, count);
			} else {
				setValue(pl, builder, item);
			}
			validateArrayFieldRestrictions(f, reinterpret_cast<PayloadFieldValue::Array*>(pl.Field(field).p_)->len, "protobuf"sv);
		}
	} else {
		CJsonBuilder& array = arraysStorage_.GetArray(item.tagName);
		if (packed) {
			while (!parser.IsEof()) {
				array.Put(TagName::Empty(), parser.ReadArrayItem(item.itemType));
			}
		} else {
			if (item.itemType.Is<KeyValueType::Composite>()) {
				Error status;
				CJsonProtobufObjectBuilder obj(array, TagName::Empty(), arraysStorage_);
				while (status.ok() && !parser.IsEof()) {
					status = decode(pl, obj, parser.ReadValue(), floatVectorsHolder);
				}
			} else {
				setValue(pl, array, item);
			}
		}
	}
	return {};
}

Error ProtobufDecoder::decode(Payload& pl, CJsonBuilder& builder, const ProtobufValue& item, FloatVectorsHolderVector& floatVectorsHolder) {
	TagsPathScope<TagsPath> tagScope(tagsPath_, item.tagName);
	return item.value.Type().EvaluateOneOf(
		[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::Bool>) {
			setValue(pl, builder, item);
			return Error{};
		},
		[&](KeyValueType::String) {
			if (item.isArray) {
				return decodeArray(pl, builder, item, floatVectorsHolder);
			} else {
				return item.itemType.EvaluateOneOf(
					[&](KeyValueType::String) {
						setValue(pl, builder, item);
						return Error{};
					},
					[&](KeyValueType::Composite) {
						CJsonProtobufObjectBuilder objBuilder(builder, item.tagName, arraysStorage_);
						ProtobufObject object(item.As<std::string_view>(), *schema_, tagsPath_, tm_);
						return decodeObject(pl, objBuilder, object, floatVectorsHolder);
					},
					[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double, KeyValueType::Float,
							  KeyValueType::Null, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Uuid,
							  KeyValueType::FloatVector>) {
						return Error(errParseProtobuf, "Error parsing length-encoded type: [{}] for field [{}]", item.itemType.Name(),
									 tm_.tag2name(item.tagName));
					});
			}
		},
		[&](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Uuid,
				  KeyValueType::FloatVector>) {
			return Error(errParseProtobuf, "Unknown field type [{}] while parsing Protobuf", item.value.Type().Name());
		});
}

Error ProtobufDecoder::decodeObject(Payload& pl, CJsonBuilder& builder, ProtobufObject& object,
									FloatVectorsHolderVector& floatVectorsHolder) {
	Error status;
	ProtobufParser parser(object);
	while (status.ok() && !parser.IsEof()) {
		status = decode(pl, builder, parser.ReadValue(), floatVectorsHolder);
	}
	return status;
}

Error ProtobufDecoder::Decode(std::string_view buf, Payload& pl, WrSerializer& wrser, FloatVectorsHolderVector& floatVectorsHolder) {
	try {
		tagsPath_.clear();
		objectScalarIndexes_.reset();
		CJsonProtobufObjectBuilder cjsonBuilder(arraysStorage_, wrser, &tm_, TagName::Empty());
		ProtobufObject object(buf, *schema_, tagsPath_, tm_);
		return decodeObject(pl, cjsonBuilder, object, floatVectorsHolder);
	} catch (Error& err) {
		return err;
	}
}

}  // namespace reindexer
