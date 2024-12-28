#include "protobufdecoder.h"
#include "core/cjson/cjsontools.h"
#include "core/schema.h"
#include "estl/protobufparser.h"

namespace reindexer {

void ArraysStorage::UpdateArraySize(int tagName, int field) { GetArray(tagName, field); }

CJsonBuilder& ArraysStorage::GetArray(int tagName, int field) {
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

void ArraysStorage::onAddObject() { indexes_.emplace_back(h_vector<int, 1>()); }

void ArraysStorage::onObjectBuilt(CJsonBuilder& parent) {
	assertrx(!indexes_.empty());
	for (int tagName : indexes_.back()) {
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
	int field = tm_.tags2field(tagsPath_.data(), tagsPath_.size());
	auto value = item.value.convert(item.itemType);
	if (field > 0) {
		const auto& f = pl.Type().Field(field);
		if rx_unlikely (!f.IsArray() && objectScalarIndexes_.test(field)) {
			throw Error(errLogic, "Non-array field '%s' [%d] from '%s' can only be encoded once.", f.Name(), field, pl.Type().Name());
		}
		if (item.isArray) {
			arraysStorage_.UpdateArraySize(item.tagName, field);
		} else {
			validateArrayFieldRestrictions(f, 1, "protobuf");
			builder.Ref(item.tagName, value, field);
		}
		pl.Set(field, convertValueForPayload(pl, field, std::move(value), "protobuf"), true);
		objectScalarIndexes_.set(field);
	} else {
		if (item.isArray) {
			auto& array = arraysStorage_.GetArray(item.tagName);
			array.Put(0, value);
		} else {
			builder.Put(item.tagName, value);
		}
	}
}

Error ProtobufDecoder::decodeArray(Payload& pl, CJsonBuilder& builder, const ProtobufValue& item) {
	ProtobufObject object(item.As<std::string_view>(), *schema_, tagsPath_, tm_);
	ProtobufParser parser(object);
	const bool packed = item.IsOfPrimitiveType();
	const int field = tm_.tags2field(tagsPath_.data(), tagsPath_.size());
	if (field > 0) {
		const auto& f = pl.Type().Field(field);
		if rx_unlikely (!f.IsArray()) {
			throw Error(errLogic, "Error parsing protobuf field '%s' - got array, expected scalar %s", f.Name(), f.Type().Name());
		}
		if (packed) {
			int count = 0;
			while (!parser.IsEof()) {
				pl.Set(field, convertValueForPayload(pl, field, parser.ReadArrayItem(item.itemType), "protobuf"), true);
				++count;
			}
			builder.ArrayRef(item.tagName, field, count);
		} else {
			setValue(pl, builder, item);
		}
		validateArrayFieldRestrictions(f, reinterpret_cast<PayloadFieldValue::Array*>(pl.Field(field).p_)->len, "protobuf");
	} else {
		CJsonBuilder& array = arraysStorage_.GetArray(item.tagName);
		if (packed) {
			while (!parser.IsEof()) {
				array.Put(0, parser.ReadArrayItem(item.itemType));
			}
		} else {
			if (item.itemType.Is<KeyValueType::Composite>()) {
				Error status;
				CJsonProtobufObjectBuilder obj(array, 0, arraysStorage_);
				while (status.ok() && !parser.IsEof()) {
					status = decode(pl, obj, parser.ReadValue());
				}
			} else {
				setValue(pl, array, item);
			}
		}
	}
	return {};
}

Error ProtobufDecoder::decode(Payload& pl, CJsonBuilder& builder, const ProtobufValue& item) {
	TagsPathScope<TagsPath> tagScope(tagsPath_, item.tagName);
	return item.value.Type().EvaluateOneOf(
		[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Bool>) {
			setValue(pl, builder, item);
			return Error{};
		},
		[&](KeyValueType::String) {
			if (item.isArray) {
				return decodeArray(pl, builder, item);
			} else {
				return item.itemType.EvaluateOneOf(
					[&](KeyValueType::String) {
						setValue(pl, builder, item);
						return Error{};
					},
					[&](KeyValueType::Composite) {
						CJsonProtobufObjectBuilder objBuilder(builder, item.tagName, arraysStorage_);
						ProtobufObject object(item.As<std::string_view>(), *schema_, tagsPath_, tm_);
						return decodeObject(pl, objBuilder, object);
					},
					[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double, KeyValueType::Null,
							  KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Uuid>) {
						return Error(errParseProtobuf, "Error parsing length-encoded type: [%s] for field [%s]", item.itemType.Name(),
									 tm_.tag2name(item.tagName));
					});
			}
		},
		[&](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::Uuid>) {
			return Error(errParseProtobuf, "Unknown field type [%s] while parsing Protobuf", item.value.Type().Name());
		});
}

Error ProtobufDecoder::decodeObject(Payload& pl, CJsonBuilder& builder, ProtobufObject& object) {
	Error status;
	ProtobufParser parser(object);
	while (status.ok() && !parser.IsEof()) {
		status = decode(pl, builder, parser.ReadValue());
	}
	return status;
}

Error ProtobufDecoder::Decode(std::string_view buf, Payload& pl, WrSerializer& wrser) {
	try {
		tagsPath_.clear();
		objectScalarIndexes_.reset();
		CJsonProtobufObjectBuilder cjsonBuilder(arraysStorage_, wrser, &tm_, 0);
		ProtobufObject object(buf, *schema_, tagsPath_, tm_);
		return decodeObject(pl, cjsonBuilder, object);
	} catch (Error& err) {
		return err;
	}
}

}  // namespace reindexer
