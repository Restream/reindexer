#include "protobufdecoder.h"
#include "core/schema.h"
#include "core/type_consts_helpers.h"
#include "estl/protobufparser.h"
#include "protobufbuilder.h"

namespace reindexer {

ArraysStorage::ArraysStorage(TagsMatcher& tm) : tm_(tm) {}

void ArraysStorage::UpdateArraySize(int tagName, int field) { GetArray(tagName, field); }

CJsonBuilder& ArraysStorage::GetArray(int tagName, int field) {
	assertrx(indexes_.size() > 0);
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
	assertrx(indexes_.size() > 0);
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

ProtobufDecoder::ProtobufDecoder(TagsMatcher& tagsMatcher, std::shared_ptr<const Schema> schema)
	: tm_(tagsMatcher), schema_(std::move(schema)), arraysStorage_(tm_) {}

void ProtobufDecoder::setValue(Payload& pl, CJsonBuilder& builder, ProtobufValue item) {
	int field = tm_.tags2field(tagsPath_.data(), tagsPath_.size());
	auto value = item.value.convert(item.itemType);
	if (field > 0) {
		pl.Set(field, value, true);
		if (item.isArray) {
			arraysStorage_.UpdateArraySize(item.tagName, field);
		} else {
			builder.Ref(item.tagName, value, field);
		}
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
	bool packed = item.IsOfPrimitiveType();
	int field = tm_.tags2field(tagsPath_.data(), tagsPath_.size());
	if (field > 0) {
		if (packed) {
			int count = 0;
			while (!parser.IsEof()) {
				pl.Set(field, parser.ReadArrayItem(item.itemType), true);
				++count;
			}
			builder.ArrayRef(item.tagName, field, count);
		} else {
			setValue(pl, builder, item);
		}
	} else {
		CJsonBuilder& array = arraysStorage_.GetArray(item.tagName);
		if (packed) {
			while (!parser.IsEof()) {
				array.Put(0, parser.ReadArrayItem(item.itemType));
			}
		} else {
			if (item.itemType.Is<KeyValueType::Composite>()) {
				Error status{errOK};
				CJsonProtobufObjectBuilder obj(array, 0, arraysStorage_);
				while (status.ok() && !parser.IsEof()) {
					status = decode(pl, obj, parser.ReadValue());
				}
			} else {
				setValue(pl, array, item);
			}
		}
	}
	return errOK;
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
	Error status{errOK};
	ProtobufParser parser(object);
	while (status.ok() && !parser.IsEof()) {
		status = decode(pl, builder, parser.ReadValue());
	}
	return status;
}

Error ProtobufDecoder::Decode(std::string_view buf, Payload& pl, WrSerializer& wrser) {
	try {
		tagsPath_.clear();
		CJsonProtobufObjectBuilder cjsonBuilder(arraysStorage_, wrser, &tm_, 0);
		ProtobufObject object(buf, *schema_, tagsPath_, tm_);
		return decodeObject(pl, cjsonBuilder, object);
	} catch (Error& err) {
		return err;
	}
}

}  // namespace reindexer
