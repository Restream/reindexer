#include "protobufdecoder.h"
#include "core/schema.h"
#include "estl/protobufparser.h"
#include "protobufbuilder.h"

namespace reindexer {

ArraysStorage::ArraysStorage(TagsMatcher& tm) : tm_(tm) {}

void ArraysStorage::UpdateArraySize(int tagName, int field) { GetArray(tagName, field); }

CJsonBuilder& ArraysStorage::GetArray(int tagName, int field) {
	assert(indexes_.size() > 0);
	auto it = data_.find(tagName);
	if (it == data_.end()) {
		indexes_.back().push_back(tagName);
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
	assert(indexes_.size() > 0);
	for (int tagName : indexes_.back()) {
		auto it = data_.find(tagName);
		assert(it != data_.end());
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
	: tm_(tagsMatcher), schema_(schema), arraysStorage_(tm_) {}

void ProtobufDecoder::setValue(Payload* pl, CJsonBuilder& builder, const ProtobufValue& item) {
	int field = tm_.tags2field(tagsPath_.data(), tagsPath_.size());
	item.value.convert(item.itemType);
	if (field > 0) {
		pl->Set(field, {item.value}, true);
		if (item.isArray) {
			arraysStorage_.UpdateArraySize(item.tagName, field);
		} else {
			builder.Ref(item.tagName, item.value, field);
		}
	} else {
		if (item.isArray) {
			auto& array = arraysStorage_.GetArray(item.tagName);
			array.Put(0, item.value);
		} else {
			builder.Put(item.tagName, item.value);
		}
	}
}

Error ProtobufDecoder::decodeArray(Payload* pl, CJsonBuilder& builder, const ProtobufValue& item) {
	ProtobufObject object(item.As<string_view>(), *schema_, tagsPath_, tm_);
	ProtobufParser parser(object);
	bool packed = item.IsOfPrimitiveType();
	int field = tm_.tags2field(tagsPath_.data(), tagsPath_.size());
	if (field > 0) {
		if (packed) {
			int count = 0;
			while (!parser.IsEof()) {
				pl->Set(field, {parser.ReadArrayItem(item.itemType)}, true);
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
			if (item.itemType == KeyValueComposite) {
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

Error ProtobufDecoder::decode(Payload* pl, CJsonBuilder& builder, const ProtobufValue& item) {
	TagsPathScope<TagsPath> tagScope(tagsPath_, item.tagName);
	Error status;
	switch (item.value.Type()) {
		case KeyValueInt:
		case KeyValueInt64:
		case KeyValueDouble:
		case KeyValueBool:
			setValue(pl, builder, item);
			break;
		case KeyValueString: {
			if (item.isArray) {
				status = decodeArray(pl, builder, item);
			} else {
				switch (item.itemType) {
					case KeyValueString:
						setValue(pl, builder, item);
						break;
					case KeyValueComposite: {
						CJsonProtobufObjectBuilder objBuilder(builder, item.tagName, arraysStorage_);
						ProtobufObject object(item.As<string_view>(), *schema_, tagsPath_, tm_);
						status = decodeObject(pl, objBuilder, object);
						break;
					}
					default:
						return Error(errParseProtobuf, "Error parsing length-encoded type: [%s] for field [%s]",
									 Variant::TypeName(item.itemType), tm_.tag2name(item.tagName));
				}
			}
			break;
		}
		default:
			return Error(errParseProtobuf, "Unknown field type [%s] while parsing Protobuf", Variant::TypeName(item.value.Type()));
	}
	return status;
}

Error ProtobufDecoder::decodeObject(Payload* pl, CJsonBuilder& builder, ProtobufObject& object) {
	Error status{errOK};
	ProtobufParser parser(object);
	while (status.ok() && !parser.IsEof()) {
		status = decode(pl, builder, parser.ReadValue());
	}
	return status;
}

Error ProtobufDecoder::Decode(string_view buf, Payload* pl, WrSerializer& wrser) {
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
