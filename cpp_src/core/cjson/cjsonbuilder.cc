#include "cjsonbuilder.h"
#include "core/type_consts.h"
#include "sparse_validator.h"

namespace reindexer::builders {

using namespace item_fields_validator;

CJsonBuilder::CJsonBuilder(WrSerializer& ser, ObjType type, const TagsMatcher* tm, concepts::TagNameOrIndex auto tag)
	: tm_(tm), ser_(&ser), type_(type) {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObjectArray:
			putTag(tag, TAG_ARRAY);
			savePos_ = ser_->Len();
			ser_->PutCArrayTag(carraytag{0, TAG_NULL});
			break;
		case ObjType::TypeObject:
			putTag(tag, TAG_OBJECT);
			break;
		case ObjType::TypePlain:
			break;
	}
}
template CJsonBuilder::CJsonBuilder(WrSerializer&, ObjType, const TagsMatcher*, TagName);
template CJsonBuilder::CJsonBuilder(WrSerializer&, ObjType, const TagsMatcher*, TagIndex);

CJsonBuilder CJsonBuilder::Object(concepts::TagNameOrIndex auto tag) {
	++count_;
	return CJsonBuilder(*ser_, ObjType::TypeObject, tm_, tag);
}
template CJsonBuilder CJsonBuilder::Object(TagName);
template CJsonBuilder CJsonBuilder::Object(TagIndex);

CJsonBuilder CJsonBuilder::Array(concepts::TagNameOrIndex auto tag, ObjType type) {
	++count_;
	return CJsonBuilder(*ser_, type, tm_, tag);
}
template CJsonBuilder CJsonBuilder::Array(TagName, ObjType);
template CJsonBuilder CJsonBuilder::Array(TagIndex, ObjType);

void CJsonBuilder::Array(concepts::TagNameOrIndex auto tag, std::span<const Uuid> data, int /*offset*/) {
	putTag(tag, TAG_ARRAY);
	ser_->PutCArrayTag(carraytag(data.size(), TAG_UUID));
	for (auto d : data) {
		ser_->PutUuid(d);
	}
	++count_;
}
template void CJsonBuilder::Array(TagName, std::span<const Uuid>, int);
template void CJsonBuilder::Array(TagIndex, std::span<const Uuid>, int);

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, bool arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_BOOL;
	} else {
		putTag(tag, TAG_BOOL);
	}
	ser_->PutBool(arg);
	++count_;
}
template void CJsonBuilder::Put(TagName, bool, int);
template void CJsonBuilder::Put(TagIndex, bool, int);

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, int64_t arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_VARINT;
	} else {
		putTag(tag, TAG_VARINT);
	}
	ser_->PutVarint(arg);
	++count_;
}
template void CJsonBuilder::Put(TagName, int64_t, int);
template void CJsonBuilder::Put(TagIndex, int64_t, int);

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, int arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_VARINT;
	} else {
		putTag(tag, TAG_VARINT);
	}
	ser_->PutVarint(arg);
	++count_;
}
template void CJsonBuilder::Put(TagName, int, int);
template void CJsonBuilder::Put(TagIndex, int, int);

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, double arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_DOUBLE;
	} else {
		putTag(tag, TAG_DOUBLE);
	}
	ser_->PutDouble(arg);
	++count_;
}
template void CJsonBuilder::Put(TagName, double, int);
template void CJsonBuilder::Put(TagIndex, double, int);

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, float arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_FLOAT;
	} else {
		putTag(tag, TAG_FLOAT);
	}
	ser_->PutFloat(arg);
	++count_;
}

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, std::string_view arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_STRING;
	} else {
		putTag(tag, TAG_STRING);
	}
	ser_->PutVString(arg);
	++count_;
}
template void CJsonBuilder::Put(TagName, std::string_view, int);
template void CJsonBuilder::Put(TagIndex, std::string_view, int);

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, Uuid arg, int /*offset*/) {
	putTag(tag, TAG_UUID);
	ser_->PutUuid(arg);
	++count_;
}

void CJsonBuilder::Null(concepts::TagNameOrIndex auto tag) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_NULL;
	} else {
		putTag(tag, TAG_NULL);
	}
	++count_;
}
template void CJsonBuilder::Null(TagName);
template void CJsonBuilder::Null(TagIndex);

void CJsonBuilder::Ref(concepts::TagNameOrIndex auto tag, const KeyValueType& type, int field) {
	type.EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64> auto) { putTag(tag, TAG_VARINT, field); },
		[&](KeyValueType::Bool) { putTag(tag, TAG_BOOL, field); }, [&](KeyValueType::Double) { putTag(tag, TAG_DOUBLE, field); },
		[&](KeyValueType::String) { putTag(tag, TAG_STRING, field); }, [&](KeyValueType::Uuid) { putTag(tag, TAG_UUID, field); },
		[&](KeyValueType::Float) { putTag(tag, TAG_FLOAT, field); },
		[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Null> auto) { putTag(tag, TAG_NULL); },
		[](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::FloatVector> auto) noexcept { std::abort(); });
}
template void CJsonBuilder::Ref(TagName, const KeyValueType&, int);
template void CJsonBuilder::Ref(TagIndex, const KeyValueType&, int);

void CJsonBuilder::ArrayRef(concepts::TagNameOrIndex auto tag, int field, int count) {
	putTag(tag, TAG_ARRAY, field);
	ser_->PutVarUint(count);
}
template void CJsonBuilder::ArrayRef(TagName, int, int);
template void CJsonBuilder::ArrayRef(TagIndex, int, int);

void CJsonBuilder::Put(concepts::TagNameOrIndex auto tag, const Variant& kv, int offset) {
	kv.Type().EvaluateOneOf(
		[&](KeyValueType::Int) { Put(tag, int(kv), offset); }, [&](KeyValueType::Int64) { Put(tag, int64_t(kv), offset); },
		[&](KeyValueType::Double) { Put(tag, double(kv), offset); }, [&](KeyValueType::Float) { Put(tag, float(kv), offset); },
		[&](KeyValueType::String) { Put(tag, std::string_view(kv), offset); }, [&](KeyValueType::Null) { Null(tag); },
		[&](KeyValueType::Bool) { Put(tag, bool(kv), offset); },
		[&](KeyValueType::Tuple) {
			auto arrNode = Array(tag);
			for (auto& val : kv.getCompositeValues()) {
				arrNode.Put(TagName::Empty(), val);
			}
		},
		[&](KeyValueType::Uuid) { Put(tag, Uuid{kv}, offset); },
		[](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::FloatVector> auto) noexcept {
			assertrx_throw(false);
		});
}
template void CJsonBuilder::Put(TagName, const Variant&, int);
template void CJsonBuilder::Put(TagIndex, const Variant&, int);

void CJsonBuilder::Array(concepts::TagNameOrIndex auto tag, Serializer& ser, TagType tagType, int count) {
	putTag(tag, TAG_ARRAY);
	ser_->PutCArrayTag(carraytag(count, tagType));
	while (count--) {
		copyCJsonValue(tagType, ser, *ser_, kNoValidation);
	}
	++count_;
}
template void CJsonBuilder::Array(TagName, Serializer&, TagType, int);
template void CJsonBuilder::Array(TagIndex, Serializer&, TagType, int);

}  // namespace reindexer::builders
