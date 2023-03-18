#include "cjsonbuilder.h"

namespace reindexer {

CJsonBuilder::CJsonBuilder(WrSerializer &ser, ObjType type, const TagsMatcher *tm, int tagName) : tm_(tm), ser_(&ser), type_(type) {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObjectArray:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
			savePos_ = ser_->Len();
			ser_->PutUInt32(0);
			break;
		case ObjType::TypeObject:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_OBJECT, tagName)));
			break;
		case ObjType::TypePlain:
			break;
	}
}

CJsonBuilder CJsonBuilder::Object(int tagName) {
	++count_;
	return CJsonBuilder(*ser_, ObjType::TypeObject, tm_, tagName);
}

CJsonBuilder CJsonBuilder::Array(int tagName, ObjType type) {
	if ((type_ == ObjType::TypeArray) || (type_ == ObjType::TypeObjectArray)) {
		throw Error(errLogic, "Nested arrays are not supported. Use nested objects with array fields instead");
	}
	++count_;
	return CJsonBuilder(*ser_, type, tm_, tagName);
}

inline void CJsonBuilder::putTag(int tagName, int tagType) { ser_->PutVarUint(static_cast<int>(ctag(tagType, tagName))); }

CJsonBuilder &CJsonBuilder::Put(int tagName, bool arg) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_BOOL;
	} else {
		putTag(tagName, TAG_BOOL);
	}
	ser_->PutBool(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, int64_t arg) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_VARINT;
	} else {
		putTag(tagName, TAG_VARINT);
	}
	ser_->PutVarint(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, int arg) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_VARINT;
	} else {
		putTag(tagName, TAG_VARINT);
	}
	ser_->PutVarint(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, double arg) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_DOUBLE;
	} else {
		putTag(tagName, TAG_DOUBLE);
	}
	ser_->PutDouble(arg);
	++count_;
	return *this;
}
CJsonBuilder &CJsonBuilder::Put(int tagName, std::string_view arg) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_STRING;
	} else {
		putTag(tagName, TAG_STRING);
	}
	ser_->PutVString(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Null(int tagName) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_NULL;
	} else {
		putTag(tagName, TAG_NULL);
	}
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Ref(int tagName, const Variant &v, int field) {
	v.Type().EvaluateOneOf(
		[&](OneOf<KeyValueType::Int, KeyValueType::Int64>) { ser_->PutVarUint(static_cast<int>(ctag(TAG_VARINT, tagName, field))); },
		[&](KeyValueType::Bool) { ser_->PutVarUint(static_cast<int>(ctag(TAG_BOOL, tagName, field))); },
		[&](KeyValueType::Double) { ser_->PutVarUint(static_cast<int>(ctag(TAG_DOUBLE, tagName, field))); },
		[&](KeyValueType::String) { ser_->PutVarUint(static_cast<int>(ctag(TAG_STRING, tagName, field))); },
		[&](OneOf<KeyValueType::Undefined, KeyValueType::Null>) { ser_->PutVarUint(static_cast<int>(ctag(TAG_NULL, tagName))); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Composite>) noexcept { std::abort(); });
	return *this;
}
CJsonBuilder &CJsonBuilder::ArrayRef(int tagName, int field, int count) {
	ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName, field)));
	ser_->PutVarUint(count);
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, const Variant &kv) {
	kv.Type().EvaluateOneOf([&](KeyValueType::Int) { Put(tagName, int(kv)); }, [&](KeyValueType::Int64) { Put(tagName, int64_t(kv)); },
							[&](KeyValueType::Double) { Put(tagName, double(kv)); },
							[&](KeyValueType::String) { Put(tagName, std::string_view(kv)); }, [&](KeyValueType::Null) { Null(tagName); },
							[&](KeyValueType::Bool) { Put(tagName, bool(kv)); },
							[&](KeyValueType::Tuple) {
								auto arrNode = Array(tagName);
								for (auto &val : kv.getCompositeValues()) {
									arrNode.Put(nullptr, val);
								}
							},
							[](OneOf<KeyValueType::Composite, KeyValueType::Undefined>) noexcept {});
	return *this;
}

}  // namespace reindexer
