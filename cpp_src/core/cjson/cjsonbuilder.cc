#include "cjsonbuilder.h"

namespace reindexer {

CJsonBuilder::CJsonBuilder(WrSerializer &ser, ObjType type, const TagsMatcher *tm, int tagName) : tm_(tm), ser_(&ser), type_(type) {
	switch (type_) {
		case ObjType::TypeArray:
		case ObjType::TypeObjectArray:
			ser_->PutCTag(ctag{TAG_ARRAY, tagName});
			savePos_ = ser_->Len();
			ser_->PutCArrayTag(carraytag{0, TAG_NULL});
			break;
		case ObjType::TypeObject:
			ser_->PutCTag(ctag{TAG_OBJECT, tagName});
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

void CJsonBuilder::Array(int tagName, span<Uuid> data, int /*offset*/) {
	ser_->PutCTag(ctag{TAG_ARRAY, tagName});
	ser_->PutCArrayTag(carraytag(data.size(), TAG_UUID));
	for (auto d : data) {
		ser_->PutUuid(d);
	}
}

CJsonBuilder &CJsonBuilder::Put(int tagName, bool arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_BOOL;
	} else {
		putTag(tagName, TAG_BOOL);
	}
	ser_->PutBool(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, int64_t arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_VARINT;
	} else {
		putTag(tagName, TAG_VARINT);
	}
	ser_->PutVarint(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, int arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_VARINT;
	} else {
		putTag(tagName, TAG_VARINT);
	}
	ser_->PutVarint(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, double arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_DOUBLE;
	} else {
		putTag(tagName, TAG_DOUBLE);
	}
	ser_->PutDouble(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, std::string_view arg, int /*offset*/) {
	if (type_ == ObjType::TypeArray) {
		itemType_ = TAG_STRING;
	} else {
		putTag(tagName, TAG_STRING);
	}
	ser_->PutVString(arg);
	++count_;
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, Uuid arg, int /*offset*/) {
	ser_->PutCTag(ctag{TAG_UUID, tagName});
	ser_->PutUuid(arg);
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
		[&](OneOf<KeyValueType::Int, KeyValueType::Int64>) {
			ser_->PutCTag(ctag{TAG_VARINT, tagName, field});
		},
		[&](KeyValueType::Bool) {
			ser_->PutCTag(ctag{TAG_BOOL, tagName, field});
		},
		[&](KeyValueType::Double) {
			ser_->PutCTag(ctag{TAG_DOUBLE, tagName, field});
		},
		[&](KeyValueType::String) {
			ser_->PutCTag(ctag{TAG_STRING, tagName, field});
		},
		[&](KeyValueType::Uuid) {
			ser_->PutCTag(ctag{TAG_UUID, tagName, field});
		},
		[&](OneOf<KeyValueType::Undefined, KeyValueType::Null>) {
			ser_->PutCTag(ctag{TAG_NULL, tagName});
		},
		[](OneOf<KeyValueType::Tuple, KeyValueType::Composite>) noexcept { std::abort(); });
	return *this;
}

CJsonBuilder &CJsonBuilder::ArrayRef(int tagName, int field, int count) {
	ser_->PutCTag(ctag{TAG_ARRAY, tagName, field});
	ser_->PutVarUint(count);
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, const Variant &kv, int offset) {
	kv.Type().EvaluateOneOf([&](KeyValueType::Int) { Put(tagName, int(kv), offset); },
							[&](KeyValueType::Int64) { Put(tagName, int64_t(kv), offset); },
							[&](KeyValueType::Double) { Put(tagName, double(kv), offset); },
							[&](KeyValueType::String) { Put(tagName, std::string_view(kv), offset); },
							[&](KeyValueType::Null) { Null(tagName); }, [&](KeyValueType::Bool) { Put(tagName, bool(kv), offset); },
							[&](KeyValueType::Tuple) {
								auto arrNode = Array(tagName);
								for (auto &val : kv.getCompositeValues()) {
									arrNode.Put(nullptr, val);
								}
							},
							[&](KeyValueType::Uuid) { Put(tagName, Uuid{kv}, offset); },
							[](OneOf<KeyValueType::Composite, KeyValueType::Undefined>) noexcept {});
	return *this;
}

}  // namespace reindexer
