#include "cjsonbuilder.h"

namespace reindexer {

CJsonBuilder::CJsonBuilder(WrSerializer &ser, ObjType type, TagsMatcher *tm, int tagName) : tm_(tm), ser_(&ser), type_(type) {
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

CJsonBuilder::~CJsonBuilder() { End(); }
CJsonBuilder &CJsonBuilder::End() {
	switch (type_) {
		case ObjType::TypeArray:
			*(reinterpret_cast<int *>(ser_->Buf() + savePos_)) = static_cast<int>(carraytag(count_, itemType_));
			break;
		case ObjType::TypeObjectArray:
			*(reinterpret_cast<int *>(ser_->Buf() + savePos_)) = static_cast<int>(carraytag(count_, TAG_OBJECT));
			break;
		case ObjType::TypeObject:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_END)));
			break;
		case ObjType::TypePlain:
			break;
	}
	type_ = ObjType::TypePlain;
	return *this;
}

void CJsonBuilder::SetTagsMatcher(const TagsMatcher *tm) { tm_ = const_cast<TagsMatcher *>(tm); }

CJsonBuilder CJsonBuilder::Object(int tagName) {
	count_++;
	return CJsonBuilder(*ser_, ObjType::TypeObject, tm_, tagName);
}

CJsonBuilder CJsonBuilder::Array(int tagName, ObjType type) {
	assert((type_ != ObjType::TypeArray) && (type_ != ObjType::TypeObjectArray));
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
CJsonBuilder &CJsonBuilder::Put(int tagName, const string_view &arg) {
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
	switch (v.Type()) {
		case KeyValueInt:
		case KeyValueInt64:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_VARINT, tagName, field)));
			break;
		case KeyValueBool:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_BOOL, tagName, field)));
			break;
		case KeyValueDouble:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_DOUBLE, tagName, field)));
			break;
		case KeyValueString:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_STRING, tagName, field)));
			break;
		case KeyValueUndefined:
		case KeyValueNull:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_NULL, tagName)));
			break;
		default:
			std::abort();
	}

	return *this;
}
CJsonBuilder &CJsonBuilder::ArrayRef(int tagName, int field, int count) {
	ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName, field)));
	ser_->PutVarUint(count);
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, const Variant &kv) {
	switch (kv.Type()) {
		case KeyValueInt:
			return Put(tagName, int(kv));
		case KeyValueInt64:
			return Put(tagName, int64_t(kv));
		case KeyValueDouble:
			return Put(tagName, double(kv));
		case KeyValueString:
			return Put(tagName, string_view(kv));
		case KeyValueNull:
			return Null(tagName);
		case KeyValueBool:
			return Put(tagName, bool(kv));
		case KeyValueTuple: {
			auto arrNode = Array(tagName);
			for (auto &val : kv.getCompositeValues()) {
				arrNode.Put(nullptr, val);
			}
			return *this;
		}
		default:
			break;
	}
	return *this;
}

}  // namespace reindexer
