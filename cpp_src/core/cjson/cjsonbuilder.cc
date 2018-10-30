#include "cjsonbuilder.h"

namespace reindexer {

CJsonBuilder::CJsonBuilder(WrSerializer &ser, ObjType type, TagsMatcher *tm, int tagName) : tm_(tm), ser_(&ser), type_(type) {
	switch (type_) {
		case TypeArray:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_ARRAY, tagName)));
			savePos_ = ser_->Len();
			ser_->PutUInt32(0);
			break;
		case TypeObject:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_OBJECT, tagName)));
			break;
		case TypePlain:
			break;
	}
}

CJsonBuilder::~CJsonBuilder() { End(); }
CJsonBuilder &CJsonBuilder::End() {
	switch (type_) {
		case TypeArray:
			*(reinterpret_cast<int *>(ser_->Buf() + savePos_)) = static_cast<int>(carraytag(count_, TAG_OBJECT));
			break;
		case TypeObject:
			ser_->PutVarUint(static_cast<int>(ctag(TAG_END)));
			break;
		case TypePlain:
			break;
	}
	type_ = TypePlain;
	return *this;
}

void CJsonBuilder::SetTagsMatcher(const TagsMatcher *tm) { tm_ = const_cast<TagsMatcher *>(tm); }

CJsonBuilder CJsonBuilder::Object(int tagName) {
	count_++;
	return CJsonBuilder(*ser_, TypeObject, tm_, tagName);
}

CJsonBuilder CJsonBuilder::Array(int tagName) {
	assert(type_ != TypeArray);
	count_++;
	return CJsonBuilder(*ser_, TypeArray, tm_, tagName);
}

inline void CJsonBuilder::putTag(int tagName, int tagType) {
	ser_->PutVarUint(static_cast<int>(ctag(tagType, tagName)));
	count_++;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, bool arg) {
	putTag(tagName, TAG_BOOL);
	ser_->PutBool(arg);
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, int64_t arg) {
	putTag(tagName, TAG_VARINT);
	ser_->PutVarint(arg);
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, int arg) {
	putTag(tagName, TAG_VARINT);
	ser_->PutVarint(arg);
	return *this;
}

CJsonBuilder &CJsonBuilder::Put(int tagName, double arg) {
	putTag(tagName, TAG_DOUBLE);
	ser_->PutDouble(arg);
	return *this;
}
CJsonBuilder &CJsonBuilder::Put(int tagName, const string_view &arg) {
	putTag(tagName, TAG_STRING);
	ser_->PutVString(arg);
	return *this;
}

CJsonBuilder &CJsonBuilder::Null(int tagName) {
	putTag(tagName, TAG_NULL);
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
