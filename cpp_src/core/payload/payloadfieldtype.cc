#include "payloadfieldtype.h"
#include "core/keyvalue/p_string.h"
#include "payloadfieldvalue.h"

namespace reindexer {

size_t PayloadFieldType::Sizeof() const {
	if (IsArray()) return sizeof(PayloadFieldValue::Array);
	return ElemSizeof();
}

size_t PayloadFieldType::ElemSizeof() const {
	switch (Type()) {
		case KeyValueBool:
			return sizeof(bool);
		case KeyValueInt:
			return sizeof(int);
		case KeyValueInt64:
			return sizeof(int64_t);
		case KeyValueDouble:
			return sizeof(double);
		case KeyValueString:
			return sizeof(p_string);
		default:
			assert(0);
	}
	return 0;
}

size_t PayloadFieldType::Alignof() const {
	if (IsArray()) return alignof(PayloadFieldValue::Array);
	switch (Type()) {
		case KeyValueInt:
			return alignof(int);
		case KeyValueBool:
			return alignof(bool);
		case KeyValueInt64:
			return alignof(int64_t);
		case KeyValueDouble:
			return alignof(double);
		case KeyValueString:
			return alignof(p_string);
		default:
			assert(0);
	}
	return 0;
}

}  // namespace reindexer
