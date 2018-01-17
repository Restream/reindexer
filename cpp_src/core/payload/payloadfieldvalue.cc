#include "payloadfieldvalue.h"

namespace reindexer {

void PayloadFieldValue::Set(KeyRef kv) {
	if (kv.Type() != t_.Type())
		throw Error(errLogic, "PayloadFieldValue::Set field '%s' type mimatch. passed '%s', expected '%s'\n", t_.Name().c_str(),
					KeyRef::TypeName(kv.Type()), KeyRef::TypeName(t_.Type()));

	switch (t_.Type()) {
		case KeyValueInt:
			*reinterpret_cast<int *>(p_) = int(kv);
			break;
		case KeyValueInt64:
			*reinterpret_cast<int64_t *>(p_) = int64_t(kv);
			break;
		case KeyValueDouble:
			*reinterpret_cast<double *>(p_) = double(kv);
			break;
		case KeyValueString:
			*reinterpret_cast<p_string *>(p_) = p_string(kv);
			break;
		default:
			abort();
	}
}

KeyRef PayloadFieldValue::Get() const {
	switch (t_.Type()) {
		case KeyValueInt:
			return KeyRef(*reinterpret_cast<const int *>(p_));
		case KeyValueInt64:
			return KeyRef(*reinterpret_cast<const int64_t *>(p_));
		case KeyValueDouble:
			return KeyRef(*reinterpret_cast<const double *>(p_));
		case KeyValueString:
			return KeyRef(*reinterpret_cast<const p_string *>(p_));
		default:
			abort();
	}
}

}  // namespace reindexer
