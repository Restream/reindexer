#include "payloadfieldvalue.h"
#include "core/keyvalue/p_string.h"

namespace reindexer {

void PayloadFieldValue::Set(Variant kv) {
	if (kv.Type() == KeyValueInt && t_.Type() == KeyValueInt64) kv.convert(KeyValueInt64);
	if (kv.Type() == KeyValueInt64 && t_.Type() == KeyValueInt) kv.convert(KeyValueInt);

	if (kv.Type() != t_.Type())
		throw Error(errLogic, "PayloadFieldValue::Set field '%s' type mimatch. passed '%s', expected '%s'\n", t_.Name().c_str(),
					Variant::TypeName(kv.Type()), Variant::TypeName(t_.Type()));

	switch (t_.Type()) {
		case KeyValueInt:
			*reinterpret_cast<int *>(p_) = int(kv);
			break;
		case KeyValueBool:
			*reinterpret_cast<bool *>(p_) = bool(kv);
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

Variant PayloadFieldValue::Get(bool enableHold) const {
	switch (t_.Type()) {
		case KeyValueBool:
			return Variant(*reinterpret_cast<const bool *>(p_));
		case KeyValueInt:
			return Variant(*reinterpret_cast<const int *>(p_));
		case KeyValueInt64:
			return Variant(*reinterpret_cast<const int64_t *>(p_));
		case KeyValueDouble:
			return Variant(*reinterpret_cast<const double *>(p_));
		case KeyValueString:
			return Variant(*reinterpret_cast<const p_string *>(p_), enableHold);
		default:
			abort();
	}
}

}  // namespace reindexer
