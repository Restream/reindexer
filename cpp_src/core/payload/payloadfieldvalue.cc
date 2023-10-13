#include "payloadfieldvalue.h"

namespace reindexer {

void PayloadFieldValue::throwSetTypeMissmatch(const Variant& kv) {
	throw Error(errLogic, "PayloadFieldValue::Set field '%s' type mismatch. passed '%s', expected '%s'\n", t_.Name(), kv.Type().Name(),
				t_.Type().Name());
}

}  // namespace reindexer
