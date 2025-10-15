#include "payloadfieldvalue.h"

namespace reindexer {

void PayloadFieldValue::throwSetTypeMissmatch(const Variant& kv) {
	throw Error(errLogic, "PayloadFieldValue::Set field '{}' type mismatch. Passed '{}', expected '{}'", t_.Name(), kv.Type().Name(),
				t_.Type().Name());
}

}  // namespace reindexer
