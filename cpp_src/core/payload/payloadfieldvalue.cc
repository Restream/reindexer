#include "payloadfieldvalue.h"

namespace reindexer {

void PayloadFieldValue::throwSetTypeMissmatch(const PayloadFieldType& fieldType, const Variant& kv) {
	throw Error(errLogic, "PayloadFieldValue::Set field '{}' type mismatch. Passed '{}', expected '{}'", fieldType.Name(), kv.Type().Name(),
				fieldType.Type().Name());
}

}  // namespace reindexer
