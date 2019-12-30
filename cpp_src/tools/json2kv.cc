#include "tools/json2kv.h"
#include <limits.h>
#include <cmath>
#include "core/keyvalue/p_string.h"

namespace reindexer {

Variant jsonValue2Variant(gason::JsonValue &v, KeyValueType t, string_view fieldName) {
	switch (v.getTag()) {
		case gason::JSON_NUMBER:
			switch (t) {
				case KeyValueUndefined: {
					int64_t val = int64_t(v.toNumber());
					return val > int64_t(INT_MIN) && val < int64_t(INT_MAX) ? Variant(static_cast<int>(val))
																			: Variant(static_cast<int64_t>(val));
				}
				case KeyValueDouble:
					return Variant(double(v.toNumber()));
				case KeyValueInt:
					return Variant(static_cast<int>(v.toNumber()));
				case KeyValueBool:
					return Variant(static_cast<bool>(v.toNumber()));
				case KeyValueInt64:
					return Variant(static_cast<int64_t>(v.toNumber()));
				default:
					throw Error(errLogic, "Error parsing json field '%s' - got number, expected %s", fieldName, Variant::TypeName(t));
			}
		case gason::JSON_DOUBLE:
			switch (t) {
				case KeyValueUndefined:
					return Variant(int64_t(v.toDouble()));
				case KeyValueDouble:
					return Variant(v.toDouble());
				default:
					throw Error(errLogic, "Error parsing json field '%s' - got number, expected %s", fieldName, Variant::TypeName(t));
			}
		case gason::JSON_STRING:
			return Variant(p_string(json_string_ftr{v.sval.ptr}));
		case gason::JSON_FALSE:
			return Variant(false);
		case gason::JSON_TRUE:
			return Variant(true);
		case gason::JSON_NULL:
			switch (t) {
				case KeyValueDouble:
					return Variant(static_cast<double>(0));
				case KeyValueBool:
					return Variant(static_cast<bool>(0));
				case KeyValueInt:
					return Variant(static_cast<int>(0));
				case KeyValueInt64:
					return Variant(static_cast<int64_t>(0));
				case KeyValueString:
					return Variant(p_string(static_cast<const char *>(nullptr)));
				default:
					throw Error(errLogic, "Error parsing json field '%s' - got null, expected %s", fieldName, Variant::TypeName(t));
			}
		case gason::JSON_OBJECT:
			throw Error(errLogic, "Error parsing json field '%s' - got object, expected %s", fieldName, Variant::TypeName(t));
		case gason::JSON_ARRAY: {
			VariantArray variants;
			for (auto elem : v) {
				if (elem->value.getTag() != gason::JSON_NULL) {
					variants.push_back(jsonValue2Variant(elem->value, KeyValueUndefined));
				}
			}
			return Variant(variants);
		}
		default:
			abort();
	}
	return Variant();
}

}  // namespace reindexer
