#include "tools/json2kv.h"
#include <limits.h>
#include <cmath>
#include "core/keyvalue/p_string.h"
namespace reindexer {

Variant jsonValue2Variant(JsonValue &v, KeyValueType t, const char *fieldName) {
	switch (v.getTag()) {
		case JSON_NUMBER:
			switch (t) {
				case KeyValueUndefined: {
					double value = v.toNumber(), intpart;
					if (std::modf(value, &intpart) == 0.0) {
						int64_t val = value;
						return val > int64_t(INT_MIN) && val < int64_t(INT_MAX) ? Variant(static_cast<int>(val))
																				: Variant(static_cast<int64_t>(val));
					}
					return Variant(v.toNumber());
				}
				case KeyValueDouble:
					return Variant(v.toNumber());
				case KeyValueInt:
					return Variant(static_cast<int>(v.toNumber()));
				case KeyValueBool:
					return Variant(static_cast<bool>(v.toNumber()));
				case KeyValueInt64:
					return Variant(static_cast<int64_t>(v.toNumber()));
				default:
					throw Error(errLogic, "Error parsing json field '%s' - got number, expected %s", fieldName, Variant::TypeName(t));
			}
		case JSON_STRING:
			return Variant(p_string(v.toString()));
		case JSON_FALSE:
			return Variant(false);
		case JSON_TRUE:
			return Variant(true);
		case JSON_NULL:
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
		case JSON_OBJECT:
			throw Error(errLogic, "Error parsing json field '%s' - got object, expected %s", fieldName, Variant::TypeName(t));
		case JSON_ARRAY: {
			VariantArray variants;
			for (auto elem : v) {
				if (elem->value.getTag() != JSON_NULL) {
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
