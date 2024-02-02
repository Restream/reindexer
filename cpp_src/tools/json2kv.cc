#include "tools/json2kv.h"
#include <limits.h>
#include <cmath>
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "estl/one_of.h"

namespace reindexer {

Variant jsonValue2Variant(const gason::JsonValue &v, KeyValueType t, std::string_view fieldName) {
	switch (v.getTag()) {
		case gason::JSON_NUMBER:
			return t.EvaluateOneOf(
				[&](KeyValueType::Undefined) noexcept {
					int64_t val = int64_t(v.toNumber());
					return val > int64_t(INT_MIN) && val < int64_t(INT_MAX) ? Variant(static_cast<int>(val))
																			: Variant(static_cast<int64_t>(val));
				},
				[&](KeyValueType::Double) noexcept { return Variant(double(v.toNumber())); },
				[&](KeyValueType::Int) noexcept { return Variant(static_cast<int>(v.toNumber())); },
				[&](KeyValueType::Bool) noexcept { return Variant(static_cast<bool>(v.toNumber())); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(v.toNumber())); },
				[&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid>)
					-> Variant { throw Error(errLogic, "Error parsing json field '%s' - got number, expected %s", fieldName, t.Name()); });
		case gason::JSON_DOUBLE:
			return t.EvaluateOneOf(
				[&](OneOf<KeyValueType::Undefined, KeyValueType::Double>) noexcept { return Variant(v.toDouble()); },
				[&](KeyValueType::Int) noexcept { return Variant(static_cast<int>(v.toDouble())); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(v.toDouble())); },
				[&](KeyValueType::Bool) noexcept { return Variant(static_cast<bool>(v.toDouble())); },
				[&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid>)
					-> Variant { throw Error(errLogic, "Error parsing json field '%s' - got number, expected %s", fieldName, t.Name()); });
		case gason::JSON_STRING:
			return t.EvaluateOneOf(
				[&](OneOf<KeyValueType::String, KeyValueType::Undefined>) {
					return Variant(p_string(json_string_ftr{v.sval.ptr}), Variant::no_hold_t{});
				},
				[&](KeyValueType::Uuid) { return Variant{Uuid{v.toString()}}; },
				[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Tuple,
						  KeyValueType::Composite, KeyValueType::Null>) -> Variant {
					throw Error(errLogic, "Error parsing json field '%s' - got string, expected %s", fieldName, t.Name());
				});
		case gason::JSON_FALSE:
			return t.EvaluateOneOf(
				[&](OneOf<KeyValueType::Undefined, KeyValueType::Bool>) noexcept { return Variant(false); },
				[&](KeyValueType::Int) noexcept { return Variant(0); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(0)); },
				[&](KeyValueType::Double) noexcept { return Variant(0.0); },
				[&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid>)
					-> Variant { throw Error(errLogic, "Error parsing json field '%s' - got bool, expected %s", fieldName, t.Name()); });
		case gason::JSON_TRUE:
			return t.EvaluateOneOf(
				[&](OneOf<KeyValueType::Undefined, KeyValueType::Bool>) noexcept { return Variant(true); },
				[&](KeyValueType::Int) noexcept { return Variant(1); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(1)); },
				[&](KeyValueType::Double) noexcept { return Variant(1.0); },
				[&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid>)
					-> Variant { throw Error(errLogic, "Error parsing json field '%s' - got bool, expected %s", fieldName, t.Name()); });
		case gason::JSON_NULL:
			return t.EvaluateOneOf(
				[](KeyValueType::Double) noexcept { return Variant(0.0); }, [](KeyValueType::Bool) noexcept { return Variant(false); },
				[](KeyValueType::Int) noexcept { return Variant(0); },
				[](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(0)); },
				[](KeyValueType::String) { return Variant(static_cast<const char *>(nullptr)); },
				[](KeyValueType::Uuid) noexcept { return Variant{Uuid{}}; },
				[&](OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null>) -> Variant {
					throw Error(errLogic, "Error parsing json field '%s' - got null, expected %s", fieldName, t.Name());
				});
		case gason::JSON_OBJECT:
			throw Error(errLogic, "Error parsing json field '%s' - got object, expected %s", fieldName, t.Name());
		case gason::JSON_ARRAY: {
			VariantArray variants;
			for (const auto &elem : v) {
				if (elem.value.getTag() != gason::JSON_NULL) {
					variants.emplace_back(jsonValue2Variant(elem.value, KeyValueType::Undefined{}));
				}
			}
			return Variant(variants);
		}
	}
	return Variant();
}

}  // namespace reindexer
