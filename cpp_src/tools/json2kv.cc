#include "tools/json2kv.h"
#include <limits.h>
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "estl/one_of.h"
#include "tools/scope_guard.h"

namespace reindexer {

Variant jsonValue2Variant(const gason::JsonValue& v, KeyValueType t, std::string_view fieldName,
						  FloatVectorsHolderVector* floatVectorsHolder) {
	switch (v.getTag()) {
		case gason::JsonTag::NUMBER:
			return t.EvaluateOneOf(
				[&](KeyValueType::Undefined) noexcept {
					int64_t val = v.toNumber();
					return val > int64_t(INT_MIN) && val < int64_t(INT_MAX) ? Variant(static_cast<int>(val))
																			: Variant(static_cast<int64_t>(val));
				},
				[&](KeyValueType::Double) noexcept { return Variant(double(v.toNumber())); },
				[&](KeyValueType::Float) noexcept { return Variant(float(v.toNumber())); },
				[&](KeyValueType::Int) noexcept { return Variant(static_cast<int>(v.toNumber())); },
				[&](KeyValueType::Bool) noexcept { return Variant(static_cast<bool>(v.toNumber())); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(v.toNumber())); },
				[&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid,
						  KeyValueType::FloatVector>) -> Variant {
					throw Error(errLogic, "Error parsing json field '{}' - got number, expected {}", fieldName, t.Name());
				});
		case gason::JsonTag::DOUBLE:
			return t.EvaluateOneOf([&](OneOf<KeyValueType::Undefined, KeyValueType::Double>) noexcept { return Variant(v.toDouble()); },
								   [&](KeyValueType::Float) noexcept { return Variant(static_cast<float>(v.toDouble())); },
								   [&](KeyValueType::Int) noexcept { return Variant(static_cast<int>(v.toDouble())); },
								   [&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(v.toDouble())); },
								   [&](KeyValueType::Bool) noexcept { return Variant(static_cast<bool>(v.toDouble())); },
								   [&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null,
											 KeyValueType::Uuid, KeyValueType::FloatVector>) -> Variant {
									   throw Error(errLogic, "Error parsing json field '{}' - got number, expected {}", fieldName,
												   t.Name());
								   });
		case gason::JsonTag::STRING:
			return t.EvaluateOneOf(
				[&](OneOf<KeyValueType::String, KeyValueType::Undefined>) {
					return Variant(p_string(json_string_ftr{v.sval.ptr}), Variant::noHold);
				},
				[&](KeyValueType::Uuid) { return Variant{Uuid{v.toString()}}; },
				[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
						  KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::FloatVector>) -> Variant {
					throw Error(errLogic, "Error parsing json field '{}' - got string, expected {}", fieldName, t.Name());
				});
		case gason::JsonTag::JFALSE:
			return t.EvaluateOneOf([&](OneOf<KeyValueType::Undefined, KeyValueType::Bool>) noexcept { return Variant(false); },
								   [&](KeyValueType::Int) noexcept { return Variant(0); },
								   [&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(0)); },
								   [&](KeyValueType::Double) noexcept { return Variant(0.0); },
								   [&](KeyValueType::Float) noexcept { return Variant(0.0f); },
								   [&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null,
											 KeyValueType::Uuid, KeyValueType::FloatVector>) -> Variant {
									   throw Error(errLogic, "Error parsing json field '{}' - got bool, expected {}", fieldName, t.Name());
								   });
		case gason::JsonTag::JTRUE:
			return t.EvaluateOneOf([&](OneOf<KeyValueType::Undefined, KeyValueType::Bool>) noexcept { return Variant(true); },
								   [&](KeyValueType::Int) noexcept { return Variant(1); },
								   [&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(1)); },
								   [&](KeyValueType::Double) noexcept { return Variant(1.0); },
								   [&](KeyValueType::Float) noexcept { return Variant(1.0f); },
								   [&](OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null,
											 KeyValueType::Uuid, KeyValueType::FloatVector>) -> Variant {
									   throw Error(errLogic, "Error parsing json field '{}' - got bool, expected {}", fieldName, t.Name());
								   });
		case gason::JsonTag::JSON_NULL:
			return t.EvaluateOneOf(
				[](KeyValueType::Double) noexcept { return Variant(0.0); }, [](KeyValueType::Float) noexcept { return Variant(0.0f); },
				[](KeyValueType::Bool) noexcept { return Variant(false); }, [](KeyValueType::Int) noexcept { return Variant(0); },
				[](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(0)); },
				[](KeyValueType::String) { return Variant(static_cast<const char*>(nullptr)); },
				[](KeyValueType::Uuid) noexcept { return Variant{Uuid{}}; },
				[&](OneOf<KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null,
						  KeyValueType::FloatVector>) -> Variant {
					throw Error(errLogic, "Error parsing json field '{}' - got null, expected {}", fieldName, t.Name());
				});
		case gason::JsonTag::OBJECT:
			throw Error(errLogic, "Error parsing json field '{}' - unable to use object in this context", fieldName);
		case gason::JsonTag::ARRAY:
			if (t.Is<KeyValueType::FloatVector>()) {
				thread_local static std::vector<float> vect;
				vect.resize(0);
				vect.reserve(kMaxThreadLocalJSONVector);
				auto guard = MakeScopeGuard([]() noexcept {
					if (vect.capacity() > kMaxThreadLocalJSONVector) {
						vect = std::vector<float>();
					}
				});
				for (const auto& elem : v) {
					switch (elem.value.getTag()) {
						case gason::JsonTag::NUMBER:
							vect.emplace_back(elem.value.toNumber());
							break;
						case gason::JsonTag::DOUBLE:
							vect.emplace_back(elem.value.toDouble());
							break;
						case gason::JsonTag::OBJECT:
						case gason::JsonTag::ARRAY:
						case gason::JsonTag::JSON_NULL:
						case gason::JsonTag::JTRUE:
						case gason::JsonTag::JFALSE:
						case gason::JsonTag::STRING:
						case gason::JsonTag::EMPTY:
							throw Error(errLogic, "Error parsing json field '{}' - got value of unexpected type in array: {}", fieldName,
										JsonTagToTypeStr(elem.value.getTag()));
					}
				}
				if (vect.empty()) {
					return Variant{ConstFloatVectorView{}};
				} else if (floatVectorsHolder) {
					floatVectorsHolder->Add(ConstFloatVectorView{vect});
					return Variant{floatVectorsHolder->Back()};
				} else {
					return Variant{ConstFloatVectorView{std::span<const float>{vect}}, Variant::hold};
				}
			} else {
				VariantArray variants;
				for (const auto& elem : v) {
					if (elem.value.getTag() != gason::JsonTag::JSON_NULL) {
						variants.emplace_back(jsonValue2Variant(elem.value, KeyValueType::Undefined{}, fieldName, floatVectorsHolder));
					}
				}
				return Variant(variants);
			}
		case gason::JsonTag::EMPTY:
		default:
			throw Error(errLogic, "Error parsing json field '{}' - got value of unexpected type: {}", fieldName,
						JsonTagToTypeStr(v.getTag()));
	}
}

}  // namespace reindexer
