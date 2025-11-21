#include "json2kv.h"
#include <limits.h>
#include "core/cjson/cjsontools.h"
#include "core/keyvalue/float_vectors_holder.h"
#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "tools/float_comparison.h"
#include "tools/scope_guard.h"

namespace reindexer {

Variant jsonValue2Variant(const gason::JsonValue& v, KeyValueType t, std::string_view fieldName,
						  FloatVectorsHolderVector* floatVectorsHolder, ConvertToString convertToString, ConvertNull convertNull) {
	using namespace std::string_view_literals;
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
				[&](KeyValueType::String) {
					if (convertToString) {
						return Variant(v.toNumber()).convert(KeyValueType::String{});
					} else {
						throwUnexpected(fieldName, t, "number"sv, kJSONFmt);
					}
				},
				[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid,
									KeyValueType::FloatVector> auto) -> Variant { throwUnexpected(fieldName, t, "number"sv, kJSONFmt); });
		case gason::JsonTag::DOUBLE:
			return t.EvaluateOneOf(
				[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Double> auto) noexcept { return Variant(v.toDouble()); },
				[&](KeyValueType::Float) noexcept { return Variant(static_cast<float>(v.toDouble())); },
				[&](KeyValueType::Int) noexcept { return Variant(static_cast<int>(v.toDouble())); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(v.toDouble())); },
				[&](KeyValueType::Bool) noexcept { return Variant(!fp::IsZero(v.toDouble())); },
				[&](KeyValueType::String) {
					if (convertToString) {
						return Variant(v.toDouble()).convert(KeyValueType::String{});
					} else {
						throwUnexpected(fieldName, t, "number"sv, kJSONFmt);
					}
				},
				[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid,
									KeyValueType::FloatVector> auto) -> Variant { throwUnexpected(fieldName, t, "number"sv, kJSONFmt); });
		case gason::JsonTag::STRING:
			return t.EvaluateOneOf(
				[&](concepts::OneOf<KeyValueType::String, KeyValueType::Undefined> auto) {
					return Variant(p_string(json_string_ftr{v.sval.ptr}), Variant::noHold);
				},
				[&](KeyValueType::Uuid) { return Variant{Uuid{v.toString()}}; },
				[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
									KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::FloatVector> auto)
					-> Variant { throwUnexpected(fieldName, t, "string"sv, kJSONFmt); });
		case gason::JsonTag::JFALSE:
			return t.EvaluateOneOf(
				[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Bool> auto) noexcept { return Variant(false); },
				[&](KeyValueType::Int) noexcept { return Variant(0); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(0)); },
				[&](KeyValueType::Double) noexcept { return Variant(0.0); }, [&](KeyValueType::Float) noexcept { return Variant(0.0f); },
				[&](KeyValueType::String) {
					if (convertToString) {
						return Variant(false).convert(KeyValueType::String{});
					} else {
						throwUnexpected(fieldName, t, "bool"sv, kJSONFmt);
					}
				},
				[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid,
									KeyValueType::FloatVector> auto) -> Variant { throwUnexpected(fieldName, t, "bool"sv, kJSONFmt); });
		case gason::JsonTag::JTRUE:
			return t.EvaluateOneOf(
				[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Bool> auto) noexcept { return Variant(true); },
				[&](KeyValueType::Int) noexcept { return Variant(1); },
				[&](KeyValueType::Int64) noexcept { return Variant(static_cast<int64_t>(1)); },
				[&](KeyValueType::Double) noexcept { return Variant(1.0); }, [&](KeyValueType::Float) noexcept { return Variant(1.0f); },
				[&](KeyValueType::String) {
					if (convertToString) {
						return Variant(true).convert(KeyValueType::String{});
					} else {
						throwUnexpected(fieldName, t, "bool"sv, kJSONFmt);
					}
				},
				[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Uuid,
									KeyValueType::FloatVector> auto) -> Variant { throwUnexpected(fieldName, t, "bool"sv, kJSONFmt); });
		case gason::JsonTag::JSON_NULL:
			return convertNullToIndexField(t, fieldName, kJSONFmt, convertNull);
		case gason::JsonTag::OBJECT:
			throwUnexpectedObjectInIndex(fieldName, kJSONFmt);
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
					[[maybe_unused]] bool added = floatVectorsHolder->Add(ConstFloatVectorView{vect});
					assertrx_dbg(added);
					return Variant{floatVectorsHolder->Back()};
				} else {
					return Variant{ConstFloatVectorView{std::span<const float>{vect}}, Variant::hold};
				}
			} else {
				VariantArray variants;
				for (const auto& elem : v) {
					if (elem.value.getTag() != gason::JsonTag::JSON_NULL) {
						variants.emplace_back(jsonValue2Variant(elem.value, KeyValueType::Undefined{}, fieldName, floatVectorsHolder,
																convertToString, convertNull));
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
