#include "payloadfieldvalue.h"
#include "core/keyvalue/p_string.h"
#include "estl/one_of.h"
#include "tools/stringstools.h"

namespace reindexer {

void PayloadFieldValue::Set(Variant kv) {
	if (kv.Type().Is<KeyValueType::Int>() && t_.Type().Is<KeyValueType::Int64>()) kv.convert(KeyValueType::Int64{});
	if (kv.Type().Is<KeyValueType::Int64>() && t_.Type().Is<KeyValueType::Int>()) kv.convert(KeyValueType::Int{});

	if (!kv.Type().IsSame(t_.Type())) {
		throw Error(errLogic, "PayloadFieldValue::Set field '%s' type mismatch. passed '%s', expected '%s'\n", t_.Name(), kv.Type().Name(),
					t_.Type().Name());
	}

	t_.Type().EvaluateOneOf([&](KeyValueType::Int) noexcept { *reinterpret_cast<int *>(p_) = int(kv); },
							[&](KeyValueType::Bool) noexcept { *reinterpret_cast<bool *>(p_) = bool(kv); },
							[&](KeyValueType::Int64) noexcept { *reinterpret_cast<int64_t *>(p_) = int64_t(kv); },
							[&](KeyValueType::Double) noexcept { *reinterpret_cast<double *>(p_) = double(kv); },
							[&](KeyValueType::String) noexcept { *reinterpret_cast<p_string *>(p_) = p_string(kv); },
							[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) noexcept {
								assertrx(0);
								abort();
							});
}

Variant PayloadFieldValue::Get(bool enableHold) const {
	return t_.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept { return Variant(*reinterpret_cast<const bool *>(p_)); },
		[&](KeyValueType::Int) noexcept { return Variant(*reinterpret_cast<const int *>(p_)); },
		[&](KeyValueType::Int64) noexcept { return Variant(*reinterpret_cast<const int64_t *>(p_)); },
		[&](KeyValueType::Double) noexcept { return Variant(*reinterpret_cast<const double *>(p_)); },
		[&](KeyValueType::String) { return Variant(*reinterpret_cast<const p_string *>(p_), enableHold); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) noexcept -> Variant {
			assertrx(0);
			abort();
		});
}
size_t PayloadFieldValue::Hash() const noexcept {
	return t_.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept { return std::hash<bool>()(*reinterpret_cast<const bool *>(p_)); },
		[&](KeyValueType::Int) noexcept { return std::hash<int>()(*reinterpret_cast<const int *>(p_)); },
		[&](KeyValueType::Int64) noexcept { return std::hash<int64_t>()(*reinterpret_cast<const int64_t *>(p_)); },
		[&](KeyValueType::Double) noexcept { return std::hash<double>()(*reinterpret_cast<const double *>(p_)); },
		[&](KeyValueType::String) noexcept { return std::hash<p_string>()(*reinterpret_cast<const p_string *>(p_)); },
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) noexcept -> size_t {
			assertrx(0);
			abort();
		});
}

bool PayloadFieldValue::IsEQ(const PayloadFieldValue &o) const {
	if (!t_.Type().IsSame(o.t_.Type())) return false;
	return t_.Type().EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept { return *reinterpret_cast<const bool *>(p_) == *reinterpret_cast<const bool *>(o.p_); },
		[&](KeyValueType::Int) noexcept { return *reinterpret_cast<const int *>(p_) == *reinterpret_cast<const int *>(o.p_); },
		[&](KeyValueType::Int64) noexcept { return *reinterpret_cast<const int64_t *>(p_) == *reinterpret_cast<const int64_t *>(o.p_); },
		[&](KeyValueType::Double) noexcept { return *reinterpret_cast<const double *>(p_) == *reinterpret_cast<const double *>(o.p_); },
		[&](KeyValueType::String) {
			return collateCompare(*reinterpret_cast<const p_string *>(p_), *reinterpret_cast<const p_string *>(o.p_), CollateOpts()) == 0;
		},
		[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) noexcept -> bool {
			assertrx(0);
			abort();
		});
}

}  // namespace reindexer
