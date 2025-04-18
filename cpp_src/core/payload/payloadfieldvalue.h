#pragma once

#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "core/keyvalue/variant.h"
#include "estl/one_of.h"
#include "payloadfieldtype.h"
#include "tools/stringstools.h"

namespace reindexer {

// Helper field's' value object
class PayloadFieldValue {
public:
	struct Array {
		unsigned offset;
		int len;
	};
	// Construct object
	PayloadFieldValue(const PayloadFieldType& t, uint8_t* v) noexcept : t_(t), p_(v) {}
	// Single value operations
	void Set(Variant kv) {
		t_.Type().EvaluateOneOf(
			[&kv](KeyValueType::Int64) {
				if (kv.Type().Is<KeyValueType::Int>()) {
					kv.convert(KeyValueType::Int64{});
				}
			},
			[&kv](KeyValueType::Int) {
				if (kv.Type().Is<KeyValueType::Int64>()) {
					kv.convert(KeyValueType::Int{});
				}
			},
			[&kv](KeyValueType::Uuid) {
				if (kv.Type().Is<KeyValueType::String>()) {
					kv.convert(KeyValueType::Uuid{});
				}
			},
			[](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Double, KeyValueType::Float, KeyValueType::Composite,
					 KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::FloatVector>) noexcept {});
		if (!kv.Type().IsSame(t_.Type())) {
			throwSetTypeMissmatch(kv);
		}

		t_.Type().EvaluateOneOf([&](KeyValueType::Int) noexcept { *reinterpret_cast<int*>(p_) = int(kv); },
								[&](KeyValueType::Bool) noexcept { *reinterpret_cast<bool*>(p_) = bool(kv); },
								[&](KeyValueType::Int64) noexcept { *reinterpret_cast<int64_t*>(p_) = int64_t(kv); },
								[&](KeyValueType::Double) noexcept { *reinterpret_cast<double*>(p_) = double(kv); },
								[&](KeyValueType::String) noexcept { *reinterpret_cast<p_string*>(p_) = p_string(kv); },
								[&](KeyValueType::Uuid) noexcept { *reinterpret_cast<Uuid*>(p_) = Uuid{kv}; },
								[&](KeyValueType::FloatVector) {
									assertrx(!kv.DoHold());
									ConstFloatVectorView vect{kv};
									if (!vect.IsEmpty() && t_.FloatVectorDimension() != vect.Dimension()) {
										throw Error{errNotValid,
													"Attempt to write vector of dimension {} in a float vector field of dimension {}",
													vect.Dimension().Value(), t_.FloatVectorDimension().Value()};
									}
									*reinterpret_cast<uint64_t*>(p_) = vect.Payload();
								},
								[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
										 KeyValueType::Float>) noexcept {
									assertrx(0);
									abort();
								});
	}
	Variant Get() noexcept { return Get(Variant::noHold); }
	template <typename HoldT>
	Variant Get(HoldT h) const noexcept(noexcept(Variant(std::declval<p_string>(), h))) {
		return t_.Type().EvaluateOneOf([&](KeyValueType::Bool) noexcept { return Variant(*reinterpret_cast<const bool*>(p_)); },
									   [&](KeyValueType::Int) noexcept { return Variant(*reinterpret_cast<const int*>(p_)); },
									   [&](KeyValueType::Int64) noexcept { return Variant(*reinterpret_cast<const int64_t*>(p_)); },
									   [&](KeyValueType::Double) noexcept { return Variant(*reinterpret_cast<const double*>(p_)); },
									   [&](KeyValueType::String) noexcept(noexcept(Variant(std::declval<p_string>(), h))) {
										   return Variant(*reinterpret_cast<const p_string*>(p_), h);
									   },
									   [&](KeyValueType::Uuid) noexcept { return Variant(*reinterpret_cast<const Uuid*>(p_)); },
									   [&](KeyValueType::FloatVector) noexcept {
										   return Variant{ConstFloatVectorView::FromUint64(*reinterpret_cast<const uint64_t*>(p_))};
									   },
									   [](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
												KeyValueType::Float>) noexcept -> Variant {
										   assertrx(0);
										   abort();
									   });
	}
	size_t Hash() const noexcept {
		return t_.Type().EvaluateOneOf(
			[&](KeyValueType::Bool) noexcept -> uint64_t { return std::hash<bool>()(*reinterpret_cast<const bool*>(p_)); },
			[&](KeyValueType::Int) noexcept -> uint64_t { return std::hash<int>()(*reinterpret_cast<const int*>(p_)); },
			[&](KeyValueType::Int64) noexcept -> uint64_t { return std::hash<int64_t>()(*reinterpret_cast<const int64_t*>(p_)); },
			[&](KeyValueType::Double) noexcept -> uint64_t { return std::hash<double>()(*reinterpret_cast<const double*>(p_)); },
			[&](KeyValueType::String) noexcept -> uint64_t { return std::hash<p_string>()(*reinterpret_cast<const p_string*>(p_)); },
			[&](KeyValueType::Uuid) noexcept -> uint64_t { return std::hash<Uuid>()(*reinterpret_cast<const Uuid*>(p_)); },
			[&](KeyValueType::FloatVector) noexcept {
				return ConstFloatVectorView::FromUint64(*reinterpret_cast<const uint64_t*>(p_)).Hash();
			},
			[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
					 KeyValueType::Float>) noexcept -> uint64_t {
				assertrx(0);
				abort();
			});
	}
	bool IsEQ(const PayloadFieldValue& o) const {
		if (!t_.Type().IsSame(o.t_.Type())) {
			return false;
		}
		return t_.Type().EvaluateOneOf(
			[&](KeyValueType::Bool) noexcept { return *reinterpret_cast<const bool*>(p_) == *reinterpret_cast<const bool*>(o.p_); },
			[&](KeyValueType::Int) noexcept { return *reinterpret_cast<const int*>(p_) == *reinterpret_cast<const int*>(o.p_); },
			[&](KeyValueType::Int64) noexcept { return *reinterpret_cast<const int64_t*>(p_) == *reinterpret_cast<const int64_t*>(o.p_); },
			[&](KeyValueType::Double) noexcept { return *reinterpret_cast<const double*>(p_) == *reinterpret_cast<const double*>(o.p_); },
			[&](KeyValueType::String) {
				return collateCompare<CollateNone>(*reinterpret_cast<const p_string*>(p_), *reinterpret_cast<const p_string*>(o.p_),
												   SortingPrioritiesTable()) == ComparationResult::Eq;
			},
			[&](KeyValueType::Uuid) noexcept { return *reinterpret_cast<const Uuid*>(p_) == *reinterpret_cast<const Uuid*>(o.p_); },
			[&](KeyValueType::FloatVector) noexcept {
				return *reinterpret_cast<const uint64_t*>(p_) == *reinterpret_cast<const uint64_t*>(o.p_);
			},
			[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
					 KeyValueType::Float>) noexcept -> bool {
				assertrx(0);
				abort();
			});
	}

	// Type of value, not owning
	const PayloadFieldType& t_;
	// Value data, not owning
	uint8_t* p_;

private:
	[[noreturn]] void throwSetTypeMissmatch(const Variant& kv);
};

}  // namespace reindexer
