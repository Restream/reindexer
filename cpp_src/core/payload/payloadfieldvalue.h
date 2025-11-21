#pragma once

#include "core/keyvalue/p_string.h"
#include "core/keyvalue/uuid.h"
#include "core/keyvalue/variant.h"
#include "payloadfieldtype.h"
#include "tools/stringstools.h"

namespace reindexer {

// Helper field's' value object
class [[nodiscard]] PayloadFieldValue {
public:
	struct [[nodiscard]] Array {
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
					std::ignore = kv.convert(KeyValueType::Int64{});
				}
			},
			[&kv](KeyValueType::Int) {
				if (kv.Type().Is<KeyValueType::Int64>()) {
					std::ignore = kv.convert(KeyValueType::Int{});
				}
			},
			[&kv](KeyValueType::Uuid) {
				if (kv.Type().Is<KeyValueType::String>()) {
					std::ignore = kv.convert(KeyValueType::Uuid{});
				}
			},
			[](concepts::OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Double, KeyValueType::Float, KeyValueType::Composite,
							   KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Tuple, KeyValueType::FloatVector> auto) noexcept {
			});
		if (!kv.Type().IsSame(t_.Type())) {
			throwSetTypeMissmatch(kv);
		}

		t_.Type().EvaluateOneOf([&](KeyValueType::Int) noexcept { copyVariantToPtr<int>(kv); },
								[&](KeyValueType::Bool) noexcept { copyVariantToPtr<bool>(kv); },
								[&](KeyValueType::Int64) noexcept { copyVariantToPtr<int64_t>(kv); },
								[&](KeyValueType::Double) noexcept { copyVariantToPtr<double>(kv); },
								[&](KeyValueType::String) noexcept { copyVariantToPtr<p_string>(kv); },
								[&](KeyValueType::Uuid) noexcept { copyVariantToPtr<Uuid>(kv); },
								[&](KeyValueType::FloatVector) {
									assertrx(!kv.DoHold());
									ConstFloatVectorView vect{kv};
									if (!vect.IsEmpty() && t_.FloatVectorDimension() != vect.Dimension()) {
										throw Error{errNotValid,
													"Attempt to write vector of dimension {} in a float vector field of dimension {}",
													vect.Dimension().Value(), t_.FloatVectorDimension().Value()};
									}
									uint64_t v = vect.Payload();
									std::memcpy(p_, &v, sizeof(uint64_t));
								},
								[](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
												   KeyValueType::Null, KeyValueType::Float> auto) noexcept {
									assertrx(0);
									abort();
								});
	}
	Variant Get() noexcept { return Get(Variant::noHold); }
	template <typename HoldT>
	Variant Get(HoldT h) const noexcept(noexcept(Variant(std::declval<p_string>(), h))) {
		return t_.Type().EvaluateOneOf([&](KeyValueType::Bool) noexcept { return getVariantFromPtr<bool>(); },
									   [&](KeyValueType::Int) noexcept { return getVariantFromPtr<int>(); },
									   [&](KeyValueType::Int64) noexcept { return getVariantFromPtr<int64_t>(); },
									   [&](KeyValueType::Double) noexcept { return getVariantFromPtr<double>(); },
									   [&](KeyValueType::String) noexcept(noexcept(Variant(std::declval<p_string>(), h))) {
										   p_string v;
										   std::memcpy(&v, p_, sizeof(p_string));
										   return Variant(v, h);
									   },
									   [&](KeyValueType::Uuid) noexcept { return getVariantFromPtr<Uuid>(); },
									   [&](KeyValueType::FloatVector) noexcept {
										   uint64_t v;
										   std::memcpy(&v, p_, sizeof(uint64_t));
										   return Variant{ConstFloatVectorView::FromUint64(v)};
									   },
									   [](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
														  KeyValueType::Null, KeyValueType::Float> auto) noexcept -> Variant {
										   assertrx(0);
										   abort();
									   });
	}
	size_t Hash() const noexcept {
		return t_.Type().EvaluateOneOf([&](KeyValueType::Bool) noexcept -> uint64_t { return getHashFromPtr<bool>(); },
									   [&](KeyValueType::Int) noexcept -> uint64_t { return getHashFromPtr<int>(); },
									   [&](KeyValueType::Int64) noexcept -> uint64_t { return getHashFromPtr<int64_t>(); },
									   [&](KeyValueType::Double) noexcept -> uint64_t { return getHashFromPtr<double>(); },
									   [&](KeyValueType::String) noexcept -> uint64_t { return getHashFromPtr<p_string>(); },
									   [&](KeyValueType::Uuid) noexcept -> uint64_t { return getHashFromPtr<Uuid>(); },
									   [&](KeyValueType::FloatVector) noexcept {
										   uint64_t v;
										   std::memcpy(&v, p_, sizeof(uint64_t));
										   return ConstFloatVectorView::FromUint64(v).Hash();
									   },
									   [](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
														  KeyValueType::Null, KeyValueType::Float> auto) noexcept -> uint64_t {
										   assertrx(0);
										   abort();
									   });
	}
	bool IsEQ(const PayloadFieldValue& o) const {
		if (!t_.Type().IsSame(o.t_.Type())) {
			return false;
		}
		return t_.Type().EvaluateOneOf([&](KeyValueType::Bool) noexcept { return (std::memcmp(p_, o.p_, sizeof(bool)) == 0); },
									   [&](KeyValueType::Int) noexcept { return (std::memcmp(p_, o.p_, sizeof(int)) == 0); },
									   [&](KeyValueType::Int64) noexcept { return (std::memcmp(p_, o.p_, sizeof(int64_t)) == 0); },
									   [&](KeyValueType::Double) noexcept { return (std::memcmp(p_, o.p_, sizeof(double)) == 0); },
									   [&](KeyValueType::String) {
										   p_string v1, v2;
										   std::memcpy(&v1, p_, sizeof(p_string));
										   std::memcpy(&v2, o.p_, sizeof(p_string));
										   return collateCompare<CollateNone>(v1, v2, SortingPrioritiesTable()) == ComparationResult::Eq;
									   },
									   [&](KeyValueType::Uuid) noexcept { return (std::memcmp(p_, o.p_, sizeof(Uuid)) == 0); },
									   [&](KeyValueType::FloatVector) noexcept { return (std::memcmp(p_, o.p_, sizeof(uint64_t)) == 0); },
									   [](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite,
														  KeyValueType::Null, KeyValueType::Float> auto) noexcept -> bool {
										   assertrx(0);
										   abort();
									   });
	}

	// Type of value, not owning
	const PayloadFieldType& t_;
	// Value data, not owning
	uint8_t* p_;

private:
	template <typename T>
	void copyVariantToPtr(const Variant& kv) noexcept(std::is_nothrow_convertible_v<Variant, T>) {
		auto v = T(kv);
		std::memcpy(p_, &v, sizeof(T));
	}

	template <typename T>
	Variant getVariantFromPtr() const noexcept {
		T v;
		std::memcpy(&v, p_, sizeof(T));
		return Variant(v);
	}

	template <typename T>
	uint64_t getHashFromPtr() const noexcept {
		T v;
		std::memcpy(&v, p_, sizeof(T));
		return std::hash<T>()(v);
	}

	[[noreturn]] void throwSetTypeMissmatch(const Variant& kv);
};

}  // namespace reindexer
