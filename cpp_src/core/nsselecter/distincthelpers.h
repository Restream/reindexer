#pragma once
#include <vector>
#include "core/key_value_type.h"
#include "core/keyvalue/variant.h"
#include "core/payload/fieldsset.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"

namespace reindexer {
namespace DistinctHelpers {

using FieldsValue = h_vector<Variant, 2>;

struct [[nodiscard]] DataType {
	DataType() = default;
	template <typename T>
	DataType(T&& d, IsArray a) noexcept : data(std::move(d)), isArray(a) {}
	std::variant<std::span<const bool>, std::span<const int64_t>, std::span<const double>, std::span<const float>,
				 std::span<const std::string_view>, std::span<const int32_t>, std::span<const Uuid>, std::span<const p_string>,
				 VariantArray>
		data;
	IsArray isArray = IsArray_False;
};

enum class [[nodiscard]] IsCompositeSupported : bool { Yes = true, No = false };
template <IsCompositeSupported isCompositeSupported>
struct [[nodiscard]] DistinctHasher {
	DistinctHasher() = default;
	DistinctHasher(const PayloadType& type, const FieldsSet& fields) : type_(type), fields_(fields) {}
	size_t operator()(const DistinctHelpers::FieldsValue& vals) const {
		(void)vals;
		int h = 0;
		for (const auto& v : vals) {
			h = (h * 127) ^
				v.Type().EvaluateOneOf(
					[&](concepts::OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String,
										KeyValueType::Bool, KeyValueType::Int, KeyValueType::Uuid, KeyValueType::Null> auto) noexcept {
						return v.Hash();
					},
					[&](KeyValueType::Composite) -> size_t {
						if constexpr (isCompositeSupported == IsCompositeSupported::Yes) {
							return ConstPayload(type_, static_cast<const PayloadValue&>(v)).GetHash(fields_);
						} else {
							throw_as_assert;
						}
					},
					[](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::FloatVector> auto) -> size_t {
						throw_as_assert;
					});
		}
		return h;
	}

private:
	PayloadType type_;
	FieldsSet fields_;
};

// compare null value
template <IsCompositeSupported isCompositeSupported>
struct [[nodiscard]] CompareVariantVector {
	CompareVariantVector() = default;
	CompareVariantVector(const PayloadType& type, const FieldsSet& fields) : type_(type), fields_(fields) {}
	bool operator()(const DistinctHelpers::FieldsValue& v1, const DistinctHelpers::FieldsValue& v2) const {
		assertrx_throw(v1.size() == v2.size());
		for (unsigned i = 0; i < v1.size(); i++) {
			const bool res = v1[i].Type().EvaluateOneOf(
				[&](concepts::OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String,
									KeyValueType::Bool, KeyValueType::Int, KeyValueType::Uuid, KeyValueType::Null> auto) {
					return v1[i].Compare<NotComparable::Return, kDefaultNullsHandling>(v2[i]) == ComparationResult::Eq;
				},
				[&](KeyValueType::Composite) -> bool {
					if constexpr (isCompositeSupported == IsCompositeSupported::Yes) {
						return ConstPayload(type_, static_cast<const PayloadValue&>(v1[i]))
							.IsEQ(static_cast<const PayloadValue&>(v2[i]), fields_);
					} else {
						throw_as_assert;
					}
				},
				[](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::FloatVector> auto) -> bool {
					throw_as_assert;
				});
			if (!res) {
				return false;
			}
		}
		return true;
	}

private:
	PayloadType type_;
	FieldsSet fields_;
};

template <IsCompositeSupported isCompositeSupported>
struct [[nodiscard]] LessDistinctVector {
	LessDistinctVector() = default;
	LessDistinctVector(const PayloadType& type, const FieldsSet& fields) : type_(type), fields_(fields) {}
	bool operator()(const DistinctHelpers::FieldsValue& v1, const DistinctHelpers::FieldsValue& v2) const {
		assertrx_throw(v1.size() == v2.size());
		for (unsigned i = 0; i < v1.size(); i++) {
			if (!v1[i].Type().IsSame(v2[i].Type())) {
				return v1[i].Type().ToTagType() < v2[i].Type().ToTagType();
			}
			const bool res = v1[i].Type().EvaluateOneOf(
				[&](concepts::OneOf<KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float, KeyValueType::String,
									KeyValueType::Bool, KeyValueType::Int, KeyValueType::Uuid, KeyValueType::Null> auto) {
					return v1[i].Compare<NotComparable::Return, kDefaultNullsHandling>(v2[i]) == ComparationResult::Lt;
				},
				[&](KeyValueType::Composite) -> bool {
					if constexpr (isCompositeSupported == IsCompositeSupported::Yes) {
						const PayloadValue& l = static_cast<const PayloadValue&>(v1[i]);
						const PayloadValue& r = static_cast<const PayloadValue&>(v2[i]);
						return ConstPayload(type_, l).Compare<WithString::No, NotComparable::Return, kDefaultNullsHandling>(r, fields_) ==
							   ComparationResult::Lt;
					} else {
						throw_as_assert;
					}
				},
				[](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::FloatVector> auto) -> bool {
					throw_as_assert;
				});
			return res;
		}
		return false;
	}

private:
	PayloadType type_;
	FieldsSet fields_;
};

bool GetMultiFieldValue(const std::vector<DataType>& data, unsigned long dataIndex, unsigned int rowLen, FieldsValue& values);

}  // namespace DistinctHelpers
}  // namespace reindexer
