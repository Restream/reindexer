#pragma once

#include <variant>

#include "comparator_indexed_distinct.h"
#include "const.h"
#include "core/index/payload_map.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadfieldvalue.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadvalue.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "helpers.h"
#include "tools/float_comparison.h"
#include "tools/string_regexp_functions.h"

namespace reindexer {

namespace comparators {

template <typename T, CondType Cond>
struct [[nodiscard]] ValuesHolder {
	using Type = T;
};

template <CondType Cond>
struct [[nodiscard]] ValuesHolder<key_string, Cond> {
	struct [[nodiscard]] Type {
		Type() noexcept = default;
		Type(key_string v) noexcept : value_{std::move(v)}, valueView_{value_} {}
		Type(const Type& other) noexcept : Type{other.value_} {}
		Type(Type&& other) noexcept : Type{std::move(other.value_)} { other.valueView_ = {}; }
		Type& operator=(const Type& other) noexcept {
			Type tmp{other.value_};
			value_ = std::move(tmp.value_);
			valueView_ = tmp.valueView_;
			return *this;
		}
		Type& operator=(Type&& other) noexcept {
			Type tmp{std::move(other.value_)};
			value_ = std::move(tmp.value_);
			valueView_ = tmp.valueView_;
			other.valueView_ = {};
			return *this;
		}
		key_string value_{nullptr};
		std::string_view valueView_{};
	};
};

template <typename T>
struct [[nodiscard]] ValuesHolder<T, CondRange> {
	// Empty
};

template <typename T>
struct [[nodiscard]] ValuesHolder<T, CondSet> {
	using Type = fast_hash_set<T>;
};

template <>
struct [[nodiscard]] ValuesHolder<key_string, CondSet> {
	using Type = key_string_set;
};

template <>
struct [[nodiscard]] ValuesHolder<PayloadValue, CondSet> {
	using Type = unordered_payload_ref_set;
};

template <typename T>
struct [[nodiscard]] ValuesHolder<T, CondAllSet> {
	struct [[nodiscard]] Type {
		fast_hash_map<T, int> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <>
struct [[nodiscard]] ValuesHolder<key_string, CondAllSet> {
	struct [[nodiscard]] Type {
		key_string_map<int> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <>
struct [[nodiscard]] ValuesHolder<PayloadValue, CondAllSet> {
	struct [[nodiscard]] Type {
		unordered_payload_map<int, false> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <typename T>
struct [[nodiscard]] DataHolder {
	using SingleType = typename ValuesHolder<T, CondEq>::Type;
	using SetType = typename ValuesHolder<T, CondSet>::Type;
	using SetWrpType = const intrusive_atomic_rc_wrapper<SetType>;	// must be const for safe intrusive copying
	using SetPtrType = intrusive_ptr<SetWrpType>;
	using AllSetType = typename ValuesHolder<T, CondAllSet>::Type;
	using AllSetPtrType = std::unique_ptr<AllSetType>;

	DataHolder() noexcept : cond_{CondEq} {}
	DataHolder(DataHolder&& other) noexcept = default;
	DataHolder(const DataHolder& o)
		: cond_{o.cond_},
		  value_{o.value_},
		  value2_{o.value2_},
		  setPtr_{o.setPtr_},
		  allSetPtr_{o.allSetPtr_ ? std::make_unique<AllSetType>(*o.allSetPtr_) : nullptr} {
		// allSetPtr's data are modified during comparison, so we have to make a real copy
	}
	DataHolder& operator=(DataHolder&& other) noexcept = default;
	DataHolder& operator=(const DataHolder& o) = delete;

	CondType cond_;
	SingleType value_{};   // Either single value or right range boundary
	SingleType value2_{};  // Left range boundary
	SetPtrType setPtr_{};
	AllSetPtrType allSetPtr_{};
};

template <typename T>
RX_ALWAYS_INLINE bool SafeEqualWithFP(const T& left, const T& right) {
	if constexpr (std::is_floating_point_v<T>) {
		return fp::ExactlyEqual(left, right);
	} else {
		return left == right;
	}
}

template <typename T>
class [[nodiscard]] ComparatorIndexedOffsetScalar : private DataHolder<T> {
public:
	ComparatorIndexedOffsetScalar(size_t offset, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		const T* ptr = reinterpret_cast<T*>(item.Ptr() + offset_);
		switch (this->cond_) {
			case CondEq:
				return SafeEqualWithFP(*ptr, this->value_);
			case CondLt:
				return *ptr < this->value_;
			case CondLe:
				return *ptr <= this->value_;
			case CondGt:
				return *ptr > this->value_;
			case CondGe:
				return *ptr >= this->value_;
			case CondRange:
				return this->value_ <= *ptr && *ptr <= this->value2_;
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return this->setPtr_->find(*ptr) != this->setPtr_->cend();
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return this->allSetPtr_->values_.size() == 1 && this->allSetPtr_->values_.find(*ptr) != this->allSetPtr_->values_.cend();
			case CondAny:
			case CondEmpty:
			case CondLike:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	size_t offset_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedColumnScalar : private DataHolder<T> {
public:
	ComparatorIndexedColumnScalar(const void* rawData, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const noexcept {
		const T& v = *(rawData_ + rowId);
		switch (this->cond_) {
			case CondEq:
				return SafeEqualWithFP(v, this->value_);
			case CondLt:
				return v < this->value_;
			case CondLe:
				return v <= this->value_;
			case CondGt:
				return v > this->value_;
			case CondGe:
				return v >= this->value_;
			case CondRange:
				return this->value_ <= v && v <= this->value2_;
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return this->setPtr_->find(v) != this->setPtr_->cend();
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return this->allSetPtr_->values_.size() == 1 && this->allSetPtr_->values_.find(v) != this->allSetPtr_->values_.cend();
			case CondAny:
			case CondEmpty:
			case CondLike:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxColumnComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	const T* rawData_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedOffsetScalarDistinct : private DataHolder<T> {
public:
	ComparatorIndexedOffsetScalarDistinct(size_t offset, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		const T& value = *reinterpret_cast<T*>(item.Ptr() + offset_);
		switch (this->cond_) {
			case CondEq:
				return SafeEqualWithFP(value, this->value_) && distinct_.Compare(value);
			case CondLt:
				return value < this->value_ && distinct_.Compare(value);
			case CondLe:
				return value <= this->value_ && distinct_.Compare(value);
			case CondGt:
				return value > this->value_ && distinct_.Compare(value);
			case CondGe:
				return value >= this->value_ && distinct_.Compare(value);
			case CondRange:
				return this->value_ <= value && value <= this->value2_ && distinct_.Compare(value);
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return this->setPtr_->find(value) != this->setPtr_->cend() && distinct_.Compare(value);
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return this->allSetPtr_->values_.size() == 1 && this->allSetPtr_->values_.find(value) != this->allSetPtr_->values_.cend() &&
					   distinct_.Compare(value);
			case CondAny:
			case CondEmpty:
			case CondLike:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(*reinterpret_cast<T*>(item.Ptr() + offset_));
	}

private:
	ComparatorIndexedDistinct<T> distinct_;
	size_t offset_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedColumnScalarDistinct : private DataHolder<T> {
public:
	ComparatorIndexedColumnScalarDistinct(const void* rawData, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const noexcept {
		const T& value = *(rawData_ + rowId);
		switch (this->cond_) {
			case CondEq:
				return SafeEqualWithFP(value, this->value_) && distinct_.Compare(value);
			case CondLt:
				return value < this->value_ && distinct_.Compare(value);
			case CondLe:
				return value <= this->value_ && distinct_.Compare(value);
			case CondGt:
				return value > this->value_ && distinct_.Compare(value);
			case CondGe:
				return value >= this->value_ && distinct_.Compare(value);
			case CondRange:
				return this->value_ <= value && value <= this->value2_ && distinct_.Compare(value);
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return this->setPtr_->find(value) != this->setPtr_->cend() && distinct_.Compare(value);
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return this->allSetPtr_->values_.size() == 1 && this->allSetPtr_->values_.find(value) != this->allSetPtr_->values_.cend() &&
					   distinct_.Compare(value);
			case CondAny:
			case CondEmpty:
			case CondLike:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxColumnComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType rowId) { distinct_.ExcludeValues(*(rawData_ + rowId)); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	const T* rawData_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedOffsetArray : private DataHolder<T> {
public:
	ComparatorIndexedOffsetArray(size_t offset, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			switch (this->cond_) {
				case CondEq:
					if (SafeEqualWithFP(*ptr, this->value_)) {
						return true;
					}
					continue;
				case CondLt:
					if (*ptr < this->value_) {
						return true;
					}
					continue;
				case CondLe:
					if (*ptr <= this->value_) {
						return true;
					}
					continue;
				case CondGt:
					if (*ptr > this->value_) {
						return true;
					}
					continue;
				case CondGe:
					if (*ptr >= this->value_) {
						return true;
					}
					continue;
				case CondRange:
					if (this->value_ <= *ptr && *ptr <= this->value2_) {
						return true;
					}
					continue;
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (this->setPtr_->find(*ptr) != this->setPtr_->cend()) {
						return true;
					}
					continue;
				case CondAllSet: {
					assertrx_dbg(this->allSetPtr_);
					const auto it = this->allSetPtr_->values_.find(*ptr);
					if (it != this->allSetPtr_->values_.cend()) {
						this->allSetPtr_->allSetValues_.insert(it->second);
						if (this->allSetPtr_->allSetValues_.size() == this->allSetPtr_->values_.size()) {
							return true;
						}
					}
				}
					continue;
				case CondLike:
				case CondAny:
				case CondEmpty:
				case CondDWithin:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	size_t offset_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedOffsetArrayDistinct : private DataHolder<T> {
public:
	ComparatorIndexedOffsetArrayDistinct(size_t offset, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			switch (this->cond_) {
				case CondEq:
					if (SafeEqualWithFP(*ptr, this->value_) && distinct_.Compare(*ptr)) {
						return true;
					}
					continue;
				case CondLt:
					if (*ptr < this->value_ && distinct_.Compare(*ptr)) {
						return true;
					}
					continue;
				case CondLe:
					if (*ptr <= this->value_ && distinct_.Compare(*ptr)) {
						return true;
					}
					continue;
				case CondGt:
					if (*ptr > this->value_ && distinct_.Compare(*ptr)) {
						return true;
					}
					continue;
				case CondGe:
					if (*ptr >= this->value_ && distinct_.Compare(*ptr)) {
						return true;
					}
					continue;
				case CondRange:
					if (this->value_ <= *ptr && *ptr <= this->value2_ && distinct_.Compare(*ptr)) {
						return true;
					}
					continue;
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (this->setPtr_->find(*ptr) != this->setPtr_->cend() && distinct_.Compare(*ptr)) {
						return true;
					}
					continue;
				case CondAllSet: {
					assertrx_dbg(this->allSetPtr_);
					bool haveDistinct = false;
					const auto it = this->allSetPtr_->values_.find(*ptr);
					if (it != this->allSetPtr_->values_.cend()) {
						haveDistinct |= distinct_.Compare(*ptr);
						this->allSetPtr_->allSetValues_.insert(it->second);
						if (haveDistinct && this->allSetPtr_->allSetValues_.size() == this->allSetPtr_->values_.size()) {
							return true;
						}
					}
				}
					continue;
				case CondLike:
				case CondAny:
				case CondEmpty:
				case CondDWithin:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(*ptr);
		}
	}

private:
	ComparatorIndexedDistinct<T> distinct_;
	size_t offset_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedJsonPath : private DataHolder<T> {
	using Base = DataHolder<T>;
	using SingleType = typename Base::SingleType;

public:
	ComparatorIndexedJsonPath(const TagsPath& tagsPath, const PayloadType& payloadType, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& value : buffer_) {
			if (value.IsNullValue()) [[unlikely]] {
				continue;
			}
			switch (this->cond_) {
				case CondEq:
					if (SafeEqualWithFP(value.As<T>(), this->value_)) {
						return true;
					}
					continue;
				case CondLt:
					if (value.As<T>() < this->value_) {
						return true;
					}
					continue;
				case CondLe:
					if (value.As<T>() <= this->value_) {
						return true;
					}
					continue;
				case CondGt:
					if (value.As<T>() > this->value_) {
						return true;
					}
					continue;
				case CondGe:
					if (value.As<T>() >= this->value_) {
						return true;
					}
					continue;
				case CondRange: {
					const auto v = value.As<T>();
					if (this->value_ <= v && v <= this->value2_) {
						return true;
					}
				}
					continue;
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (this->setPtr_->find(value.As<T>()) != this->setPtr_->cend()) {
						return true;
					}
					continue;
				case CondAllSet: {
					assertrx_dbg(this->allSetPtr_);
					const auto it = this->allSetPtr_->values_.find(value.As<T>());
					if (it != this->allSetPtr_->values_.cend()) {
						this->allSetPtr_->allSetValues_.insert(it->second);
						if (this->allSetPtr_->allSetValues_.size() == this->allSetPtr_->values_.size()) {
							return true;
						}
					}
				}
					continue;
				case CondEmpty:
				case CondAny:
				case CondDWithin:
				case CondLike:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedJsonPathDistinct : private DataHolder<T> {
	using Base = DataHolder<T>;
	using SingleType = typename Base::SingleType;

public:
	ComparatorIndexedJsonPathDistinct(const TagsPath& tagsPath, const PayloadType& payloadType, const VariantArray&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& v : buffer_) {
			if (v.IsNullValue()) [[unlikely]] {
				continue;
			}
			const auto value = v.As<T>();
			switch (this->cond_) {
				case CondEq:
					if (SafeEqualWithFP(value, this->value_) && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondLt:
					if (value < this->value_ && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondLe:
					if (value <= this->value_ && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondGt:
					if (value > this->value_ && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondGe:
					if (value >= this->value_ && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondRange:
					if (this->value_ <= value && value <= this->value2_ && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (this->setPtr_->find(value) != this->setPtr_->cend() && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondAllSet: {
					assertrx_dbg(this->allSetPtr_);
					bool haveDistinct = false;
					const auto it = this->allSetPtr_->values_.find(value);
					if (it != this->allSetPtr_->values_.cend()) {
						haveDistinct |= distinct_.Compare(value);
						this->allSetPtr_->allSetValues_.insert(it->second);
						if (haveDistinct && this->allSetPtr_->allSetValues_.size() == this->allSetPtr_->values_.size()) {
							return true;
						}
					}
				}
					continue;
				case CondEmpty:
				case CondAny:
				case CondDWithin:
				case CondLike:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& v : buffer_) {
			if (v.IsNullValue()) [[unlikely]] {
				continue;
			}
			distinct_.ExcludeValues(v.As<T>());
		}
	}

private:
	ComparatorIndexedDistinct<T> distinct_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class [[nodiscard]] ComparatorIndexedOffsetScalarString : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetScalarString(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const std::string_view value = *reinterpret_cast<const p_string*>(item.Ptr() + offset_);
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend();
			case CondRange:
				return (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le);
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return allSetPtr_->values_.size() == 1 && allSetPtr_->values_.find(value) != allSetPtr_->values_.cend();
			case CondLike:
				return matchLikePattern(value, value_.valueView_);
			case CondEq:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq;
			case CondLt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt;
			case CondLe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le;
			case CondGt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt;
			case CondGe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge;
			case CondAny:
			case CondEmpty:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	const CollateOpts* collateOpts_;
	size_t offset_;
};

// There are no column rawData for the string indexes
class [[nodiscard]] ComparatorIndexedColumnScalarString : private DataHolder<key_string> {
public:
	ComparatorIndexedColumnScalarString(const void* rawData, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const {
		const std::string_view value(*(rawData_ + rowId));
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend();
			case CondRange:
				return (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le);
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return allSetPtr_->values_.size() == 1 && allSetPtr_->values_.find(value) != allSetPtr_->values_.cend();
			case CondLike:
				return matchLikePattern(value, value_.valueView_);
			case CondEq:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq;
			case CondLt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt;
			case CondLe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le;
			case CondGt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt;
			case CondGe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge;
			case CondAny:
			case CondEmpty:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxColumnComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	const CollateOpts* collateOpts_;
	const std::string_view* rawData_;
};

class [[nodiscard]] ComparatorIndexedOffsetScalarStringDistinct : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetScalarStringDistinct(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const std::string_view value = *reinterpret_cast<const p_string*>(item.Ptr() + offset_);
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend() && distinct_.Compare(value);
			case CondRange:
				return (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le) && distinct_.Compare(value);
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return allSetPtr_->values_.size() == 1 && allSetPtr_->values_.find(value) != allSetPtr_->values_.cend() &&
					   distinct_.Compare(value);
			case CondLike:
				return matchLikePattern(value, value_.valueView_) && distinct_.Compare(value);
			case CondEq:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq && distinct_.Compare(value);
			case CondLt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt && distinct_.Compare(value);
			case CondLe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le && distinct_.Compare(value);
			case CondGt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt && distinct_.Compare(value);
			case CondGe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge && distinct_.Compare(value);
			case CondAny:
			case CondEmpty:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(std::string_view(*reinterpret_cast<const p_string*>(item.Ptr() + offset_)));
	}

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedColumnScalarStringDistinct : private DataHolder<key_string> {
public:
	ComparatorIndexedColumnScalarStringDistinct(const void* rawData, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const {
		const std::string_view value(*(rawData_ + rowId));
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend() && distinct_.Compare(value);
			case CondRange:
				return (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le) && distinct_.Compare(value);
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return allSetPtr_->values_.size() == 1 && allSetPtr_->values_.find(value) != allSetPtr_->values_.cend() &&
					   distinct_.Compare(value);
			case CondLike:
				return matchLikePattern(value, value_.valueView_) && distinct_.Compare(value);
			case CondEq:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq && distinct_.Compare(value);
			case CondLt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt && distinct_.Compare(value);
			case CondLe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le && distinct_.Compare(value);
			case CondGt:
				return collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt && distinct_.Compare(value);
			case CondGe:
				return collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge && distinct_.Compare(value);
			case CondAny:
			case CondEmpty:
			case CondDWithin:
			case CondKnn:
			default:
				abort();
		}
	}
	static double CostMultiplier() noexcept { return comparators::kIdxColumnComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType rowId) { distinct_.ExcludeValues(*(rawData_ + rowId)); }

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	const std::string_view* rawData_;
};

class [[nodiscard]] ComparatorIndexedOffsetArrayString : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetArrayString(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const p_string* const end = ptr + arr.len; ptr != end; ++ptr) {
			const std::string_view value = *ptr;
			switch (cond_) {
				case CondSet:
					if (setPtr_->find(value) != setPtr_->cend()) {
						return true;
					}
					continue;
				case CondRange:
					if ((collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le)) {
						return true;
					}
					continue;
				case CondAllSet: {
					assertrx_dbg(this->allSetPtr_);
					const auto it = allSetPtr_->values_.find(value);
					if (it != allSetPtr_->values_.cend()) {
						allSetPtr_->allSetValues_.insert(it->second);
						if (allSetPtr_->allSetValues_.size() == allSetPtr_->values_.size()) {
							return true;
						}
					}
				}
					continue;
				case CondLike:
					if (matchLikePattern(value, value_.valueView_)) {
						return true;
					}
					continue;
				case CondEq:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq) {
						return true;
					}
					continue;
				case CondLt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt) {
						return true;
					}
					continue;
				case CondLe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le) {
						return true;
					}
					continue;
				case CondGt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt) {
						return true;
					}
					continue;
				case CondGe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) {
						return true;
					}
					continue;
				case CondAny:
				case CondEmpty:
				case CondDWithin:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	const CollateOpts* collateOpts_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedOffsetArrayStringDistinct : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetArrayStringDistinct(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const p_string* const end = ptr + arr.len; ptr != end; ++ptr) {
			const std::string_view value = *ptr;
			switch (cond_) {
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (setPtr_->find(value) != setPtr_->cend() && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondRange:
					if ((collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le) && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondAllSet: {
					bool haveDistinct = false;
					assertrx_dbg(this->allSetPtr_);
					const auto it = allSetPtr_->values_.find(value);
					if (it != allSetPtr_->values_.cend()) {
						haveDistinct |= distinct_.Compare(value);
						allSetPtr_->allSetValues_.insert(it->second);
						if (haveDistinct && allSetPtr_->allSetValues_.size() == allSetPtr_->values_.size()) {
							return true;
						}
					}
				}
					continue;
				case CondLike:
					if (matchLikePattern(value, value_.valueView_) && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondEq:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondLt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondLe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondGt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondGe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge && distinct_.Compare(value)) {
						return true;
					}
					continue;
				case CondAny:
				case CondEmpty:
				case CondDWithin:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const p_string* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(std::string_view(*ptr));
		}
	}

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedJsonPathString : private DataHolder<key_string> {
	using Base = DataHolder<key_string>;

public:
	ComparatorIndexedJsonPathString(const TagsPath&, const PayloadType&, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& v : buffer_) {
			if (v.IsNullValue()) [[unlikely]] {
				continue;
			}

			if (!v.Type().Is<KeyValueType::String>()) {
				v.convert(KeyValueType::String{});
			}
			const auto value = static_cast<std::string_view>(v);
			switch (cond_) {
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (setPtr_->find(value) != setPtr_->cend()) {
						return true;
					}
					break;
				case CondRange:
					if ((collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le)) {
						return true;
					}
					break;
				case CondAllSet: {
					assertrx_dbg(this->allSetPtr_);
					const auto it = allSetPtr_->values_.find(value);
					if (it != allSetPtr_->values_.cend()) {
						allSetPtr_->allSetValues_.insert(it->second);
						if (allSetPtr_->allSetValues_.size() == allSetPtr_->values_.size()) {
							return true;
						}
					}
				} break;
				case CondLike:
					if (matchLikePattern(value, value_.valueView_)) {
						return true;
					}
					break;
				case CondEq:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq) {
						return true;
					}
					break;
				case CondLt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt) {
						return true;
					}
					break;
				case CondLe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le) {
						return true;
					}
					break;
				case CondGt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt) {
						return true;
					}
					break;
				case CondGe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) {
						return true;
					}
					break;
				case CondEmpty:
				case CondAny:
				case CondDWithin:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	const CollateOpts* collateOpts_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class [[nodiscard]] ComparatorIndexedJsonPathStringDistinct : private DataHolder<key_string> {
	using Base = DataHolder<key_string>;

public:
	ComparatorIndexedJsonPathStringDistinct(const TagsPath&, const PayloadType&, const VariantArray&, const CollateOpts&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& v : buffer_) {
			if (v.IsNullValue()) [[unlikely]] {
				continue;
			}

			if (!v.Type().Is<KeyValueType::String>()) {
				v.convert(KeyValueType::String{});
			}
			const auto value = static_cast<std::string_view>(v);
			switch (cond_) {
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (setPtr_->find(value) != setPtr_->cend() && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondRange:
					if ((collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, value2_.valueView_, *collateOpts_) & ComparationResult::Le) && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondAllSet: {
					bool haveDistinct = false;
					assertrx_dbg(this->allSetPtr_);
					const auto it = allSetPtr_->values_.find(value);
					if (it != allSetPtr_->values_.cend()) {
						haveDistinct |= distinct_.Compare(value);
						allSetPtr_->allSetValues_.insert(it->second);
						if (haveDistinct && allSetPtr_->allSetValues_.size() == allSetPtr_->values_.size()) {
							return true;
						}
					}
				} break;
				case CondLike:
					if (matchLikePattern(value, value_.valueView_) && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondEq:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Eq && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondLt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Lt && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondLe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Le && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondGt:
					if (collateCompare(value, value_.valueView_, *collateOpts_) == ComparationResult::Gt && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondGe:
					if (collateCompare(value, value_.valueView_, *collateOpts_) & ComparationResult::Ge && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondEmpty:
				case CondAny:
				case CondDWithin:
				case CondKnn:
				default:
					abort();
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& v : buffer_) {
			if (v.IsNullValue()) [[unlikely]] {
				continue;
			}

			if (!v.Type().Is<KeyValueType::String>()) {
				v.convert(KeyValueType::String{});
			}
			const auto value = static_cast<std::string_view>(v);
			distinct_.ExcludeValues(value);
		}
	}

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class [[nodiscard]] ComparatorIndexedCompositeBase : protected DataHolder<PayloadValue> {
	using Base = DataHolder<PayloadValue>;

public:
	ComparatorIndexedCompositeBase(const VariantArray& values, const CollateOpts& collate, const FieldsSet& fields,
								   const PayloadType& payloadType, CondType cond);

	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;

	RX_ALWAYS_INLINE bool CompareItems(const PayloadValue& pv) {
		ConstPayload item{payloadType_, pv};
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(pv) != setPtr_->cend();
			case CondRange: {
				return (item.Compare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_, *fields_, *collateOpts_) &
						ComparationResult::Ge) &&
					   (item.Compare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value2_, *fields_, *collateOpts_) &
						ComparationResult::Le);
			}
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return allSetPtr_->values_.size() == 1 && allSetPtr_->values_.find(pv) != allSetPtr_->values_.end();
			case CondEq:
				return item.Compare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_, *fields_, *collateOpts_) ==
					   ComparationResult::Eq;
			case CondLt:
				return item.Compare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_, *fields_, *collateOpts_) ==
					   ComparationResult::Lt;
			case CondLe:
				return item.Compare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_, *fields_, *collateOpts_) &
					   ComparationResult::Le;
			case CondGt:
				return item.Compare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_, *fields_, *collateOpts_) ==
					   ComparationResult::Gt;
			case CondGe:
				return item.Compare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_, *fields_, *collateOpts_) &
					   ComparationResult::Ge;
			case CondAny:
			case CondEmpty:
			case CondDWithin:
			case CondLike:
			case CondKnn:
			default:
				abort();
		}
	}

protected:
	// Using pointer for cheap copying and ExpressionTree size reduction
	using FieldsSetWrp = const intrusive_atomic_rc_wrapper<FieldsSet>;	// must be const for safe intrusive copying

	const CollateOpts* collateOpts_;
	intrusive_ptr<FieldsSetWrp> fields_;
	PayloadType payloadType_;
};

class [[nodiscard]] ComparatorIndexedComposite : public ComparatorIndexedCompositeBase {
	using Base = ComparatorIndexedCompositeBase;
	using Base::Base;

public:
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& pv, IdType) { return CompareItems(pv); }
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType) const noexcept {}
};

class [[nodiscard]] ComparatorIndexedCompositeDistinct : public ComparatorIndexedCompositeBase {
	using Base = ComparatorIndexedCompositeBase;

public:
	ComparatorIndexedCompositeDistinct(const VariantArray&, const CollateOpts&, const FieldsSet&, const PayloadType&, CondType);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& pv, IdType /*rowId*/) { return CompareItems(pv) && distinct_.Compare(pv); }

	void ExcludeDistinctValues(const PayloadValue& item, IdType) { distinct_.ExcludeValues(item); }

	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }

private:
	using CompositeTypes = h_vector<KeyValueType, 4>;

	ComparatorIndexedDistinctPayload distinct_;
	CompositeTypes compositeTypes_;
};

class [[nodiscard]] ComparatorIndexedOffsetArrayDWithin {
public:
	ComparatorIndexedOffsetArrayDWithin(size_t offset, const VariantArray&);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		if (arr.len != 2) [[unlikely]] {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		const double* ptr = reinterpret_cast<const double*>(item.Ptr() + arr.offset);
		return DWithin(Point{ptr[0], ptr[1]}, point_, distance_);
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	Point point_;
	double distance_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedOffsetArrayDWithinDistinct {
public:
	ComparatorIndexedOffsetArrayDWithinDistinct(size_t offset, const VariantArray&);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		if (arr.len != 2) [[unlikely]] {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		const double* ptr = reinterpret_cast<const double*>(item.Ptr() + arr.offset);
		const Point p{ptr[0], ptr[1]};
		return DWithin(p, point_, distance_) && distinct_.Compare(p);
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		if (arr.len != 2) [[unlikely]] {
			return;
		}
		const double* ptr = reinterpret_cast<const double*>(item.Ptr() + arr.offset);
		distinct_.ExcludeValues(Point{ptr[0], ptr[1]});
	}

private:
	ComparatorIndexedDistinct<Point, fast_hash_set_l<Point>> distinct_;
	Point point_;
	double distance_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedJsonPathDWithin {
public:
	ComparatorIndexedJsonPathDWithin(const FieldsSet& fields, const PayloadType& payloadType, const VariantArray&);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		VariantArray buffer;
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer, KeyValueType::Double{});
		if (buffer.size() != 2) [[unlikely]] {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		return DWithin(Point{buffer[0].As<double>(), buffer[1].As<double>()}, point_, distance_);
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	PayloadType payloadType_;
	TagsPath tagsPath_;
	Point point_;
	double distance_;
};

class [[nodiscard]] ComparatorIndexedJsonPathDWithinDistinct {
public:
	ComparatorIndexedJsonPathDWithinDistinct(const FieldsSet& fields, const PayloadType& payloadType, const VariantArray&);
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		VariantArray buffer;
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer, KeyValueType::Double{});
		if (buffer.size() != 2) [[unlikely]] {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		const Point p{buffer[0].As<double>(), buffer[1].As<double>()};
		return DWithin(p, point_, distance_) && distinct_.Compare(p);
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		VariantArray buffer;
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer, KeyValueType::Double{});
		if (buffer.size() != 2) [[unlikely]] {
			return;
		}
		distinct_.ExcludeValues(Point{buffer[0].As<double>(), buffer[1].As<double>()});
	}

private:
	ComparatorIndexedDistinct<Point, fast_hash_set_l<Point>> distinct_;
	PayloadType payloadType_;
	TagsPath tagsPath_;
	Point point_;
	double distance_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedOffsetScalarAnyDistinct {
public:
	ComparatorIndexedOffsetScalarAnyDistinct(size_t offset) noexcept : offset_{offset} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		return distinct_.Compare(*reinterpret_cast<T*>(item.Ptr() + offset_));
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(*reinterpret_cast<T*>(item.Ptr() + offset_));
	}

private:
	ComparatorIndexedDistinct<T> distinct_;
	size_t offset_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedColumnScalarAnyDistinct {
public:
	ComparatorIndexedColumnScalarAnyDistinct(const void* rawData) noexcept : rawData_{static_cast<const T*>(rawData)} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const noexcept {
		return distinct_.Compare(*(rawData_ + rowId));
	}
	static double CostMultiplier() noexcept { return comparators::kIdxColumnComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType rowId) { distinct_.ExcludeValues(*(rawData_ + rowId)); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	const T* rawData_;
};

class [[nodiscard]] ComparatorIndexedOffsetScalarAnyStringDistinct {
public:
	ComparatorIndexedOffsetScalarAnyStringDistinct(size_t offset, const CollateOpts& collate) noexcept
		: distinct_{collate}, offset_{offset} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		return distinct_.Compare(*reinterpret_cast<p_string*>(item.Ptr() + offset_));
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(std::string_view(*reinterpret_cast<p_string*>(item.Ptr() + offset_)));
	}

private:
	ComparatorIndexedDistinctString distinct_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedOffsetArrayAny {
public:
	ComparatorIndexedOffsetArrayAny(size_t offset) noexcept : offset_{offset} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		return arr.len != 0;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	size_t offset_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedOffsetArrayAnyDistinct {
public:
	ComparatorIndexedOffsetArrayAnyDistinct(size_t offset) noexcept : offset_{offset} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			if (distinct_.Compare(*ptr)) {
				return true;
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(*ptr);
		}
	}

private:
	using ComparatorDistinctType = std::conditional_t<std::is_same_v<T, Point>, ComparatorIndexedDistinct<Point, fast_hash_set_l<Point>>,
													  ComparatorIndexedDistinct<T>>;

	ComparatorDistinctType distinct_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedOffsetArrayAnyStringDistinct {
public:
	ComparatorIndexedOffsetArrayAnyStringDistinct(size_t offset, const CollateOpts& collate) noexcept
		: distinct_{collate}, offset_{offset} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			if (distinct_.Compare(*ptr)) {
				return true;
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const p_string* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(std::string_view(*ptr));
		}
	}

private:
	ComparatorIndexedDistinctString distinct_;
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedJsonPathAny {
public:
	ComparatorIndexedJsonPathAny(const TagsPath& tagsPath, const PayloadType& payloadType)
		: tagsPath_{tagsPath}, payloadType_{payloadType} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::Undefined{});
		for (Variant& value : buffer_) {
			if (!value.IsNullValue()) {
				return true;
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

template <typename T>
class [[nodiscard]] ComparatorIndexedJsonPathAnyDistinct {
public:
	ComparatorIndexedJsonPathAnyDistinct(const TagsPath& tagsPath, const PayloadType& payloadType)
		: tagsPath_{tagsPath}, payloadType_{payloadType} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& value : buffer_) {
			if (!value.IsNullValue() && distinct_.Compare(value.As<T>())) {
				return true;
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& value : buffer_) {
			if (!value.IsNullValue()) {
				distinct_.ExcludeValues(value.As<T>());
			}
		}
	}

private:
	ComparatorIndexedDistinct<T> distinct_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class [[nodiscard]] ComparatorIndexedJsonPathAnyStringDistinct {
public:
	ComparatorIndexedJsonPathAnyStringDistinct(const TagsPath& tagsPath, const PayloadType& payloadType, const CollateOpts& collate)
		: distinct_{collate}, tagsPath_{tagsPath}, payloadType_{payloadType} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& value : buffer_) {
			if (!value.IsNullValue() && distinct_.Compare(std::string_view{value})) {
				return true;
			}
		}
		return false;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& value : buffer_) {
			if (!value.IsNullValue()) {
				distinct_.ExcludeValues(std::string_view(value));  // key_string{value});
			}
		}
	}

private:
	ComparatorIndexedDistinctString distinct_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class [[nodiscard]] ComparatorIndexedOffsetArrayEmpty {
public:
	ComparatorIndexedOffsetArrayEmpty(size_t offset) noexcept : offset_{offset} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		return arr.len == 0;
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	size_t offset_;
};

class [[nodiscard]] ComparatorIndexedJsonPathEmpty {
public:
	ComparatorIndexedJsonPathEmpty(const TagsPath& tagsPath, const PayloadType& payloadType)
		: tagsPath_{tagsPath}, payloadType_{payloadType} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::Undefined{});
		if (buffer_.IsObjectValue()) [[unlikely]] {
			return false;
		}
		for (Variant& value : buffer_) {
			if (value.IsNullValue()) {
				return true;
			}
		}
		return buffer_.empty();
	}
	static double CostMultiplier() noexcept { return comparators::kIdxJsonPathComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class [[nodiscard]] ComparatorIndexedFloatVectorAny {
public:
	ComparatorIndexedFloatVectorAny(size_t offset) noexcept : offset_{offset} {}
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType) noexcept {
		return !ConstFloatVectorView::FromUint64(*reinterpret_cast<const uint64_t*>(item.Ptr() + offset_)).IsEmpty();
	}
	static double CostMultiplier() noexcept { return comparators::kIdxOffsetComparatorCostMultiplier; }
	std::string ConditionStr() const;
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType) const noexcept {}

private:
	size_t offset_{0};
};

template <typename T>
struct [[nodiscard]] ComparatorIndexedVariantHelper {
	using type = std::variant<
		ComparatorIndexedColumnScalar<T>, ComparatorIndexedOffsetScalar<T>, ComparatorIndexedOffsetArray<T>,
		ComparatorIndexedOffsetArrayAny, ComparatorIndexedOffsetArrayEmpty, ComparatorIndexedJsonPath<T>, ComparatorIndexedJsonPathAny,
		ComparatorIndexedJsonPathEmpty, ComparatorIndexedOffsetScalarDistinct<T>, ComparatorIndexedColumnScalarDistinct<T>,
		ComparatorIndexedOffsetScalarAnyDistinct<T>, ComparatorIndexedColumnScalarAnyDistinct<T>, ComparatorIndexedOffsetArrayDistinct<T>,
		ComparatorIndexedOffsetArrayAnyDistinct<T>, ComparatorIndexedJsonPathDistinct<T>, ComparatorIndexedJsonPathAnyDistinct<T>>;
};

template <>
struct [[nodiscard]] ComparatorIndexedVariantHelper<key_string> {
	using type = std::variant<ComparatorIndexedColumnScalarString, ComparatorIndexedOffsetScalarString, ComparatorIndexedOffsetArrayString,
							  ComparatorIndexedOffsetArrayAny, ComparatorIndexedOffsetArrayEmpty, ComparatorIndexedJsonPathString,
							  ComparatorIndexedJsonPathAny, ComparatorIndexedJsonPathEmpty, ComparatorIndexedColumnScalarStringDistinct,
							  ComparatorIndexedOffsetScalarStringDistinct, ComparatorIndexedOffsetScalarAnyStringDistinct,
							  ComparatorIndexedOffsetArrayStringDistinct, ComparatorIndexedOffsetArrayAnyStringDistinct,
							  ComparatorIndexedJsonPathStringDistinct, ComparatorIndexedJsonPathAnyStringDistinct>;
};

template <>
struct [[nodiscard]] ComparatorIndexedVariantHelper<PayloadValue> {
	using type = std::variant<ComparatorIndexedComposite, ComparatorIndexedCompositeDistinct>;
};

template <>
struct [[nodiscard]] ComparatorIndexedVariantHelper<Point> {
	using type =
		std::variant<ComparatorIndexedOffsetArrayDWithin, ComparatorIndexedJsonPathDWithin, ComparatorIndexedOffsetArrayAnyDistinct<Point>,
					 ComparatorIndexedOffsetArrayDWithinDistinct, ComparatorIndexedJsonPathDWithinDistinct>;
};

template <>
struct [[nodiscard]] ComparatorIndexedVariantHelper<FloatVector> {
	using type = ComparatorIndexedFloatVectorAny;
};

template <typename T>
using ComparatorIndexedVariant = typename ComparatorIndexedVariantHelper<T>::type;

}  // namespace comparators

template <typename T>
class [[nodiscard]] ComparatorIndexed {
public:
	ComparatorIndexed(std::string_view indexName, CondType cond, const VariantArray& values, const void* rawData, IsArray isArray,
					  reindexer::IsDistinct distinct, const PayloadType& payloadType, const FieldsSet& fields,
					  const CollateOpts& collateOpts = CollateOpts())
		: impl_{createImpl(cond, values, rawData, distinct, isArray, payloadType, fields, collateOpts)}, indexName_{indexName} {}

	std::string_view Name() const noexcept { return indexName_; }
	std::string ConditionStr() const;
	std::string Dump() const { return std::string{Name()} + ' ' + ConditionStr(); }
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	double Cost(double expectedIterations) const noexcept {
		const auto val = expectedIterations * costMultiplier();
		return val + 1.0 + (isNotOperation_ ? val : 0.0);
	}
	void SetNotOperationFlag(bool isNotOperation) noexcept { isNotOperation_ = isNotOperation; }

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType rowId) {
		static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<T>> == 16);
		bool res;
		++totalCalls_;
		// Duplicated pattern for 'matchedCount_ + return' here gives visible performance boost in comparators benchmarks on Centos7.
		// This is probably related to code cache locality
		switch (impl_.index()) {
			case 0:
				res = std::get_if<0>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 1:
				res = std::get_if<1>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 2:
				res = std::get_if<2>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 3:
				res = std::get_if<3>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 4:
				res = std::get_if<4>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 5:
				res = std::get_if<5>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 6:
				res = std::get_if<6>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 7:
				res = std::get_if<7>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 8:
				res = std::get_if<8>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 9:
				res = std::get_if<9>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 10:
				res = std::get_if<10>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 11:
				res = std::get_if<11>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 12:
				res = std::get_if<12>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 13:
				res = std::get_if<13>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 14:
				res = std::get_if<14>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			case 15:
				res = std::get_if<15>(&impl_)->Compare(item, rowId);
				matchedCount_ += res;
				return res;
			default:
				abort();
		}
	}
	void ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
		std::visit([&item, rowId](auto& impl) { impl.ExcludeDistinctValues(item, rowId); }, impl_);
	}
	reindexer::IsDistinct IsDistinct() const noexcept {
		return std::visit([](auto& impl) { return impl.IsDistinct(); }, impl_);
	}
	bool IsIndexed() const noexcept { return true; }

private:
	static comparators::ComparatorIndexedVariant<T> createImpl(CondType cond, const VariantArray&, const void* rawData,
															   reindexer::IsDistinct distinct, IsArray isArray, const PayloadType&,
															   const FieldsSet&, const CollateOpts&);
	double costMultiplier() const noexcept {
		return std::visit([](auto& impl) { return impl.CostMultiplier(); }, impl_);
	}

	int totalCalls_{0};
	int matchedCount_{0};
	comparators::ComparatorIndexedVariant<T> impl_;
	std::string_view indexName_;
	bool isNotOperation_{false};
};

template <>
RX_ALWAYS_INLINE bool ComparatorIndexed<key_string>::Compare(const PayloadValue& item, IdType rowId) {
	static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<key_string>> == 15);
	bool res;
	++totalCalls_;
	switch (impl_.index()) {
		case 0:
			res = std::get_if<0>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 1:
			res = std::get_if<1>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 2:
			res = std::get_if<2>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 3:
			res = std::get_if<3>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 4:
			res = std::get_if<4>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 5:
			res = std::get_if<5>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 6:
			res = std::get_if<6>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 7:
			res = std::get_if<7>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 8:
			res = std::get_if<8>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 9:
			res = std::get_if<9>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 10:
			res = std::get_if<10>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 11:
			res = std::get_if<11>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 12:
			res = std::get_if<12>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 13:
			res = std::get_if<13>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 14:
			res = std::get_if<14>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		default:
			abort();
	}
}

template <>
RX_ALWAYS_INLINE bool ComparatorIndexed<PayloadValue>::Compare(const PayloadValue& item, IdType rowId) {
	static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<PayloadValue>> == 2);
	++totalCalls_;
	bool res{false};
	switch (impl_.index()) {
		case 0:
			res = std::get_if<0>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 1:
			res = std::get_if<1>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		default:
			abort();
	}
	return res;
}

template <>
RX_ALWAYS_INLINE void ComparatorIndexed<PayloadValue>::ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
	std::visit([&item, rowId](auto& impl) { impl.ExcludeDistinctValues(item, rowId); }, impl_);
}

template <>
RX_ALWAYS_INLINE std::string ComparatorIndexed<PayloadValue>::ConditionStr() const {
	return std::visit([](const auto& impl) { return impl.ConditionStr(); }, impl_);
}

template <>
RX_ALWAYS_INLINE reindexer::IsDistinct ComparatorIndexed<PayloadValue>::IsDistinct() const noexcept {
	return std::visit([](const auto& impl) { return impl.IsDistinct(); }, impl_);
}
template <>
RX_ALWAYS_INLINE double ComparatorIndexed<PayloadValue>::costMultiplier() const noexcept {
	return std::visit([](const auto& impl) { return impl.CostMultiplier(); }, impl_);
}

template <>
RX_ALWAYS_INLINE bool ComparatorIndexed<Point>::Compare(const PayloadValue& item, IdType rowId) {
	static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<Point>> == 5);
	bool res;
	++totalCalls_;
	switch (impl_.index()) {
		case 0:
			res = std::get_if<0>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 1:
			res = std::get_if<1>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 2:
			res = std::get_if<2>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 3:
			res = std::get_if<3>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		case 4:
			res = std::get_if<4>(&impl_)->Compare(item, rowId);
			matchedCount_ += res;
			return res;
		default:
			abort();
	}
}

template <>
RX_ALWAYS_INLINE bool ComparatorIndexed<FloatVector>::Compare(const PayloadValue& item, IdType rowId) {
	++totalCalls_;
	const bool res = impl_.Compare(item, rowId);
	matchedCount_ += res;
	return res;
}
template <>
RX_ALWAYS_INLINE void ComparatorIndexed<FloatVector>::ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
	impl_.ExcludeDistinctValues(item, rowId);
}
template <>
RX_ALWAYS_INLINE IsDistinct ComparatorIndexed<FloatVector>::IsDistinct() const noexcept {
	return impl_.IsDistinct();
}
template <>
RX_ALWAYS_INLINE double ComparatorIndexed<FloatVector>::costMultiplier() const noexcept {
	return impl_.CostMultiplier();
}

extern template std::string ComparatorIndexed<int>::ConditionStr() const;
extern template std::string ComparatorIndexed<int64_t>::ConditionStr() const;
extern template std::string ComparatorIndexed<bool>::ConditionStr() const;
extern template std::string ComparatorIndexed<double>::ConditionStr() const;
extern template std::string ComparatorIndexed<key_string>::ConditionStr() const;
extern template std::string ComparatorIndexed<Point>::ConditionStr() const;
extern template std::string ComparatorIndexed<Uuid>::ConditionStr() const;

extern template comparators::ComparatorIndexedVariant<int> ComparatorIndexed<int>::createImpl(CondType, const VariantArray&, const void*,
																							  reindexer::IsDistinct, IsArray,
																							  const PayloadType&, const FieldsSet&,
																							  const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<int64_t> ComparatorIndexed<int64_t>::createImpl(CondType, const VariantArray&,
																									  const void*, reindexer::IsDistinct,
																									  IsArray, const PayloadType&,
																									  const FieldsSet&, const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<double> ComparatorIndexed<double>::createImpl(CondType, const VariantArray&,
																									const void*, reindexer::IsDistinct,
																									IsArray, const PayloadType&,
																									const FieldsSet&, const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<bool> ComparatorIndexed<bool>::createImpl(CondType, const VariantArray&, const void*,
																								reindexer::IsDistinct, IsArray,
																								const PayloadType&, const FieldsSet&,
																								const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<Uuid> ComparatorIndexed<Uuid>::createImpl(CondType, const VariantArray&, const void*,
																								reindexer::IsDistinct, IsArray,
																								const PayloadType&, const FieldsSet&,
																								const CollateOpts&);
template <>
comparators::ComparatorIndexedVariant<key_string> ComparatorIndexed<key_string>::createImpl(CondType, const VariantArray&, const void*,
																							reindexer::IsDistinct, IsArray,
																							const PayloadType&, const FieldsSet&,
																							const CollateOpts&);
template <>
comparators::ComparatorIndexedVariant<Point> ComparatorIndexed<Point>::createImpl(CondType, const VariantArray&, const void*,
																				  reindexer::IsDistinct, IsArray, const PayloadType&,
																				  const FieldsSet&, const CollateOpts&);
template <>
comparators::ComparatorIndexedVariant<PayloadValue> ComparatorIndexed<PayloadValue>::createImpl(CondType, const VariantArray&, const void*,
																								reindexer::IsDistinct, IsArray,
																								const PayloadType&, const FieldsSet&,
																								const CollateOpts&);

template <>
comparators::ComparatorIndexedVariant<FloatVector> ComparatorIndexed<FloatVector>::createImpl(CondType, const VariantArray&, const void*,
																							  reindexer::IsDistinct, IsArray,
																							  const PayloadType&, const FieldsSet&,
																							  const CollateOpts&);

}  // namespace reindexer
