#pragma once

#include <unordered_map>
#include <unordered_set>
#include <variant>

#include "comparator_indexed_distinct.h"
#include "core/index/payload_map.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/variant.h"
#include "core/payload/payloadfieldvalue.h"
#include "core/payload/payloadtype.h"
#include "core/payload/payloadvalue.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"
#include "helpers.h"
#include "tools/string_regexp_functions.h"
#include "vendor/sparse-map/sparse_set.h"

namespace reindexer {

namespace comparators {

template <typename T, CondType Cond>
struct ValuesHolder {
	using Type = T;
};

template <CondType Cond>
struct ValuesHolder<key_string, Cond> {
	struct Type {
		Type() noexcept = default;
		Type(key_string v) noexcept : value_{std::move(v)}, valueView_{value_ ? std::string_view{*value_} : std::string_view{}} {}
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

// TODO: fast_hash_map here somehow causes crashes in the centos 7 asan CI builds
template <typename T, typename U, typename HashT>
using LowSparsityMap = tsl::sparse_map<T, U, HashT, std::equal_to<T>, std::allocator<std::pair<T, U>>,
									   tsl::sh::power_of_two_growth_policy<2>, tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

template <typename T, typename HashT>
using LowSparsitySet = tsl::sparse_set<T, HashT, std::equal_to<T>, std::allocator<T>, tsl::sh::power_of_two_growth_policy<2>,
									   tsl::sh::exception_safety::basic, tsl::sh::sparsity::low>;

template <typename T>
struct ValuesHolder<T, CondRange> {
	using Type = std::pair<T, T>;
};

template <>
struct ValuesHolder<key_string, CondRange> {
	struct Type {
		ValuesHolder<key_string, CondEq>::Type value1_;
		ValuesHolder<key_string, CondEq>::Type value2_;
	};
};

template <>
struct ValuesHolder<int, CondSet> {
	using Type = LowSparsitySet<int, hash_int<int>>;
};

template <>
struct ValuesHolder<int64_t, CondSet> {
	using Type = LowSparsitySet<int64_t, hash_int<int64_t>>;
};

template <typename T>
struct ValuesHolder<T, CondSet> {
	using Type = LowSparsitySet<T, std::hash<T>>;
};

template <>
struct ValuesHolder<key_string, CondSet> {
	using Type = key_string_set;
};

template <>
struct ValuesHolder<PayloadValue, CondSet> {
	using Type = unordered_payload_ref_set;
};

template <>
struct ValuesHolder<int, CondAllSet> {
	struct Type {
		LowSparsityMap<int, int, hash_int<int>> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <>
struct ValuesHolder<int64_t, CondAllSet> {
	struct Type {
		LowSparsityMap<int64_t, int, hash_int<int64_t>> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <typename T>
struct ValuesHolder<T, CondAllSet> {
	struct Type {
		LowSparsityMap<T, int, std::hash<T>> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <>
struct ValuesHolder<key_string, CondAllSet> {
	struct Type {
		key_string_map<int> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <>
struct ValuesHolder<PayloadValue, CondAllSet> {
	struct Type {
		unordered_payload_map<int, false> values_;
		fast_hash_set<int> allSetValues_;
	};
};

template <typename T>
struct DataHolder {
	using SingleType = typename ValuesHolder<T, CondEq>::Type;
	using RangeType = typename ValuesHolder<T, CondRange>::Type;
	using SetType = typename ValuesHolder<T, CondSet>::Type;
	using SetWrpType = intrusive_rc_wrapper<SetType>;
	using SetPtrType = intrusive_ptr<SetWrpType>;
	using AllSetType = typename ValuesHolder<T, CondAllSet>::Type;
	using AllSetWrpType = intrusive_rc_wrapper<AllSetType>;
	using AllSetPtrType = intrusive_ptr<AllSetWrpType>;

	DataHolder() noexcept : cond_{CondEq}, value_{} {}
	DataHolder(DataHolder&& other) noexcept : cond_{other.cond_} {
		switch (other.cond_) {
			case CondEq:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondLike:
				new (&this->value_) SingleType{std::move(other.value_)};
				break;
			case CondRange:
				new (&this->range_) RangeType{std::move(other.range_)};
				break;
			case CondSet:
				new (&this->setPtr_) SetPtrType{std::move(other.setPtr_)};
				break;
			case CondAllSet:
				new (&this->allSetPtr_) AllSetPtrType{std::move(other.allSetPtr_)};
				break;
			case CondAny:
			case CondEmpty:
			case CondDWithin:
				new (&this->value_) SingleType{};
				break;
		}
	}
	DataHolder(const DataHolder& other) : cond_{CondAny} {
		switch (other.cond_) {
			case CondEq:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondLike:
				new (&this->value_) SingleType{other.value_};
				break;
			case CondRange:
				new (&this->range_) RangeType{other.range_};
				break;
			case CondSet:
				new (&this->setPtr_) SetPtrType{other.setPtr_};
				break;
			case CondAllSet:
				new (&this->allSetPtr_) AllSetPtrType{other.allSetPtr_};
				break;
			case CondAny:
			case CondEmpty:
			case CondDWithin:
				new (&this->value_) SingleType{};
				break;
		}
		cond_ = other.cond_;
	}
	DataHolder& operator=(DataHolder&& other) noexcept {
		if (this == &other) {
			return *this;
		}
		if (cond_ != other.cond_) {
			clear();
			switch (other.cond_) {
				case CondEq:
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondLike:
					new (&this->value_) SingleType{std::move(other.value_)};
					break;
				case CondRange:
					new (&this->range_) RangeType{std::move(other.range_)};
					break;
				case CondSet:
					new (&this->setPtr_) SetPtrType{std::move(other.setPtr_)};
					break;
				case CondAllSet:
					new (&this->allSetPtr_) AllSetPtrType{std::move(other.allSetPtr_)};
					break;
				case CondAny:
				case CondEmpty:
				case CondDWithin:
					break;
			}
			this->cond_ = other.cond_;
		} else {
			switch (other.cond_) {
				case CondEq:
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondLike:
					this->value_ = std::move(other.value_);
					break;
				case CondRange:
					this->range_ = std::move(other.range_);
					break;
				case CondSet:
					this->setPtr_ = std::move(other.setPtr_);
					break;
				case CondAllSet:
					this->allSetPtr_ = std::move(other.allSetPtr_);
					break;
				case CondAny:
				case CondEmpty:
				case CondDWithin:
					break;
			}
		}
		return *this;
	}
	DataHolder& operator=(const DataHolder& other) {
		if (this == &other) {
			return *this;
		}
		if (cond_ != other.cond_) {
			clear();
			switch (other.cond_) {
				case CondEq:
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondLike:
					new (&this->value_) SingleType{other.value_};
					break;
				case CondRange:
					new (&this->range_) RangeType{other.range_};
					break;
				case CondSet:
					new (&this->setPtr_) SetPtrType{other.setPtr_};
					break;
				case CondAllSet:
					new (&this->allSetPtr_) AllSetPtrType{other.allSetPtr_};
					break;
				case CondAny:
				case CondEmpty:
				case CondDWithin:
					break;
			}
			this->cond_ = other.cond_;
		} else {
			DataHolder tmp{other};
			*this = std::move(tmp);
		}
		return *this;
	}
	~DataHolder() { clear(); }
	void clear() noexcept {
		switch (cond_) {
			case CondEq:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondLike:
				value_.~SingleType();
				break;
			case CondRange:
				range_.~RangeType();
				break;
			case CondSet:
				setPtr_.~SetPtrType();
				break;
			case CondAllSet:
				allSetPtr_.~AllSetPtrType();
				break;
			case CondDWithin:
			case CondAny:
			case CondEmpty:
			default:
				break;
		}
		cond_ = CondAny;
	}
	CondType cond_;
	union {
		SingleType value_;
		RangeType range_;
		SetPtrType setPtr_;
		AllSetPtrType allSetPtr_;
	};
};

template <typename T>
class ComparatorIndexedOffsetScalar : private DataHolder<T> {
public:
	ComparatorIndexedOffsetScalar(size_t offset, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		const T* ptr = reinterpret_cast<T*>(item.Ptr() + offset_);
		switch (this->cond_) {
			case CondEq:
				return *ptr == this->value_;
			case CondLt:
				return *ptr < this->value_;
			case CondLe:
				return *ptr <= this->value_;
			case CondGt:
				return *ptr > this->value_;
			case CondGe:
				return *ptr >= this->value_;
			case CondRange:
				return this->range_.first <= *ptr && *ptr <= this->range_.second;
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	size_t offset_;
};

template <typename T>
class ComparatorIndexedColumnScalar : private DataHolder<T> {
public:
	ComparatorIndexedColumnScalar(const void* rawData, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const noexcept {
		const T& v = *(rawData_ + rowId);
		switch (this->cond_) {
			case CondEq:
				return v == this->value_;
			case CondLt:
				return v < this->value_;
			case CondLe:
				return v <= this->value_;
			case CondGt:
				return v > this->value_;
			case CondGe:
				return v >= this->value_;
			case CondRange:
				return this->range_.first <= v && v <= this->range_.second;
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	const T* rawData_;
};

template <typename T>
class ComparatorIndexedOffsetScalarDistinct : private DataHolder<T> {
public:
	ComparatorIndexedOffsetScalarDistinct(size_t offset, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		const T& value = *reinterpret_cast<T*>(item.Ptr() + offset_);
		switch (this->cond_) {
			case CondEq:
				return value == this->value_ && distinct_.Compare(value);
			case CondLt:
				return value < this->value_ && distinct_.Compare(value);
			case CondLe:
				return value <= this->value_ && distinct_.Compare(value);
			case CondGt:
				return value > this->value_ && distinct_.Compare(value);
			case CondGe:
				return value >= this->value_ && distinct_.Compare(value);
			case CondRange:
				return this->range_.first <= value && value <= this->range_.second && distinct_.Compare(value);
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(*reinterpret_cast<T*>(item.Ptr() + offset_));
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	size_t offset_;
};

template <typename T>
class ComparatorIndexedColumnScalarDistinct : private DataHolder<T> {
public:
	ComparatorIndexedColumnScalarDistinct(const void* rawData, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const noexcept {
		const T& value = *(rawData_ + rowId);
		switch (this->cond_) {
			case CondEq:
				return value == this->value_ && distinct_.Compare(value);
			case CondLt:
				return value < this->value_ && distinct_.Compare(value);
			case CondLe:
				return value <= this->value_ && distinct_.Compare(value);
			case CondGt:
				return value > this->value_ && distinct_.Compare(value);
			case CondGe:
				return value >= this->value_ && distinct_.Compare(value);
			case CondRange:
				return this->range_.first <= value && value <= this->range_.second && distinct_.Compare(value);
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType rowId) { distinct_.ExcludeValues(*(rawData_ + rowId)); }
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	const T* rawData_;
};

template <typename T>
class ComparatorIndexedOffsetArray : private DataHolder<T> {
public:
	ComparatorIndexedOffsetArray(size_t offset, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			switch (this->cond_) {
				case CondEq:
					if (*ptr == this->value_) {
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
					if (this->range_.first <= *ptr && *ptr <= this->range_.second) {
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	size_t offset_;
};

template <typename T>
class ComparatorIndexedOffsetArrayDistinct : private DataHolder<T> {
public:
	ComparatorIndexedOffsetArrayDistinct(size_t offset, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			switch (this->cond_) {
				case CondEq:
					if (*ptr == this->value_ && distinct_.Compare(*ptr)) {
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
					if (this->range_.first <= *ptr && *ptr <= this->range_.second && distinct_.Compare(*ptr)) {
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(*ptr);
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	size_t offset_;
};

template <typename T>
class ComparatorIndexedJsonPath : private DataHolder<T> {
	using Base = DataHolder<T>;
	using SingleType = typename Base::SingleType;

public:
	ComparatorIndexedJsonPath(const TagsPath& tagsPath, const PayloadType& payloadType, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& value : buffer_) {
			if rx_unlikely (value.IsNullValue()) {
				continue;
			}
			switch (this->cond_) {
				case CondEq:
					if (value.As<T>() == this->value_) {
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
					if (this->range_.first <= v && v <= this->range_.second) {
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

template <typename T>
class ComparatorIndexedJsonPathDistinct : private DataHolder<T> {
	using Base = DataHolder<T>;
	using SingleType = typename Base::SingleType;

public:
	ComparatorIndexedJsonPathDistinct(const TagsPath& tagsPath, const PayloadType& payloadType, const VariantArray&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (this->cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			this->allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& v : buffer_) {
			if rx_unlikely (v.IsNullValue()) {
				continue;
			}
			const auto value = v.As<T>();
			switch (this->cond_) {
				case CondEq:
					if (value == this->value_ && distinct_.Compare(value)) {
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
				case CondRange: {
					const auto v = value;
					if (this->range_.first <= v && v <= this->range_.second && distinct_.Compare(value)) {
						return true;
					}
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& v : buffer_) {
			if rx_unlikely (v.IsNullValue()) {
				continue;
			}
			distinct_.ExcludeValues(v.As<T>());
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class ComparatorIndexedOffsetScalarString : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetScalarString(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const std::string_view value = *reinterpret_cast<const p_string*>(item.Ptr() + offset_);
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend();
			case CondRange:
				return (collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le);
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	const CollateOpts* collateOpts_;
	size_t offset_;
};

// There are no column rawData for the string indexes
class ComparatorIndexedColumnScalarString : private DataHolder<key_string> {
public:
	ComparatorIndexedColumnScalarString(const void* rawData, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const {
		const std::string_view value(*(rawData_ + rowId));
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend();
			case CondRange:
				return (collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le);
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	const CollateOpts* collateOpts_;
	const std::string_view* rawData_;
};

class ComparatorIndexedOffsetScalarStringDistinct : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetScalarStringDistinct(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const std::string_view value = *reinterpret_cast<const p_string*>(item.Ptr() + offset_);
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend() && distinct_.Compare(value);
			case CondRange:
				return (collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le) &&
					   distinct_.Compare(value);
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(std::string_view(*reinterpret_cast<const p_string*>(item.Ptr() + offset_)));
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	size_t offset_;
};

class ComparatorIndexedColumnScalarStringDistinct : private DataHolder<key_string> {
public:
	ComparatorIndexedColumnScalarStringDistinct(const void* rawData, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const {
		const std::string_view value(*(rawData_ + rowId));
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(value) != setPtr_->cend() && distinct_.Compare(value);
			case CondRange:
				return (collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
					   (collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le) &&
					   distinct_.Compare(value);
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
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType rowId) { distinct_.ExcludeValues(*(rawData_ + rowId)); }
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	const std::string_view* rawData_;
};

class ComparatorIndexedOffsetArrayString : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetArrayString(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			allSetPtr_->allSetValues_.clear();
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
					if ((collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le)) {
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	const CollateOpts* collateOpts_;
	size_t offset_;
};

class ComparatorIndexedOffsetArrayStringDistinct : private DataHolder<key_string> {
public:
	ComparatorIndexedOffsetArrayStringDistinct(size_t offset, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			allSetPtr_->allSetValues_.clear();
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
					if ((collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le) &&
						distinct_.Compare(value)) {
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const p_string* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(std::string_view(*ptr));
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	size_t offset_;
};

class ComparatorIndexedJsonPathString : private DataHolder<key_string> {
	using Base = DataHolder<key_string>;

public:
	ComparatorIndexedJsonPathString(const TagsPath&, const PayloadType&, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& v : buffer_) {
			const std::string_view value = static_cast<std::string_view>(v);
			switch (cond_) {
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (setPtr_->find(value) != setPtr_->cend()) {
						return true;
					}
					break;
				case CondRange:
					if ((collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le)) {
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	const CollateOpts* collateOpts_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class ComparatorIndexedJsonPathStringDistinct : private DataHolder<key_string> {
	using Base = DataHolder<key_string>;

public:
	ComparatorIndexedJsonPathStringDistinct(const TagsPath&, const PayloadType&, const VariantArray&, const CollateOpts&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		if (cond_ == CondAllSet) {
			assertrx_dbg(this->allSetPtr_);
			allSetPtr_->allSetValues_.clear();
		}
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& v : buffer_) {
			const std::string_view value = static_cast<std::string_view>(v);
			switch (cond_) {
				case CondSet:
					assertrx_dbg(this->setPtr_);
					if (setPtr_->find(value) != setPtr_->cend() && distinct_.Compare(value)) {
						return true;
					}
					break;
				case CondRange:
					if ((collateCompare(value, range_.value1_.valueView_, *collateOpts_) & ComparationResult::Ge) &&
						(collateCompare(value, range_.value2_.valueView_, *collateOpts_) & ComparationResult::Le) &&
						distinct_.Compare(value)) {
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
				default:
					abort();
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& v : buffer_) {
			distinct_.ExcludeValues(std::string_view(v));  // key_string{v});
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinctString distinct_;
	const CollateOpts* collateOpts_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class ComparatorIndexedComposite : private DataHolder<PayloadValue> {
	using Base = DataHolder<PayloadValue>;

public:
	ComparatorIndexedComposite(const VariantArray&, const CollateOpts&, const FieldsSet&, const PayloadType&, CondType);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		switch (cond_) {
			case CondSet:
				assertrx_dbg(this->setPtr_);
				return setPtr_->find(item) != setPtr_->cend();
			case CondRange: {
				ConstPayload pv{payloadType_, item};
				return (pv.Compare<WithString::Yes, NotComparable::Throw>(range_.first, fields_, *collateOpts_) & ComparationResult::Ge) &&
					   (pv.Compare<WithString::Yes, NotComparable::Throw>(range_.second, fields_, *collateOpts_) & ComparationResult::Le);
			}
			case CondAllSet:
				assertrx_dbg(this->allSetPtr_);
				return allSetPtr_->values_.size() == 1 && allSetPtr_->values_.find(item) != allSetPtr_->values_.end();
			case CondEq:
				return ConstPayload{payloadType_, item}.Compare<WithString::Yes, NotComparable::Throw>(value_, fields_, *collateOpts_) ==
					   ComparationResult::Eq;
			case CondLt:
				return ConstPayload{payloadType_, item}.Compare<WithString::Yes, NotComparable::Throw>(value_, fields_, *collateOpts_) ==
					   ComparationResult::Lt;
			case CondLe:
				return ConstPayload{payloadType_, item}.Compare<WithString::Yes, NotComparable::Throw>(value_, fields_, *collateOpts_) &
					   ComparationResult::Le;
			case CondGt:
				return ConstPayload{payloadType_, item}.Compare<WithString::Yes, NotComparable::Throw>(value_, fields_, *collateOpts_) ==
					   ComparationResult::Gt;
			case CondGe:
				return ConstPayload{payloadType_, item}.Compare<WithString::Yes, NotComparable::Throw>(value_, fields_, *collateOpts_) &
					   ComparationResult::Ge;
			case CondAny:
			case CondEmpty:
			case CondDWithin:
			case CondLike:
			default:
				abort();
		}
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	const CollateOpts* collateOpts_;
	FieldsSet fields_;
	PayloadType payloadType_;
};

class ComparatorIndexedOffsetArrayDWithin {
public:
	ComparatorIndexedOffsetArrayDWithin(size_t offset, const VariantArray&);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		if rx_unlikely (arr.len != 2) {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		const double* ptr = reinterpret_cast<const double*>(item.Ptr() + arr.offset);
		return DWithin(Point{ptr[0], ptr[1]}, point_, distance_);
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	Point point_;
	double distance_;
	size_t offset_;
};

class ComparatorIndexedOffsetArrayDWithinDistinct {
public:
	ComparatorIndexedOffsetArrayDWithinDistinct(size_t offset, const VariantArray&);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		if rx_unlikely (arr.len != 2) {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		const double* ptr = reinterpret_cast<const double*>(item.Ptr() + arr.offset);
		const Point p{ptr[0], ptr[1]};
		return DWithin(p, point_, distance_) && distinct_.Compare(p);
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		if rx_unlikely (arr.len != 2) {
			return;
		}
		const double* ptr = reinterpret_cast<const double*>(item.Ptr() + arr.offset);
		distinct_.ExcludeValues(Point{ptr[0], ptr[1]});
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<Point> distinct_;
	Point point_;
	double distance_;
	size_t offset_;
};

class ComparatorIndexedJsonPathDWithin {
public:
	ComparatorIndexedJsonPathDWithin(const FieldsSet& fields, const PayloadType& payloadType, const VariantArray&);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::Double{});
		if rx_unlikely (buffer_.size() != 2) {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		return DWithin(Point{buffer_[0].As<double>(), buffer_[1].As<double>()}, point_, distance_);
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	PayloadType payloadType_;
	TagsPath tagsPath_;
	VariantArray buffer_;
	Point point_;
	double distance_;
};

class ComparatorIndexedJsonPathDWithinDistinct {
public:
	ComparatorIndexedJsonPathDWithinDistinct(const FieldsSet& fields, const PayloadType& payloadType, const VariantArray&);
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::Double{});
		if rx_unlikely (buffer_.size() != 2) {
			throw Error(errQueryExec, "DWithin with not point data");
		}
		const Point p{buffer_[0].As<double>(), buffer_[1].As<double>()};
		return DWithin(p, point_, distance_) && distinct_.Compare(p);
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::Double{});
		if rx_unlikely (buffer_.size() != 2) {
			return;
		}
		distinct_.ExcludeValues(Point{buffer_[0].As<double>(), buffer_[1].As<double>()});
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<Point> distinct_;
	PayloadType payloadType_;
	TagsPath tagsPath_;
	VariantArray buffer_;
	Point point_;
	double distance_;
};

template <typename T>
class ComparatorIndexedOffsetScalarAnyDistinct {
public:
	ComparatorIndexedOffsetScalarAnyDistinct(size_t offset) noexcept : offset_{offset} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		return distinct_.Compare(*reinterpret_cast<T*>(item.Ptr() + offset_));
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(*reinterpret_cast<T*>(item.Ptr() + offset_));
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	size_t offset_;
};

template <typename T>
class ComparatorIndexedColumnScalarAnyDistinct {
public:
	ComparatorIndexedColumnScalarAnyDistinct(const void* rawData) noexcept : rawData_{static_cast<const T*>(rawData)} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& /*item*/, IdType rowId) const noexcept {
		return distinct_.Compare(*(rawData_ + rowId));
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& /*item*/, IdType rowId) { distinct_.ExcludeValues(*(rawData_ + rowId)); }
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	const T* rawData_;
};

class ComparatorIndexedOffsetScalarAnyStringDistinct {
public:
	ComparatorIndexedOffsetScalarAnyStringDistinct(size_t offset, const CollateOpts& collate) noexcept
		: distinct_{collate}, offset_{offset} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const noexcept {
		return distinct_.Compare(*reinterpret_cast<p_string*>(item.Ptr() + offset_));
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		distinct_.ExcludeValues(std::string_view(*reinterpret_cast<p_string*>(item.Ptr() + offset_)));
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinctString distinct_;
	size_t offset_;
};

class ComparatorIndexedOffsetArrayAny {
public:
	ComparatorIndexedOffsetArrayAny(size_t offset) noexcept : offset_{offset} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		return arr.len != 0;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	size_t offset_;
};

template <typename T>
class ComparatorIndexedOffsetArrayAnyDistinct {
public:
	ComparatorIndexedOffsetArrayAnyDistinct(size_t offset) noexcept : offset_{offset} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			if (distinct_.Compare(*ptr)) {
				return true;
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const auto* ptr = reinterpret_cast<const T*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(*ptr);
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	size_t offset_;
};

class ComparatorIndexedOffsetArrayAnyStringDistinct {
public:
	ComparatorIndexedOffsetArrayAnyStringDistinct(size_t offset, const CollateOpts& collate) noexcept
		: distinct_{collate}, offset_{offset} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) const {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			if (distinct_.Compare(*ptr)) {
				return true;
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		const p_string* ptr = reinterpret_cast<const p_string*>(item.Ptr() + arr.offset);
		for (const auto* const end = ptr + arr.len; ptr != end; ++ptr) {
			distinct_.ExcludeValues(std::string_view(*ptr));
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinctString distinct_;
	size_t offset_;
};

class ComparatorIndexedJsonPathAny {
public:
	ComparatorIndexedJsonPathAny(const TagsPath& tagsPath, const PayloadType& payloadType)
		: tagsPath_{tagsPath}, payloadType_{payloadType} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::Undefined{});
		for (Variant& value : buffer_) {
			if (!value.IsNullValue()) {
				return true;
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

template <typename T>
class ComparatorIndexedJsonPathAnyDistinct {
public:
	ComparatorIndexedJsonPathAnyDistinct(const TagsPath& tagsPath, const PayloadType& payloadType)
		: tagsPath_{tagsPath}, payloadType_{payloadType} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& value : buffer_) {
			if (!value.IsNullValue() && distinct_.Compare(value.As<T>())) {
				return true;
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::From<T>());
		for (Variant& value : buffer_) {
			if (!value.IsNullValue()) {
				distinct_.ExcludeValues(value.As<T>());
			}
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinct<T> distinct_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class ComparatorIndexedJsonPathAnyStringDistinct {
public:
	ComparatorIndexedJsonPathAnyStringDistinct(const TagsPath& tagsPath, const PayloadType& payloadType, const CollateOpts& collate)
		: distinct_{collate}, tagsPath_{tagsPath}, payloadType_{payloadType} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& value : buffer_) {
			if (!value.IsNullValue() && distinct_.Compare(std::string_view{value})) {
				return true;
			}
		}
		return false;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return true; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::String{});
		for (Variant& value : buffer_) {
			if (!value.IsNullValue()) {
				distinct_.ExcludeValues(std::string_view(value));  // key_string{value});
			}
		}
	}
	void ClearDistinctValues() noexcept { distinct_.ClearValues(); }

private:
	ComparatorIndexedDistinctString distinct_;
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

class ComparatorIndexedOffsetArrayEmpty {
public:
	ComparatorIndexedOffsetArrayEmpty(size_t offset) noexcept : offset_{offset} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		const PayloadFieldValue::Array& arr = *reinterpret_cast<const PayloadFieldValue::Array*>(item.Ptr() + offset_);
		return arr.len == 0;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	size_t offset_;
};

class ComparatorIndexedJsonPathEmpty {
public:
	ComparatorIndexedJsonPathEmpty(const TagsPath& tagsPath, const PayloadType& payloadType)
		: tagsPath_{tagsPath}, payloadType_{payloadType} {}
	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		buffer_.clear<false>();
		ConstPayload(payloadType_, item).GetByJsonPath(tagsPath_, buffer_, KeyValueType::Undefined{});
		for (Variant& value : buffer_) {
			if (!value.IsNullValue()) {
				return false;
			}
		}
		return true;
	}
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] bool IsDistinct() const noexcept { return false; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	void ClearDistinctValues() const noexcept {}

private:
	TagsPath tagsPath_;
	PayloadType payloadType_;
	VariantArray buffer_;
};

template <typename T>
struct ComparatorIndexedVariantHelper {
	using type = std::variant<
		ComparatorIndexedColumnScalar<T>, ComparatorIndexedOffsetScalar<T>, ComparatorIndexedOffsetArray<T>,
		ComparatorIndexedOffsetArrayAny, ComparatorIndexedOffsetArrayEmpty, ComparatorIndexedJsonPath<T>, ComparatorIndexedJsonPathAny,
		ComparatorIndexedJsonPathEmpty, ComparatorIndexedOffsetScalarDistinct<T>, ComparatorIndexedColumnScalarDistinct<T>,
		ComparatorIndexedOffsetScalarAnyDistinct<T>, ComparatorIndexedColumnScalarAnyDistinct<T>, ComparatorIndexedOffsetArrayDistinct<T>,
		ComparatorIndexedOffsetArrayAnyDistinct<T>, ComparatorIndexedJsonPathDistinct<T>, ComparatorIndexedJsonPathAnyDistinct<T>>;
};

template <>
struct ComparatorIndexedVariantHelper<key_string> {
	using type = std::variant<ComparatorIndexedColumnScalarString, ComparatorIndexedOffsetScalarString, ComparatorIndexedOffsetArrayString,
							  ComparatorIndexedOffsetArrayAny, ComparatorIndexedOffsetArrayEmpty, ComparatorIndexedJsonPathString,
							  ComparatorIndexedJsonPathAny, ComparatorIndexedJsonPathEmpty, ComparatorIndexedColumnScalarStringDistinct,
							  ComparatorIndexedOffsetScalarStringDistinct, ComparatorIndexedOffsetScalarAnyStringDistinct,
							  ComparatorIndexedOffsetArrayStringDistinct, ComparatorIndexedOffsetArrayAnyStringDistinct,
							  ComparatorIndexedJsonPathStringDistinct, ComparatorIndexedJsonPathAnyStringDistinct>;
};

template <>
struct ComparatorIndexedVariantHelper<PayloadValue> {
	using type = std::variant<ComparatorIndexedComposite>;
};

template <>
struct ComparatorIndexedVariantHelper<Point> {
	using type =
		std::variant<ComparatorIndexedOffsetArrayDWithin, ComparatorIndexedJsonPathDWithin, ComparatorIndexedOffsetArrayAnyDistinct<Point>,
					 ComparatorIndexedOffsetArrayDWithinDistinct, ComparatorIndexedJsonPathDWithinDistinct>;
};

template <typename T>
using ComparatorIndexedVariant = typename ComparatorIndexedVariantHelper<T>::type;

}  // namespace comparators

template <typename T>
class ComparatorIndexed {
public:
	ComparatorIndexed(std::string_view indexName, CondType cond, const VariantArray& values, const void* rawData, bool isArray,
					  bool distinct, const PayloadType& payloadType, const FieldsSet& fields,
					  const CollateOpts& collateOpts = CollateOpts())
		: impl_{createImpl(cond, values, rawData, distinct, isArray, payloadType, fields, collateOpts)}, indexName_{indexName} {}

	[[nodiscard]] std::string_view Name() const noexcept { return indexName_; }
	[[nodiscard]] std::string ConditionStr() const;
	[[nodiscard]] std::string Dump() const { return std::string{Name()} + ' ' + ConditionStr(); }
	[[nodiscard]] int GetMatchedCount() const noexcept { return matchedCount_; }
	[[nodiscard]] double Cost(double expectedIterations) const noexcept {
		return expectedIterations + 1.0 + (isNotOperation_ ? expectedIterations : 0.0);
	}
	void SetNotOperationFlag(bool isNotOperation) noexcept { isNotOperation_ = isNotOperation; }

	[[nodiscard]] RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType rowId) {
		static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<T>> == 16);
		bool res;
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
	void ClearDistinctValues() noexcept {
		std::visit([](auto& impl) { impl.ClearDistinctValues(); }, impl_);
	}
	void ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
		std::visit([&item, rowId](auto& impl) { impl.ExcludeDistinctValues(item, rowId); }, impl_);
	}
	[[nodiscard]] bool IsDistinct() const noexcept {
		return std::visit([](auto& impl) { return impl.IsDistinct(); }, impl_);
	}

private:
	[[nodiscard]] static comparators::ComparatorIndexedVariant<T> createImpl(CondType cond, const VariantArray&, const void* rawData,
																			 bool distinct, bool isArray, const PayloadType&,
																			 const FieldsSet&, const CollateOpts&);

	comparators::ComparatorIndexedVariant<T> impl_;
	std::string_view indexName_;
	int matchedCount_{0};
	bool isNotOperation_{false};
};

template <>
[[nodiscard]] RX_ALWAYS_INLINE bool ComparatorIndexed<key_string>::Compare(const PayloadValue& item, IdType rowId) {
	static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<key_string>> == 15);
	bool res;
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
[[nodiscard]] RX_ALWAYS_INLINE bool ComparatorIndexed<PayloadValue>::Compare(const PayloadValue& item, IdType rowId) {
	static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<PayloadValue>> == 1);
	const bool res = std::get_if<0>(&impl_)->Compare(item, rowId);
	matchedCount_ += res;
	return res;
}

template <>
[[nodiscard]] RX_ALWAYS_INLINE bool ComparatorIndexed<Point>::Compare(const PayloadValue& item, IdType rowId) {
	static_assert(std::variant_size_v<comparators::ComparatorIndexedVariant<Point>> == 5);
	bool res;
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

extern template std::string ComparatorIndexed<int>::ConditionStr() const;
extern template std::string ComparatorIndexed<int64_t>::ConditionStr() const;
extern template std::string ComparatorIndexed<bool>::ConditionStr() const;
extern template std::string ComparatorIndexed<double>::ConditionStr() const;
extern template std::string ComparatorIndexed<key_string>::ConditionStr() const;
extern template std::string ComparatorIndexed<PayloadValue>::ConditionStr() const;
extern template std::string ComparatorIndexed<Point>::ConditionStr() const;
extern template std::string ComparatorIndexed<Uuid>::ConditionStr() const;

extern template comparators::ComparatorIndexedVariant<int> ComparatorIndexed<int>::createImpl(CondType, const VariantArray&, const void*,
																							  bool, bool, const PayloadType&,
																							  const FieldsSet&, const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<int64_t> ComparatorIndexed<int64_t>::createImpl(CondType, const VariantArray&,
																									  const void*, bool, bool,
																									  const PayloadType&, const FieldsSet&,
																									  const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<double> ComparatorIndexed<double>::createImpl(CondType, const VariantArray&,
																									const void*, bool, bool,
																									const PayloadType&, const FieldsSet&,
																									const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<bool> ComparatorIndexed<bool>::createImpl(CondType, const VariantArray&, const void*,
																								bool, bool, const PayloadType&,
																								const FieldsSet&, const CollateOpts&);
extern template comparators::ComparatorIndexedVariant<Uuid> ComparatorIndexed<Uuid>::createImpl(CondType, const VariantArray&, const void*,
																								bool, bool, const PayloadType&,
																								const FieldsSet&, const CollateOpts&);
template <>
[[nodiscard]] comparators::ComparatorIndexedVariant<key_string> ComparatorIndexed<key_string>::createImpl(
	CondType, const VariantArray&, const void*, bool, bool, const PayloadType&, const FieldsSet&, const CollateOpts&);
template <>
[[nodiscard]] comparators::ComparatorIndexedVariant<Point> ComparatorIndexed<Point>::createImpl(CondType, const VariantArray&, const void*,
																								bool, bool, const PayloadType&,
																								const FieldsSet&, const CollateOpts&);
template <>
[[nodiscard]] comparators::ComparatorIndexedVariant<PayloadValue> ComparatorIndexed<PayloadValue>::createImpl(
	CondType, const VariantArray&, const void*, bool, bool, const PayloadType&, const FieldsSet&, const CollateOpts&);

}  // namespace reindexer
