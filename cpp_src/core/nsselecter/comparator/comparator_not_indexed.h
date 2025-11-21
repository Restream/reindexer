#pragma once

#include "comparator_not_indexed_distinct.h"
#include "const.h"
#include "core/keyvalue/geometry.h"
#include "core/keyvalue/relaxed_variant_hash.h"
#include "core/payload/payloadiface.h"
#include "core/payload/payloadtype.h"
#include "estl/multihash_map.h"
#include "estl/multihash_set.h"
#include "tools/string_regexp_functions.h"

namespace reindexer {

namespace comparators {

template <CondType Cond>
class [[nodiscard]] ComparatorNotIndexedImplBase {
protected:
	ComparatorNotIndexedImplBase(const VariantArray&);

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const Variant& v) const {
		if constexpr (Cond == CondEq) {
			return v.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_) == ComparationResult::Eq;
		} else if constexpr (Cond == CondLt) {
			return v.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_) == ComparationResult::Lt;
		} else if constexpr (Cond == CondLe) {
			return v.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_) & ComparationResult::Le;
		} else if constexpr (Cond == CondGt) {
			return v.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_) == ComparationResult::Gt;
		} else if constexpr (Cond == CondGe) {
			return v.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value_) & ComparationResult::Ge;
		} else {
			static_assert(Cond == CondEq || Cond == CondLt || Cond == CondLe || Cond == CondGt || Cond == CondGe);
		}
	}

private:
	Variant value_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImplBase<CondRange> {
protected:
	ComparatorNotIndexedImplBase(const VariantArray&);

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const Variant& v) const {
		return (v.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value1_) & ComparationResult::Ge) &&
			   (v.RelaxCompare<WithString::Yes, NotComparable::Return, kWhereCompareNullHandling>(value2_) & ComparationResult::Le);
	}

private:
	Variant value1_, value2_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImplBase<CondSet> {
protected:
	ComparatorNotIndexedImplBase(const VariantArray& values);

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const Variant& v) const { return values_.find(v) != values_.cend(); }

private:
	MultiHashSet<Variant, RelaxedHasher<NotComparable::Return>::indexesCount, RelaxedHasher<NotComparable::Return>,
				 RelaxedComparator<NotComparable::Return>>
		values_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImplBase<CondLike> {
protected:
	ComparatorNotIndexedImplBase(const VariantArray&);

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const Variant& v) const {
		if (!v.Type().Is<KeyValueType::String>()) {
			return false;
		}
		return matchLikePattern(static_cast<p_string>(v), valueView_);
	}

private:
	key_string value_;
	std::string_view valueView_;
};

template <CondType Cond, bool Distinct>
class ComparatorNotIndexedImpl;

template <CondType Cond>
class [[nodiscard]] ComparatorNotIndexedImpl<Cond, false> : private ComparatorNotIndexedImplBase<Cond> {
	using Base = ComparatorNotIndexedImplBase<Cond>;

public:
	ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType, const TagsPath& fieldPath)
		: Base{values}, payloadType_{payloadType}, fieldPath_{fieldPath} {}

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		for (const Variant& v : buffer_) {
			if (Base::Compare(v)) {
				return true;
			}
		}
		return false;
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}
	using Base::ConditionStr;

private:
	PayloadType payloadType_;
	TagsPath fieldPath_;
	VariantArray buffer_;
};

template <CondType Cond>
class [[nodiscard]] ComparatorNotIndexedImpl<Cond, true> : private ComparatorNotIndexedImplBase<Cond> {
	using Base = ComparatorNotIndexedImplBase<Cond>;

public:
	ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType, const TagsPath& fieldPath)
		: Base{values}, distinct_{}, payloadType_{payloadType}, fieldPath_{fieldPath} {}

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		for (const Variant& v : buffer_) {
			if (Base::Compare(v) == 0 && distinct_.Compare(v)) {
				return true;
			}
		}
		return false;
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		for (Variant& v : buffer_) {
			distinct_.ExcludeValues(std::move(v));
		}
	}
	using Base::ConditionStr;

private:
	ComparatorNotIndexedDistinct distinct_;
	PayloadType payloadType_;
	TagsPath fieldPath_;
	VariantArray buffer_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondAny, false> {
public:
	ComparatorNotIndexedImpl(const PayloadType& payloadType, const TagsPath& fieldPath)
		: payloadType_{payloadType}, fieldPath_{fieldPath} {}

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		for (const Variant& v : buffer_) {
			if (!v.IsNullValue()) {
				return true;
			}
		}
		return false;
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	PayloadType payloadType_;
	TagsPath fieldPath_;
	VariantArray buffer_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondAny, true> {
public:
	ComparatorNotIndexedImpl(const PayloadType& payloadType, const TagsPath& fieldPath)
		: payloadType_{payloadType}, fieldPath_{fieldPath} {}

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		for (const Variant& v : buffer_) {
			if (!v.IsNullValue() && distinct_.Compare(v)) {
				return true;
			}
		}
		return false;
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		for (Variant& v : buffer_) {
			distinct_.ExcludeValues(std::move(v));
		}
	}

private:
	ComparatorNotIndexedDistinct distinct_;
	PayloadType payloadType_;
	TagsPath fieldPath_;
	VariantArray buffer_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondEmpty, false> {
public:
	ComparatorNotIndexedImpl(const PayloadType& payloadType, const TagsPath& fieldPath)
		: payloadType_{payloadType}, fieldPath_{fieldPath} {}

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		if (buffer_.IsObjectValue()) [[unlikely]] {
			return false;
		}
		for (const Variant& v : buffer_) {
			if (v.IsNullValue()) [[unlikely]] {
				return true;
			}
		}
		return buffer_.empty();
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

private:
	PayloadType payloadType_;
	TagsPath fieldPath_;
	VariantArray buffer_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondEmpty, true> : private ComparatorNotIndexedImpl<CondEmpty, false> {
	using Base = ComparatorNotIndexedImpl<CondEmpty, false>;

public:
	ComparatorNotIndexedImpl(const PayloadType& payloadType, const TagsPath& fieldPath) : Base{payloadType, fieldPath} {}

	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	using Base::ExcludeDistinctValues;
	using Base::ConditionStr;
	using Base::Compare;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondDWithin, false> {
public:
	ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType, const TagsPath& fieldPath);

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		VariantArray buffer;
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer, KeyValueType::Undefined{});
		if (buffer.size() < 2 || !buffer[0].Type().IsNumeric() || !buffer[1].Type().IsNumeric()) {
			return false;
		}
		return DWithin(Point{buffer[0].As<double>(), buffer[1].As<double>()}, point_, distance_);
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

protected:
	PayloadType payloadType_;
	TagsPath fieldPath_;
	Point point_;
	double distance_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondDWithin, true> : private ComparatorNotIndexedImpl<CondDWithin, false> {
	using Base = ComparatorNotIndexedImpl<CondDWithin, false>;

public:
	ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType, const TagsPath& fieldPath)
		: Base{values, payloadType, fieldPath} {}

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		VariantArray buffer;
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer, KeyValueType::Undefined{});
		if (buffer.size() != 2 || !buffer[0].Type().Is<KeyValueType::Double>() || !buffer[0].Type().Is<KeyValueType::Double>()) {
			return false;
		}
		const Point p{buffer[0].As<double>(), buffer[1].As<double>()};
		return DWithin(p, point_, distance_) && distinct_.Compare(Variant{p});
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_True; }
	void ExcludeDistinctValues(const PayloadValue& item, IdType /*rowId*/) {
		VariantArray buffer;
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer, KeyValueType::Undefined{});
		if (buffer.size() != 2 || !buffer[0].Type().Is<KeyValueType::Double>() || !buffer[0].Type().Is<KeyValueType::Double>()) {
			return;
		}
		const Point p{buffer[0].As<double>(), buffer[1].As<double>()};
		distinct_.ExcludeValues(Variant{p});
	}
	using Base::ConditionStr;

private:
	ComparatorNotIndexedDistinct distinct_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondAllSet, false> {
public:
	ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType, const TagsPath& fieldPath);

	std::string ConditionStr() const;
	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		allSetValues_.clear();
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		for (const Variant& v : buffer_) {
			const auto it = values_.find(v);
			if (it != values_.cend()) {
				allSetValues_.emplace(it->second);
				if (allSetValues_.size() == values_.size()) {
					return true;
				}
			}
		}
		return false;
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

protected:
	PayloadType payloadType_;
	TagsPath fieldPath_;
	VariantArray buffer_;
	MultiHashMap<Variant, int, RelaxedHasher<NotComparable::Return>::indexesCount, RelaxedHasher<NotComparable::Return>,
				 RelaxedComparator<NotComparable::Return>>
		values_;
	fast_hash_set<int> allSetValues_;
};

template <>
class [[nodiscard]] ComparatorNotIndexedImpl<CondAllSet, true> : private ComparatorNotIndexedImpl<CondAllSet, false> {
	using Base = ComparatorNotIndexedImpl<CondAllSet, false>;

public:
	ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType, const TagsPath& fieldPath)
		: Base{values, payloadType, fieldPath} {}

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType /*rowId*/) {
		allSetValues_.clear();
		ConstPayload{payloadType_, item}.GetByJsonPath(fieldPath_, buffer_, KeyValueType::Undefined{});
		bool haveNotDistinct = false;
		for (const Variant& v : buffer_) {
			const auto it = values_.find(v);
			if (it != values_.cend()) {
				allSetValues_.emplace(it->second);
				if (distinct_.Compare(it->first)) {
					haveNotDistinct = true;
				}
				if (haveNotDistinct && allSetValues_.size() == values_.size()) {
					return true;
				}
			}
		}
		return false;
	}
	reindexer::IsDistinct IsDistinct() const noexcept { return IsDistinct_False; }
	void ExcludeDistinctValues(const PayloadValue&, IdType /*rowId*/) const noexcept {}

	using Base::ConditionStr;

private:
	ComparatorNotIndexedDistinct distinct_;
};

using ComparatorNotIndexedVariant = std::variant<
	ComparatorNotIndexedImpl<CondAny, false>, ComparatorNotIndexedImpl<CondEmpty, false>, ComparatorNotIndexedImpl<CondEq, false>,
	ComparatorNotIndexedImpl<CondLt, false>, ComparatorNotIndexedImpl<CondLe, false>, ComparatorNotIndexedImpl<CondGt, false>,
	ComparatorNotIndexedImpl<CondGe, false>, ComparatorNotIndexedImpl<CondRange, false>, ComparatorNotIndexedImpl<CondLike, false>,
	ComparatorNotIndexedImpl<CondSet, false>, ComparatorNotIndexedImpl<CondAllSet, false>, ComparatorNotIndexedImpl<CondDWithin, false>,
	ComparatorNotIndexedImpl<CondAny, true>, ComparatorNotIndexedImpl<CondEmpty, true>, ComparatorNotIndexedImpl<CondEq, true>,
	ComparatorNotIndexedImpl<CondLt, true>, ComparatorNotIndexedImpl<CondLe, true>, ComparatorNotIndexedImpl<CondGt, true>,
	ComparatorNotIndexedImpl<CondGe, true>, ComparatorNotIndexedImpl<CondRange, true>, ComparatorNotIndexedImpl<CondLike, true>,
	ComparatorNotIndexedImpl<CondSet, true>, ComparatorNotIndexedImpl<CondAllSet, true>, ComparatorNotIndexedImpl<CondDWithin, true>>;

}  // namespace comparators

class [[nodiscard]] ComparatorNotIndexed {
public:
	ComparatorNotIndexed(std::string_view fieldName, CondType cond, const VariantArray& values, const PayloadType& payloadType,
						 const TagsPath& fieldPath, IsDistinct distinct)
		: impl_{createImpl(cond, values, payloadType, fieldPath, distinct)}, fieldName_{fieldName} {}

	const std::string& Name() const& noexcept { return fieldName_; }
	auto Name() const&& = delete;
	std::string ConditionStr() const;
	std::string Dump() const { return Name() + ' ' + ConditionStr(); }
	int GetMatchedCount(bool invert) const noexcept {
		assertrx_dbg(totalCalls_ >= matchedCount_);
		return invert ? (totalCalls_ - matchedCount_) : matchedCount_;
	}
	double Cost(double expectedIterations) const noexcept {
		return comparators::kNonIdxFieldComparatorCostMultiplier * double(expectedIterations) + 1.0 +
			   (isNotOperation_ ? expectedIterations : 0.0);
	}
	void SetNotOperationFlag(bool isNotOperation) noexcept { isNotOperation_ = isNotOperation; }

	RX_ALWAYS_INLINE bool Compare(const PayloadValue& item, IdType rowId) {
		static_assert(std::variant_size_v<comparators::ComparatorNotIndexedVariant> == 24);
		bool res;
		++totalCalls_;
		switch (impl_.index()) {
			case 0:
				res = std::get_if<0>(&impl_)->Compare(item, rowId);
				break;
			case 1:
				res = std::get_if<1>(&impl_)->Compare(item, rowId);
				break;
			case 2:
				res = std::get_if<2>(&impl_)->Compare(item, rowId);
				break;
			case 3:
				res = std::get_if<3>(&impl_)->Compare(item, rowId);
				break;
			case 4:
				res = std::get_if<4>(&impl_)->Compare(item, rowId);
				break;
			case 5:
				res = std::get_if<5>(&impl_)->Compare(item, rowId);
				break;
			case 6:
				res = std::get_if<6>(&impl_)->Compare(item, rowId);
				break;
			case 7:
				res = std::get_if<7>(&impl_)->Compare(item, rowId);
				break;
			case 8:
				res = std::get_if<8>(&impl_)->Compare(item, rowId);
				break;
			case 9:
				res = std::get_if<9>(&impl_)->Compare(item, rowId);
				break;
			case 10:
				res = std::get_if<10>(&impl_)->Compare(item, rowId);
				break;
			case 11:
				res = std::get_if<11>(&impl_)->Compare(item, rowId);
				break;
			case 12:
				res = std::get_if<12>(&impl_)->Compare(item, rowId);
				break;
			case 13:
				res = std::get_if<13>(&impl_)->Compare(item, rowId);
				break;
			case 14:
				res = std::get_if<14>(&impl_)->Compare(item, rowId);
				break;
			case 15:
				res = std::get_if<15>(&impl_)->Compare(item, rowId);
				break;
			case 16:
				res = std::get_if<16>(&impl_)->Compare(item, rowId);
				break;
			case 17:
				res = std::get_if<17>(&impl_)->Compare(item, rowId);
				break;
			case 18:
				res = std::get_if<18>(&impl_)->Compare(item, rowId);
				break;
			case 19:
				res = std::get_if<19>(&impl_)->Compare(item, rowId);
				break;
			case 20:
				res = std::get_if<20>(&impl_)->Compare(item, rowId);
				break;
			case 21:
				res = std::get_if<21>(&impl_)->Compare(item, rowId);
				break;
			case 22:
				res = std::get_if<22>(&impl_)->Compare(item, rowId);
				break;
			case 23:
				res = std::get_if<23>(&impl_)->Compare(item, rowId);
				break;
			default:
				abort();
		}
		matchedCount_ += res;
		return res;
	}
	void ExcludeDistinctValues(const PayloadValue& item, IdType rowId) {
		static_assert(std::variant_size_v<comparators::ComparatorNotIndexedVariant> == 24);
		switch (impl_.index()) {
			case 0:
				return std::get_if<0>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 1:
				return std::get_if<1>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 2:
				return std::get_if<2>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 3:
				return std::get_if<3>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 4:
				return std::get_if<4>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 5:
				return std::get_if<5>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 6:
				return std::get_if<6>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 7:
				return std::get_if<7>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 8:
				return std::get_if<8>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 9:
				return std::get_if<9>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 10:
				return std::get_if<10>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 11:
				return std::get_if<11>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 12:
				return std::get_if<12>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 13:
				return std::get_if<13>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 14:
				return std::get_if<14>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 15:
				return std::get_if<15>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 16:
				return std::get_if<16>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 17:
				return std::get_if<17>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 18:
				return std::get_if<18>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 19:
				return std::get_if<19>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 20:
				return std::get_if<20>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 21:
				return std::get_if<21>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 22:
				return std::get_if<22>(&impl_)->ExcludeDistinctValues(item, rowId);
			case 23:
				return std::get_if<23>(&impl_)->ExcludeDistinctValues(item, rowId);
			default:
				abort();
		}
	}
	reindexer::IsDistinct IsDistinct() const noexcept {
		return std::visit([](auto& impl) { return impl.IsDistinct(); }, impl_);
	}
	bool IsIndexed() const noexcept { return false; }

private:
	using ImplVariantType = comparators::ComparatorNotIndexedVariant;
	static ImplVariantType createImpl(CondType, const VariantArray& values, const PayloadType&, const TagsPath&, reindexer::IsDistinct);

	int totalCalls_{0};
	int matchedCount_{0};
	ImplVariantType impl_;
	std::string fieldName_;
	bool isNotOperation_{false};
};

}  // namespace reindexer
