#include "comparator_not_indexed.h"
#include "helpers.h"

namespace {

[[nodiscard]] std::string anyComparatorCondStr() {
	using namespace std::string_literals;
	return "IS NOT NULL"s;
}

[[nodiscard]] std::string emptyComparatorCondStr() {
	using namespace std::string_literals;
	return "IS NULL"s;
}

}  // namespace

namespace reindexer {

namespace comparators {

template <CondType Cond>
ComparatorNotIndexedImplBase<Cond>::ComparatorNotIndexedImplBase(const VariantArray& values) : value_{GetValue<Variant>(Cond, values, 0)} {}

ComparatorNotIndexedImplBase<CondRange>::ComparatorNotIndexedImplBase(const VariantArray& values)
	: value1_{GetValue<Variant>(CondRange, values, 0)}, value2_{GetValue<Variant>(CondRange, values, 1)} {}

ComparatorNotIndexedImplBase<CondLike>::ComparatorNotIndexedImplBase(const VariantArray& values)
	: value_{GetValue<key_string>(CondLike, values, 0)}, valueView_{p_string{value_}} {}

ComparatorNotIndexedImpl<CondDWithin, false>::ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType,
																	   const TagsPath& fieldPath)
	: payloadType_{payloadType},
	  fieldPath_{fieldPath},
	  point_{GetValue<Point>(CondDWithin, values, 0)},
	  distance_{GetValue<double>(CondDWithin, values, 1)} {}

template <CondType Cond>
[[nodiscard]] std::string ComparatorNotIndexedImplBase<Cond>::ConditionStr() const {
	return fmt::sprintf("%s %s", CondToStr<Cond>(), value_.As<std::string>());
}

[[nodiscard]] std::string ComparatorNotIndexedImplBase<CondRange>::ConditionStr() const {
	return fmt::sprintf("RANGE(%s %s)", value1_.As<std::string>(), value2_.As<std::string>());
}

[[nodiscard]] std::string ComparatorNotIndexedImplBase<CondSet>::ConditionStr() const {
	using namespace std::string_literals;
	if (values_.empty()) {
		return "IN []"s;
	} else {
		return fmt::sprintf("IN [%s ...]", values_.cbegin()->As<std::string>());
	}
}

[[nodiscard]] std::string ComparatorNotIndexedImplBase<CondLike>::ConditionStr() const { return fmt::sprintf("LIKE \"%s\"", valueView_); }

[[nodiscard]] std::string ComparatorNotIndexedImpl<CondAny, false>::ConditionStr() const { return anyComparatorCondStr(); }

[[nodiscard]] std::string ComparatorNotIndexedImpl<CondAny, true>::ConditionStr() const { return anyComparatorCondStr(); }

[[nodiscard]] std::string ComparatorNotIndexedImpl<CondEmpty, false>::ConditionStr() const { return emptyComparatorCondStr(); }

[[nodiscard]] std::string ComparatorNotIndexedImpl<CondDWithin, false>::ConditionStr() const {
	return fmt::sprintf("DWITHIN(POINT(%.4f %.4f), %.4f)", point_.X(), point_.Y(), distance_);
}

[[nodiscard]] std::string ComparatorNotIndexedImpl<CondAllSet, false>::ConditionStr() const {
	if (values_.empty()) {
		return fmt::sprintf("ALLSET []");
	} else {
		return fmt::sprintf("ALLSET [%s ...]", values_.cbegin()->first.As<std::string>());
	}
}

}  // namespace comparators

[[nodiscard]] std::string ComparatorNotIndexed::ConditionStr() const {
	assertrx_dbg(dynamic_cast<const ImplVariantType*>(impl_.get()));
	return std::visit([&](const auto& impl) { return impl.ConditionStr(); }, *static_cast<const ImplVariantType*>(impl_.get()));
}

ComparatorNotIndexed::ImplVariantType ComparatorNotIndexed::createImpl(CondType cond, const VariantArray& values,
																	   const PayloadType& payloadType, const TagsPath& fieldPath,
																	   bool distinct) {
	using namespace comparators;
	if (distinct) {
		switch (cond) {
			case CondEq:
			case CondSet:
			case CondAllSet:
				if (values.size() == 1) {
					return ComparatorNotIndexedImpl<CondEq, true>{values, payloadType, fieldPath};
				} else if (cond == CondAllSet) {
					return ComparatorNotIndexedImpl<CondAllSet, true>{values, payloadType, fieldPath};
				} else {
					return ComparatorNotIndexedImpl<CondSet, true>{values, payloadType, fieldPath};
				}
			case CondLt:
				return ComparatorNotIndexedImpl<CondLt, true>{values, payloadType, fieldPath};
			case CondLe:
				return ComparatorNotIndexedImpl<CondLe, true>{values, payloadType, fieldPath};
			case CondGt:
				return ComparatorNotIndexedImpl<CondGt, true>{values, payloadType, fieldPath};
			case CondGe:
				return ComparatorNotIndexedImpl<CondGe, true>{values, payloadType, fieldPath};
			case CondRange:
				return ComparatorNotIndexedImpl<CondRange, true>{values, payloadType, fieldPath};
			case CondLike:
				return ComparatorNotIndexedImpl<CondLike, true>{values, payloadType, fieldPath};
			case CondDWithin:
				return ComparatorNotIndexedImpl<CondDWithin, true>{values, payloadType, fieldPath};
			case CondAny:
				return ComparatorNotIndexedImpl<CondAny, true>{payloadType, fieldPath};
			case CondEmpty:
				return ComparatorNotIndexedImpl<CondEmpty, true>{payloadType, fieldPath};
		}
	} else {
		switch (cond) {
			case CondEq:
			case CondSet:
			case CondAllSet:
				if (values.size() == 1) {
					return ComparatorNotIndexedImpl<CondEq, false>{values, payloadType, fieldPath};
				} else if (cond == CondAllSet) {
					return ComparatorNotIndexedImpl<CondAllSet, false>{values, payloadType, fieldPath};
				} else {
					return ComparatorNotIndexedImpl<CondSet, false>{values, payloadType, fieldPath};
				}
			case CondLt:
				return ComparatorNotIndexedImpl<CondLt, false>{values, payloadType, fieldPath};
			case CondLe:
				return ComparatorNotIndexedImpl<CondLe, false>{values, payloadType, fieldPath};
			case CondGt:
				return ComparatorNotIndexedImpl<CondGt, false>{values, payloadType, fieldPath};
			case CondGe:
				return ComparatorNotIndexedImpl<CondGe, false>{values, payloadType, fieldPath};
			case CondRange:
				return ComparatorNotIndexedImpl<CondRange, false>{values, payloadType, fieldPath};
			case CondLike:
				return ComparatorNotIndexedImpl<CondLike, false>{values, payloadType, fieldPath};
			case CondDWithin:
				return ComparatorNotIndexedImpl<CondDWithin, false>{values, payloadType, fieldPath};
			case CondAny:
				return ComparatorNotIndexedImpl<CondAny, false>{payloadType, fieldPath};
			case CondEmpty:
				return ComparatorNotIndexedImpl<CondEmpty, false>{payloadType, fieldPath};
		}
	}
	abort();
}

}  // namespace reindexer
