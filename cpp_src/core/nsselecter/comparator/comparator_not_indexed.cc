#include "comparator_not_indexed.h"
#include "helpers.h"

namespace {

std::string anyComparatorCondStr() {
	using namespace std::string_literals;
	return "IS NOT NULL"s;
}

std::string emptyComparatorCondStr() {
	using namespace std::string_literals;
	return "IS NULL"s;
}

}  // namespace

namespace reindexer {

namespace comparators {

template <CondType Cond>
ComparatorNotIndexedImplBase<Cond>::ComparatorNotIndexedImplBase(const VariantArray& values) : value_(GetValue<Variant>(Cond, values, 0)) {
	auto t = value_.Type();
	if (!(t.template Is<KeyValueType::String>() || t.template Is<KeyValueType::Uuid>() || t.IsNumeric())) {
		throw Error{errQueryExec, "Value type in condition for non indexed field must be string, numeric or uuid. Value type is '{}'",
					t.Name()};
	}
}

ComparatorNotIndexedImplBase<CondRange>::ComparatorNotIndexedImplBase(const VariantArray& values)
	: value1_{GetValue<Variant>(CondRange, values, 0)}, value2_{GetValue<Variant>(CondRange, values, 1)} {}

ComparatorNotIndexedImplBase<CondSet>::ComparatorNotIndexedImplBase(const VariantArray& values) : values_{values.size()} {
	for (const Variant& v : values) {
		assertrx_dbg(!v.IsNullValue());
		auto t = v.Type();
		if (t.Is<KeyValueType::String>() || t.Is<KeyValueType::Uuid>() || t.IsNumeric()) [[likely]] {
			std::ignore = values_.insert(v);
		} else {
			throw Error{errQueryExec, "Value type in CondSet for non indexed field must be string, numeric or uuid. Value type is '{}'",
						t.Name()};
		}
	}
}

ComparatorNotIndexedImplBase<CondLike>::ComparatorNotIndexedImplBase(const VariantArray& values)
	: value_{GetValue<key_string>(CondLike, values, 0)}, valueView_{p_string{value_}} {}

ComparatorNotIndexedImpl<CondDWithin, false>::ComparatorNotIndexedImpl(const VariantArray& values, const PayloadType& payloadType,
																	   const TagsPath& fieldPath)
	: payloadType_{payloadType},
	  fieldPath_{fieldPath},
	  point_{GetValue<Point>(CondDWithin, values, 0)},
	  distance_{GetValue<double>(CondDWithin, values, 1)} {}

reindexer::comparators::ComparatorNotIndexedImpl<CondAllSet, false>::ComparatorNotIndexedImpl(const VariantArray& values,
																							  const PayloadType& payloadType,
																							  const TagsPath& fieldPath)
	: payloadType_{payloadType}, fieldPath_{fieldPath}, values_{values.size()} {
	int i = 0;
	for (const Variant& v : values) {
		assertrx_dbg(!v.IsNullValue());
		std::ignore = values_.emplace(v, i);
		++i;
	}
}

template <CondType Cond>
std::string ComparatorNotIndexedImplBase<Cond>::ConditionStr() const {
	return fmt::format("{} {}", CondToStr<Cond>(), value_.As<std::string>());
}

std::string ComparatorNotIndexedImplBase<CondRange>::ConditionStr() const {
	return fmt::format("RANGE({} {})", value1_.As<std::string>(), value2_.As<std::string>());
}

std::string ComparatorNotIndexedImplBase<CondSet>::ConditionStr() const {
	using namespace std::string_literals;
	if (values_.empty()) {
		return "IN []"s;
	} else {
		return fmt::format("IN [{} ...]", values_.cbegin()->As<std::string>());
	}
}

std::string ComparatorNotIndexedImplBase<CondLike>::ConditionStr() const { return fmt::format("LIKE \"{}\"", valueView_); }

std::string ComparatorNotIndexedImpl<CondAny, false>::ConditionStr() const { return anyComparatorCondStr(); }

std::string ComparatorNotIndexedImpl<CondAny, true>::ConditionStr() const { return anyComparatorCondStr(); }

std::string ComparatorNotIndexedImpl<CondEmpty, false>::ConditionStr() const { return emptyComparatorCondStr(); }

std::string ComparatorNotIndexedImpl<CondDWithin, false>::ConditionStr() const {
	return fmt::format("DWITHIN(POINT({:.4f} {:.4f}), {:.4f})", point_.X(), point_.Y(), distance_);
}

std::string ComparatorNotIndexedImpl<CondAllSet, false>::ConditionStr() const {
	if (values_.empty()) {
		return fmt::format("ALLSET []");
	} else {
		return fmt::format("ALLSET [{} ...]", values_.cbegin()->first.As<std::string>());
	}
}

}  // namespace comparators

std::string ComparatorNotIndexed::ConditionStr() const {
	return std::visit([&](const auto& impl) { return impl.ConditionStr(); }, impl_);
}

ComparatorNotIndexed::ImplVariantType ComparatorNotIndexed::createImpl(CondType cond, const VariantArray& values,
																	   const PayloadType& payloadType, const TagsPath& fieldPath,
																	   reindexer::IsDistinct distinct) {
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
			case CondKnn:
				throw_as_assert;
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
			case CondKnn:
				throw_as_assert;
		}
	}
	throw_as_assert;
}

}  // namespace reindexer
