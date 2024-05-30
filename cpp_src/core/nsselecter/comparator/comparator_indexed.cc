#include "comparator_indexed.h"
#include <cwchar>
#include "core/nsselecter/comparator/helpers.h"

namespace {

using reindexer::make_intrusive;

template <typename T>
std::string_view typeToStr() noexcept;

template <>
std::string_view typeToStr<int>() noexcept {
	using namespace std::string_view_literals;
	return "int"sv;
}

template <>
std::string_view typeToStr<int64_t>() noexcept {
	using namespace std::string_view_literals;
	return "int64"sv;
}

template <>
std::string_view typeToStr<double>() noexcept {
	using namespace std::string_view_literals;
	return "double"sv;
}

template <>
std::string_view typeToStr<reindexer::Uuid>() noexcept {
	using namespace std::string_view_literals;
	return "UUID"sv;
}

template <>
std::string_view typeToStr<bool>() noexcept {
	using namespace std::string_view_literals;
	return "bool"sv;
}

template <>
std::string_view typeToStr<reindexer::key_string>() noexcept {
	using namespace std::string_view_literals;
	return "string"sv;
}

template <>
std::string_view typeToStr<reindexer::PayloadValue>() noexcept {
	using namespace std::string_view_literals;
	return "composite"sv;
}

template <>
std::string_view typeToStr<reindexer::Point>() noexcept {
	using namespace std::string_view_literals;
	return "point"sv;
}

template <typename T, CondType Cond>
typename reindexer::comparators::ValuesHolder<T, Cond>::Type getInitValues(const reindexer::VariantArray& values) {
	if constexpr (Cond == CondRange) {
		return {reindexer::comparators::GetValue<T>(Cond, values, 0), reindexer::comparators::GetValue<T>(Cond, values, 1)};
	} else if constexpr (Cond == CondSet || Cond == CondAllSet) {
		return {};
	} else if constexpr (Cond == CondEq || Cond == CondLt || Cond == CondLe || Cond == CondGt || Cond == CondGe) {
		return reindexer::comparators::GetValue<T>(Cond, values, 0);
	}
}

template <CondType Cond>
typename reindexer::comparators::ValuesHolder<reindexer::key_string, Cond>::Type getInitStringValues(const reindexer::VariantArray& values,
																									 const CollateOpts& collate) {
	if constexpr (Cond == CondRange) {
		return {reindexer::comparators::GetValue<reindexer::key_string>(Cond, values, 0),
				reindexer::comparators::GetValue<reindexer::key_string>(Cond, values, 1)};
	} else if constexpr (Cond == CondSet) {
		return {collate};
	} else if constexpr (Cond == CondAllSet) {
		return {collate, {}};
	} else if constexpr (Cond == CondEq || Cond == CondLt || Cond == CondLe || Cond == CondGt || Cond == CondGe || Cond == CondLike) {
		return reindexer::comparators::GetValue<reindexer::key_string>(Cond, values, 0);
	}
}

template <typename T, CondType Cond>
void initComparator(const reindexer::VariantArray& from, typename reindexer::comparators::ValuesHolder<T, Cond>::Type& to) {
	if constexpr (Cond == CondSet) {
		for (const reindexer::Variant& v : from) {
			to.insert(reindexer::comparators::GetValue<T>(v));
		}
	} else if constexpr (Cond == CondAllSet) {
		int i = 0;
		for (const reindexer::Variant& v : from) {
			to.values_.emplace(reindexer::comparators::GetValue<T>(v), i);
			++i;
		}
	}
}

template <typename T>
void initComparator(CondType cond, const reindexer::VariantArray& from, reindexer::comparators::DataHolder<T>& to) {
	using namespace reindexer::comparators;
	using SingleType = typename DataHolder<T>::SingleType;
	using RangeType = typename DataHolder<T>::RangeType;
	using SetPtrType = typename DataHolder<T>::SetPtrType;
	using AllSetPtrType = typename DataHolder<T>::AllSetPtrType;
	using SetWrpType = typename DataHolder<T>::SetWrpType;
	using AllSetWrpType = typename DataHolder<T>::AllSetWrpType;
	switch (cond) {
		case CondEq:
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			to.value_ = GetValue<T>(cond, from, 0);
			break;
		case CondRange:
			to.value_.~SingleType();
			new (&to.range_) RangeType{GetValue<T>(cond, from, 0), GetValue<T>(cond, from, 1)};
			break;
		case CondSet:
			to.value_.~SingleType();
			new (&to.setPtr_) SetPtrType{make_intrusive<SetWrpType>()};
			initComparator<T, CondSet>(from, *to.setPtr_);
			break;
		case CondAllSet:
			to.value_.~SingleType();
			new (&to.allSetPtr_) AllSetPtrType{make_intrusive<AllSetWrpType>()};
			initComparator<T, CondAllSet>(from, *to.allSetPtr_);
			break;
		case CondAny:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			break;
	}
	to.cond_ = cond;
}

void initStringComparator(CondType cond, const reindexer::VariantArray& from, reindexer::comparators::DataHolder<reindexer::key_string>& to,
						  const CollateOpts& collate) {
	using namespace reindexer::comparators;
	using SingleType = DataHolder<reindexer::key_string>::SingleType;
	using RangeType = DataHolder<reindexer::key_string>::RangeType;
	using SetType = DataHolder<reindexer::key_string>::SetType;
	using AllSetType = DataHolder<reindexer::key_string>::AllSetType;
	using SetPtrType = DataHolder<reindexer::key_string>::SetPtrType;
	using AllSetPtrType = DataHolder<reindexer::key_string>::AllSetPtrType;
	using SetWrpType = DataHolder<reindexer::key_string>::SetWrpType;
	using AllSetWrpType = DataHolder<reindexer::key_string>::AllSetWrpType;
	switch (cond) {
		case CondEq:
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
		case CondLike:
			to.value_ = GetValue<reindexer::key_string>(cond, from, 0);
			break;
		case CondRange:
			to.value_.~SingleType();
			new (&to.range_) RangeType{GetValue<reindexer::key_string>(cond, from, 0), GetValue<reindexer::key_string>(cond, from, 1)};
			break;
		case CondSet:
			to.value_.~SingleType();
			new (&to.setPtr_) SetPtrType{make_intrusive<SetWrpType>(SetType{collate})};
			initComparator<reindexer::key_string, CondSet>(from, *to.setPtr_);
			break;
		case CondAllSet:
			to.value_.~SingleType();
			new (&to.allSetPtr_) AllSetPtrType{make_intrusive<AllSetWrpType>(AllSetType{collate, {}})};
			initComparator<reindexer::key_string, CondAllSet>(from, *to.allSetPtr_);
			break;
		case CondAny:
		case CondEmpty:
		case CondDWithin:
			break;
	}
	to.cond_ = cond;
}

template <typename T, CondType Cond, typename V>
[[nodiscard]] std::string comparatorCondStr(const V& values) {
	using namespace std::string_literals;
	if constexpr (Cond == CondRange) {
		if constexpr (std::is_same_v<T, reindexer::key_string>) {
			return fmt::sprintf("RANGE(%s, %s)", values.value1_.valueView_, values.value2_.valueView_);
		} else {
			return fmt::format("RANGE({}, {})", values.first, values.second);
		}
	} else if constexpr (Cond == CondSet) {
		if (values.empty()) {
			return "IN []"s;
		} else {
			return fmt::format("IN [{}, ...]", *values.begin());
		}
	} else if constexpr (Cond == CondAllSet) {
		if (values.values_.empty()) {
			return "ALLSET []"s;
		} else {
			return fmt::format("ALLSET [{}, ...]", values.values_.begin()->first);
		}
	} else if constexpr (Cond == CondEq || Cond == CondLt || Cond == CondLe || Cond == CondGt || Cond == CondGe) {
		if constexpr (std::is_same_v<T, reindexer::key_string>) {
			return fmt::sprintf("%s %s", reindexer::comparators::CondToStr<Cond>(), values.valueView_);
		} else {
			return fmt::format("{} {}", reindexer::comparators::CondToStr<Cond>(), values);
		}
	} else if constexpr (Cond == CondLike && std::is_same_v<T, reindexer::key_string>) {
		return fmt::sprintf("LIKE \"%s\"", values.valueView_);
	}
	abort();
}

template <typename T>
[[nodiscard]] std::string comparatorCondStr(const reindexer::comparators::DataHolder<T>& data) {
	switch (data.cond_) {
		case CondEq:
			return comparatorCondStr<T, CondEq>(data.value_);
		case CondLt:
			return comparatorCondStr<T, CondLt>(data.value_);
		case CondLe:
			return comparatorCondStr<T, CondLe>(data.value_);
		case CondGt:
			return comparatorCondStr<T, CondGt>(data.value_);
		case CondGe:
			return comparatorCondStr<T, CondGe>(data.value_);
		case CondRange:
			return comparatorCondStr<T, CondRange>(data.range_);
		case CondSet:
			assertrx_dbg(data.setPtr_);
			return comparatorCondStr<T, CondSet>(*data.setPtr_);
		case CondAllSet:
			assertrx_dbg(data.allSetPtr_);
			return comparatorCondStr<T, CondAllSet>(*data.allSetPtr_);
		case CondLike:
			return comparatorCondStr<T, CondLike>(data.value_);
		case CondAny:
		case CondEmpty:
		case CondDWithin:
		default:
			abort();
	}
}

template <CondType Cond>
[[nodiscard]] std::string compositeComparatorCondStr(
	const typename reindexer::comparators::ValuesHolder<reindexer::PayloadValue, Cond>::Type& values,
	const reindexer::PayloadType& payloadType, const reindexer::FieldsSet& fields) {
	using namespace std::string_literals;
	if constexpr (Cond == CondRange) {
		return fmt::sprintf("RANGE(%s, %s)", reindexer::Variant{values.first}.As<std::string>(payloadType, fields),
							reindexer::Variant{values.second}.As<std::string>(payloadType, fields));
	} else if constexpr (Cond == CondSet) {
		if (values.empty()) {
			return "IN []"s;
		} else {
			return fmt::sprintf("IN [%s, ...]", reindexer::Variant{*values.begin()}.As<std::string>(payloadType, fields));
		}
	} else if constexpr (Cond == CondAllSet) {
		if (values.values_.empty()) {
			return "ALLSET []"s;
		} else {
			return fmt::sprintf("ALLSET [%s, ...]", reindexer::Variant{values.values_.begin()->first}.As<std::string>(payloadType, fields));
		}
	} else if constexpr (Cond == CondEq || Cond == CondLt || Cond == CondLe || Cond == CondGt || Cond == CondGe) {
		return fmt::sprintf("%s %s", reindexer::comparators::CondToStr<Cond>(),
							reindexer::Variant{values}.As<std::string>(payloadType, fields));
	}
}

[[nodiscard]] std::string anyComparatorCondStr() {
	using namespace std::string_literals;
	return "IS NOT NULL"s;
}

[[nodiscard]] std::string emptyComparatorCondStr() {
	using namespace std::string_literals;
	return "IS NULL"s;
}

[[nodiscard]] std::string pointComparatorCondStr(reindexer::Point point, double distance) {
	return fmt::sprintf("DWITHIN(POINT(%.4f %.4f), %.4f)", point.X(), point.Y(), distance);
}

}  // namespace

namespace reindexer {

namespace comparators {

template <typename T>
ComparatorIndexedOffsetScalar<T>::ComparatorIndexedOffsetScalar(size_t offset, const VariantArray& values, CondType cond)
	: offset_{offset} {
	initComparator(cond, values, *this);
}

template <typename T>
ComparatorIndexedColumnScalar<T>::ComparatorIndexedColumnScalar(const void* rawData, const VariantArray& values, CondType cond)
	: rawData_{static_cast<const T*>(rawData)} {
	initComparator(cond, values, *this);
}

template <typename T>
ComparatorIndexedOffsetScalarDistinct<T>::ComparatorIndexedOffsetScalarDistinct(size_t offset, const VariantArray& values, CondType cond)
	: offset_{offset} {
	initComparator(cond, values, *this);
}

template <typename T>
ComparatorIndexedColumnScalarDistinct<T>::ComparatorIndexedColumnScalarDistinct(const void* rawData, const VariantArray& values,
																				CondType cond)
	: rawData_{static_cast<const T*>(rawData)} {
	initComparator(cond, values, *this);
}

template <typename T>
ComparatorIndexedOffsetArray<T>::ComparatorIndexedOffsetArray(size_t offset, const VariantArray& values, CondType cond) : offset_{offset} {
	initComparator(cond, values, *this);
}

template <typename T>
ComparatorIndexedOffsetArrayDistinct<T>::ComparatorIndexedOffsetArrayDistinct(size_t offset, const VariantArray& values, CondType cond)
	: offset_{offset} {
	initComparator(cond, values, *this);
}

template <typename T>
ComparatorIndexedJsonPath<T>::ComparatorIndexedJsonPath(const TagsPath& tagsPath, const PayloadType& payloadType,
														const VariantArray& values, CondType cond)
	: tagsPath_{tagsPath}, payloadType_{payloadType} {
	initComparator(cond, values, *this);
}

template <typename T>
ComparatorIndexedJsonPathDistinct<T>::ComparatorIndexedJsonPathDistinct(const TagsPath& tagsPath, const PayloadType& payloadType,
																		const VariantArray& values, CondType cond)
	: tagsPath_{tagsPath}, payloadType_{payloadType} {
	initComparator(cond, values, *this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedOffsetScalar<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedColumnScalar<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedOffsetScalarDistinct<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedColumnScalarDistinct<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedOffsetArray<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedOffsetArrayDistinct<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedJsonPath<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedJsonPathDistinct<T>::ConditionStr() const {
	return comparatorCondStr(*this);
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedOffsetScalarAnyDistinct<T>::ConditionStr() const {
	return anyComparatorCondStr();
}

template <typename T>
[[nodiscard]] std::string ComparatorIndexedColumnScalarAnyDistinct<T>::ConditionStr() const {
	return anyComparatorCondStr();
}

[[nodiscard]] std::string ComparatorIndexedOffsetScalarAnyStringDistinct::ConditionStr() const { return anyComparatorCondStr(); }

[[nodiscard]] std::string ComparatorIndexedOffsetArrayAny::ConditionStr() const { return anyComparatorCondStr(); }

template <typename T>
[[nodiscard]] std::string ComparatorIndexedOffsetArrayAnyDistinct<T>::ConditionStr() const {
	return anyComparatorCondStr();
}

[[nodiscard]] std::string ComparatorIndexedOffsetArrayAnyStringDistinct::ConditionStr() const { return anyComparatorCondStr(); }

[[nodiscard]] std::string ComparatorIndexedJsonPathAny::ConditionStr() const { return anyComparatorCondStr(); }

template <typename T>
[[nodiscard]] std::string ComparatorIndexedJsonPathAnyDistinct<T>::ConditionStr() const {
	return anyComparatorCondStr();
}

[[nodiscard]] std::string ComparatorIndexedJsonPathAnyStringDistinct::ConditionStr() const { return anyComparatorCondStr(); }

[[nodiscard]] std::string ComparatorIndexedOffsetArrayEmpty::ConditionStr() const { return emptyComparatorCondStr(); }

[[nodiscard]] std::string ComparatorIndexedJsonPathEmpty::ConditionStr() const { return emptyComparatorCondStr(); }

ComparatorIndexedOffsetScalarString::ComparatorIndexedOffsetScalarString(size_t offset, const VariantArray& values,
																		 const CollateOpts& collate, CondType cond)
	: collateOpts_{&collate}, offset_{offset} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

ComparatorIndexedColumnScalarString::ComparatorIndexedColumnScalarString(const void* rawData, const VariantArray& values,
																		 const CollateOpts& collate, CondType cond)
	: collateOpts_{&collate}, rawData_{static_cast<const std::string_view*>(rawData)} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

ComparatorIndexedOffsetScalarStringDistinct::ComparatorIndexedOffsetScalarStringDistinct(size_t offset, const VariantArray& values,
																						 const CollateOpts& collate, CondType cond)
	: distinct_{collate}, collateOpts_{&collate}, offset_{offset} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

ComparatorIndexedColumnScalarStringDistinct::ComparatorIndexedColumnScalarStringDistinct(const void* rawData, const VariantArray& values,
																						 const CollateOpts& collate, CondType cond)
	: distinct_{collate}, collateOpts_{&collate}, rawData_{static_cast<const std::string_view*>(rawData)} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

ComparatorIndexedOffsetArrayString::ComparatorIndexedOffsetArrayString(size_t offset, const VariantArray& values,
																	   const CollateOpts& collate, CondType cond)
	: collateOpts_{&collate}, offset_{offset} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

ComparatorIndexedOffsetArrayStringDistinct::ComparatorIndexedOffsetArrayStringDistinct(size_t offset, const VariantArray& values,
																					   const CollateOpts& collate, CondType cond)
	: distinct_{collate}, collateOpts_{&collate}, offset_{offset} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

ComparatorIndexedJsonPathString::ComparatorIndexedJsonPathString(const TagsPath& tagsPath, const PayloadType& payloadType,
																 const VariantArray& values, const CollateOpts& collate, CondType cond)
	: collateOpts_{&collate}, tagsPath_{tagsPath}, payloadType_{payloadType} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

ComparatorIndexedJsonPathStringDistinct::ComparatorIndexedJsonPathStringDistinct(const TagsPath& tagsPath, const PayloadType& payloadType,
																				 const VariantArray& values, const CollateOpts& collate,
																				 CondType cond)
	: distinct_{collate}, collateOpts_{&collate}, tagsPath_{tagsPath}, payloadType_{payloadType} {
	initStringComparator(cond, values, *this, *collateOpts_);
}

[[nodiscard]] std::string ComparatorIndexedOffsetScalarString::ConditionStr() const { return comparatorCondStr(*this); }

[[nodiscard]] std::string ComparatorIndexedColumnScalarString::ConditionStr() const { return comparatorCondStr(*this); }

[[nodiscard]] std::string ComparatorIndexedOffsetScalarStringDistinct::ConditionStr() const { return comparatorCondStr(*this); }

[[nodiscard]] std::string ComparatorIndexedColumnScalarStringDistinct::ConditionStr() const { return comparatorCondStr(*this); }

[[nodiscard]] std::string ComparatorIndexedOffsetArrayString::ConditionStr() const { return comparatorCondStr(*this); }

[[nodiscard]] std::string ComparatorIndexedOffsetArrayStringDistinct::ConditionStr() const { return comparatorCondStr(*this); }

[[nodiscard]] std::string ComparatorIndexedJsonPathString::ConditionStr() const { return comparatorCondStr(*this); }

[[nodiscard]] std::string ComparatorIndexedJsonPathStringDistinct::ConditionStr() const { return comparatorCondStr(*this); }

ComparatorIndexedComposite::ComparatorIndexedComposite(const VariantArray& values, const CollateOpts& collate, const FieldsSet& fields,
													   const PayloadType& payloadType, CondType cond)
	: collateOpts_{&collate}, fields_{fields}, payloadType_{payloadType} {
	switch (cond) {
		case CondEq:
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
			value_ = GetValue<PayloadValue>(cond, values, 0);
			break;
		case CondRange:
			value_.~PayloadValue();
			new (&range_) RangeType{GetValue<PayloadValue>(cond, values, 0), GetValue<PayloadValue>(cond, values, 1)};
			break;
		case CondSet:
			value_.~PayloadValue();
			new (&setPtr_) SetPtrType{make_intrusive<SetWrpType>(SetType{values.size(), reindexer::hash_composite_ref{payloadType, fields},
																		 reindexer::equal_composite_ref{payloadType, fields}})};
			initComparator<PayloadValue, CondSet>(values, *setPtr_);
			break;
		case CondAllSet:
			value_.~PayloadValue();
			new (&allSetPtr_) AllSetPtrType{
				make_intrusive<AllSetWrpType>(AllSetType{{reindexer::PayloadType{payloadType}, reindexer::FieldsSet{fields}}, {}})};
			initComparator<PayloadValue, CondAllSet>(values, *allSetPtr_);
			break;
		case CondAny:
		case CondEmpty:
		case CondLike:
		case CondDWithin:
			break;
	}
	cond_ = cond;
}

[[nodiscard]] std::string ComparatorIndexedComposite::ConditionStr() const {
	switch (cond_) {
		case CondEq:
			return compositeComparatorCondStr<CondEq>(value_, payloadType_, fields_);
		case CondLt:
			return compositeComparatorCondStr<CondLt>(value_, payloadType_, fields_);
		case CondLe:
			return compositeComparatorCondStr<CondLe>(value_, payloadType_, fields_);
		case CondGt:
			return compositeComparatorCondStr<CondGt>(value_, payloadType_, fields_);
		case CondGe:
			return compositeComparatorCondStr<CondGe>(value_, payloadType_, fields_);
		case CondRange:
			return compositeComparatorCondStr<CondRange>(range_, payloadType_, fields_);
		case CondSet:
			assertrx_dbg(setPtr_);
			return compositeComparatorCondStr<CondSet>(*setPtr_, payloadType_, fields_);
		case CondAllSet:
			assertrx_dbg(allSetPtr_);
			return compositeComparatorCondStr<CondAllSet>(*allSetPtr_, payloadType_, fields_);
		case CondLike:
		case CondAny:
		case CondEmpty:
		case CondDWithin:
		default:
			abort();
	}
}

ComparatorIndexedOffsetArrayDWithin::ComparatorIndexedOffsetArrayDWithin(size_t offset, const VariantArray& values)
	: point_{GetValue<Point>(CondDWithin, values, 0)}, distance_{GetValue<double>(CondDWithin, values, 1)}, offset_{offset} {}

ComparatorIndexedOffsetArrayDWithinDistinct::ComparatorIndexedOffsetArrayDWithinDistinct(size_t offset, const VariantArray& values)
	: point_{GetValue<Point>(CondDWithin, values, 0)}, distance_{GetValue<double>(CondDWithin, values, 1)}, offset_{offset} {}

ComparatorIndexedJsonPathDWithin::ComparatorIndexedJsonPathDWithin(const FieldsSet& fields, const PayloadType& payloadType,
																   const VariantArray& values)
	: payloadType_{payloadType},
	  tagsPath_{fields.getTagsPath(0)},
	  point_{GetValue<Point>(CondDWithin, values, 0)},
	  distance_{GetValue<double>(CondDWithin, values, 1)} {}

ComparatorIndexedJsonPathDWithinDistinct::ComparatorIndexedJsonPathDWithinDistinct(const FieldsSet& fields, const PayloadType& payloadType,
																				   const VariantArray& values)
	: payloadType_{payloadType},
	  tagsPath_{fields.getTagsPath(0)},
	  point_{GetValue<Point>(CondDWithin, values, 0)},
	  distance_{GetValue<double>(CondDWithin, values, 1)} {}

[[nodiscard]] std::string ComparatorIndexedOffsetArrayDWithin::ConditionStr() const { return pointComparatorCondStr(point_, distance_); }

[[nodiscard]] std::string ComparatorIndexedOffsetArrayDWithinDistinct::ConditionStr() const {
	return pointComparatorCondStr(point_, distance_);
}

[[nodiscard]] std::string ComparatorIndexedJsonPathDWithin::ConditionStr() const { return pointComparatorCondStr(point_, distance_); }

[[nodiscard]] std::string ComparatorIndexedJsonPathDWithinDistinct::ConditionStr() const {
	return pointComparatorCondStr(point_, distance_);
}

}  // namespace comparators

template <typename T>
[[nodiscard]] std::string ComparatorIndexed<T>::ConditionStr() const {
	return std::visit([](const auto& impl) { return impl.ConditionStr(); }, impl_);
}

template std::string ComparatorIndexed<int>::ConditionStr() const;
template std::string ComparatorIndexed<int64_t>::ConditionStr() const;
template std::string ComparatorIndexed<bool>::ConditionStr() const;
template std::string ComparatorIndexed<double>::ConditionStr() const;
template std::string ComparatorIndexed<key_string>::ConditionStr() const;
template std::string ComparatorIndexed<PayloadValue>::ConditionStr() const;
template std::string ComparatorIndexed<Point>::ConditionStr() const;
template std::string ComparatorIndexed<Uuid>::ConditionStr() const;

template <typename T>
[[nodiscard]] comparators::ComparatorIndexedVariant<T> ComparatorIndexed<T>::createImpl(CondType cond, const VariantArray& values,
																						const void* rawData, bool distinct, bool isArray,
																						const PayloadType& payloadType,
																						const FieldsSet& fields, const CollateOpts&) {
	using namespace comparators;
	if (fields.getTagsPathsLength() != 0) {
		switch (cond) {
			case CondEmpty:
				return ComparatorIndexedJsonPathEmpty{fields.getTagsPath(0), payloadType};
			case CondAny:
				if (distinct) {
					return ComparatorIndexedJsonPathAnyDistinct<T>{fields.getTagsPath(0), payloadType};
				} else {
					return ComparatorIndexedJsonPathAny{fields.getTagsPath(0), payloadType};
				}
			case CondEq:
			case CondSet:
			case CondAllSet:
				if (values.size() == 1) {
					if (distinct) {
						return ComparatorIndexedJsonPathDistinct<T>{fields.getTagsPath(0), payloadType, values, CondEq};
					} else {
						return ComparatorIndexedJsonPath<T>{fields.getTagsPath(0), payloadType, values, CondEq};
					}
				} else if (cond == CondAllSet) {
					if (distinct) {
						return ComparatorIndexedJsonPathDistinct<T>{fields.getTagsPath(0), payloadType, values, CondAllSet};
					} else {
						return ComparatorIndexedJsonPath<T>{fields.getTagsPath(0), payloadType, values, CondAllSet};
					}
				} else {
					if (distinct) {
						return ComparatorIndexedJsonPathDistinct<T>{fields.getTagsPath(0), payloadType, values, CondSet};
					} else {
						return ComparatorIndexedJsonPath<T>{fields.getTagsPath(0), payloadType, values, CondSet};
					}
				}
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondRange:
				if (distinct) {
					return ComparatorIndexedJsonPathDistinct<T>{fields.getTagsPath(0), payloadType, values, cond};
				} else {
					return ComparatorIndexedJsonPath<T>{fields.getTagsPath(0), payloadType, values, cond};
				}
			case CondDWithin:
			case CondLike:
			default:
				throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<T>()};
		}
	} else {
		const auto offset = payloadType->Field(fields[0]).Offset();
		if (isArray) {
			switch (cond) {
				case CondEmpty:
					return ComparatorIndexedOffsetArrayEmpty{offset};
				case CondAny:
					if (distinct) {
						return ComparatorIndexedOffsetArrayAnyDistinct<T>{offset};
					} else {
						return ComparatorIndexedOffsetArrayAny{offset};
					}
				case CondEq:
				case CondSet:
				case CondAllSet:
					if (values.size() == 1) {
						if (distinct) {
							return ComparatorIndexedOffsetArrayDistinct<T>{offset, values, CondEq};
						} else {
							return ComparatorIndexedOffsetArray<T>{offset, values, CondEq};
						}
					} else if (cond == CondAllSet) {
						if (distinct) {
							return ComparatorIndexedOffsetArrayDistinct<T>{offset, values, CondAllSet};
						} else {
							return ComparatorIndexedOffsetArray<T>{offset, values, CondAllSet};
						}
					} else {
						if (distinct) {
							return ComparatorIndexedOffsetArrayDistinct<T>{offset, values, CondSet};
						} else {
							return ComparatorIndexedOffsetArray<T>{offset, values, CondSet};
						}
					}
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondRange:
					if (distinct) {
						return ComparatorIndexedOffsetArrayDistinct<T>{offset, values, cond};
					} else {
						return ComparatorIndexedOffsetArray<T>{offset, values, cond};
					}
				case CondDWithin:
				case CondLike:
				default:
					throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<T>()};
			}
		} else {
			switch (cond) {
				case CondAny:
					if (!distinct) {
						throw Error{errQueryExec, "Condition %s with not array field", CondTypeToStr(cond)};
					}
					if (rawData) {
						return ComparatorIndexedColumnScalarAnyDistinct<T>{rawData};
					}
					return ComparatorIndexedOffsetScalarAnyDistinct<T>{offset};
				case CondEq:
				case CondSet:
				case CondAllSet:
					if (values.size() == 1) {
						if (distinct) {
							if (rawData) {
								return ComparatorIndexedColumnScalarDistinct<T>{rawData, values, CondEq};
							}
							return ComparatorIndexedOffsetScalarDistinct<T>{offset, values, CondEq};
						}
						if (rawData) {
							return ComparatorIndexedColumnScalar<T>{rawData, values, CondEq};
						}
						return ComparatorIndexedOffsetScalar<T>{offset, values, CondEq};
					} else if (cond == CondAllSet) {
						if (distinct) {
							if (rawData) {
								return ComparatorIndexedColumnScalarDistinct<T>{rawData, values, CondAllSet};
							}
							return ComparatorIndexedOffsetScalarDistinct<T>{offset, values, CondAllSet};
						}
						if (rawData) {
							return ComparatorIndexedColumnScalar<T>{rawData, values, CondAllSet};
						}
						return ComparatorIndexedOffsetScalar<T>{offset, values, CondAllSet};
					} else {
						if (distinct) {
							if (rawData) {
								return ComparatorIndexedColumnScalarDistinct<T>{rawData, values, CondSet};
							}
							return ComparatorIndexedOffsetScalarDistinct<T>{offset, values, CondSet};
						}
						if (rawData) {
							return ComparatorIndexedColumnScalar<T>{rawData, values, CondSet};
						}
						return ComparatorIndexedOffsetScalar<T>{offset, values, CondSet};
					}
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondRange:
					if (distinct) {
						if (rawData) {
							return ComparatorIndexedColumnScalarDistinct<T>{rawData, values, cond};
						}
						return ComparatorIndexedOffsetScalarDistinct<T>{offset, values, cond};
					}
					if (rawData) {
						return ComparatorIndexedColumnScalar<T>{rawData, values, cond};
					}
					return ComparatorIndexedOffsetScalar<T>{offset, values, cond};
				case CondEmpty:
				case CondDWithin:
				case CondLike:
				default:
					throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<T>()};
			}
		}
	}
}

template comparators::ComparatorIndexedVariant<int> ComparatorIndexed<int>::createImpl(CondType, const VariantArray&, const void*, bool,
																					   bool, const PayloadType&, const FieldsSet&,
																					   const CollateOpts&);
template comparators::ComparatorIndexedVariant<int64_t> ComparatorIndexed<int64_t>::createImpl(CondType, const VariantArray&, const void*,
																							   bool, bool, const PayloadType&,
																							   const FieldsSet&, const CollateOpts&);
template comparators::ComparatorIndexedVariant<double> ComparatorIndexed<double>::createImpl(CondType, const VariantArray&, const void*,
																							 bool, bool, const PayloadType&,
																							 const FieldsSet&, const CollateOpts&);
template comparators::ComparatorIndexedVariant<bool> ComparatorIndexed<bool>::createImpl(CondType, const VariantArray&, const void*, bool,
																						 bool, const PayloadType&, const FieldsSet&,
																						 const CollateOpts&);
template comparators::ComparatorIndexedVariant<Uuid> ComparatorIndexed<Uuid>::createImpl(CondType, const VariantArray&, const void*, bool,
																						 bool, const PayloadType&, const FieldsSet&,
																						 const CollateOpts&);

template <>
[[nodiscard]] comparators::ComparatorIndexedVariant<key_string> ComparatorIndexed<key_string>::createImpl(
	CondType cond, const VariantArray& values, const void* rawData, bool distinct, bool isArray, const PayloadType& payloadType,
	const FieldsSet& fields, const CollateOpts& collate) {
	using namespace comparators;
	if (fields.getTagsPathsLength() != 0) {
		switch (cond) {
			case CondEmpty:
				return ComparatorIndexedJsonPathEmpty{fields.getTagsPath(0), payloadType};
			case CondAny:
				if (distinct) {
					return ComparatorIndexedJsonPathAnyStringDistinct{fields.getTagsPath(0), payloadType, collate};
				} else {
					return ComparatorIndexedJsonPathAny{fields.getTagsPath(0), payloadType};
				}
			case CondEq:
			case CondSet:
			case CondAllSet:
				if (values.size() == 1) {
					if (distinct) {
						return ComparatorIndexedJsonPathStringDistinct{fields.getTagsPath(0), payloadType, values, collate, CondEq};
					} else {
						return ComparatorIndexedJsonPathString{fields.getTagsPath(0), payloadType, values, collate, CondEq};
					}
				} else if (cond == CondAllSet) {
					if (distinct) {
						return ComparatorIndexedJsonPathStringDistinct{fields.getTagsPath(0), payloadType, values, collate, CondAllSet};
					} else {
						return ComparatorIndexedJsonPathString{fields.getTagsPath(0), payloadType, values, collate, CondAllSet};
					}
				} else {
					if (distinct) {
						return ComparatorIndexedJsonPathStringDistinct{fields.getTagsPath(0), payloadType, values, collate, CondSet};
					} else {
						return ComparatorIndexedJsonPathString{fields.getTagsPath(0), payloadType, values, collate, CondSet};
					}
				}
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondRange:
			case CondLike:
				if (distinct) {
					return ComparatorIndexedJsonPathStringDistinct{fields.getTagsPath(0), payloadType, values, collate, cond};
				} else {
					return ComparatorIndexedJsonPathString{fields.getTagsPath(0), payloadType, values, collate, cond};
				}
			case CondDWithin:
			default:
				throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<key_string>()};
		}
	} else {
		const auto offset = payloadType->Field(fields[0]).Offset();
		if (isArray) {
			switch (cond) {
				case CondEmpty:
					return ComparatorIndexedOffsetArrayEmpty{offset};
				case CondAny:
					if (distinct) {
						return ComparatorIndexedOffsetArrayAnyStringDistinct{offset, collate};
					} else {
						return ComparatorIndexedOffsetArrayAny{offset};
					}
				case CondEq:
				case CondSet:
				case CondAllSet:
					if (values.size() == 1) {
						if (distinct) {
							return ComparatorIndexedOffsetArrayStringDistinct{offset, values, collate, CondEq};
						} else {
							return ComparatorIndexedOffsetArrayString{offset, values, collate, CondEq};
						}
					} else if (cond == CondAllSet) {
						if (distinct) {
							return ComparatorIndexedOffsetArrayStringDistinct{offset, values, collate, CondAllSet};
						} else {
							return ComparatorIndexedOffsetArrayString{offset, values, collate, CondAllSet};
						}
					} else {
						if (distinct) {
							return ComparatorIndexedOffsetArrayStringDistinct{offset, values, collate, CondSet};
						} else {
							return ComparatorIndexedOffsetArrayString{offset, values, collate, CondSet};
						}
					}
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondRange:
				case CondLike:
					if (distinct) {
						return ComparatorIndexedOffsetArrayStringDistinct{offset, values, collate, cond};
					} else {
						return ComparatorIndexedOffsetArrayString{offset, values, collate, cond};
					}
				case CondDWithin:
				default:
					throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<key_string>()};
			}
		} else {
			switch (cond) {
				case CondAny:
					if (!distinct) {
						throw Error{errQueryExec, "Condition %s with not array field", CondTypeToStr(cond)};
					}
					return ComparatorIndexedOffsetScalarAnyStringDistinct{offset, collate};
				case CondEq:
				case CondSet:
				case CondAllSet:
					if (values.size() == 1) {
						if (distinct) {
							if (rawData) {
								return ComparatorIndexedColumnScalarStringDistinct{rawData, values, collate, CondEq};
							}
							return ComparatorIndexedOffsetScalarStringDistinct{offset, values, collate, CondEq};
						}
						if (rawData) {
							return ComparatorIndexedColumnScalarString{rawData, values, collate, CondEq};
						}
						return ComparatorIndexedOffsetScalarString{offset, values, collate, CondEq};
					} else if (cond == CondAllSet) {
						if (distinct) {
							if (rawData) {
								return ComparatorIndexedColumnScalarStringDistinct{rawData, values, collate, CondAllSet};
							}
							return ComparatorIndexedOffsetScalarStringDistinct{offset, values, collate, CondAllSet};
						}
						if (rawData) {
							return ComparatorIndexedColumnScalarString{rawData, values, collate, CondAllSet};
						}
						return ComparatorIndexedOffsetScalarString{offset, values, collate, CondAllSet};
					} else {
						if (distinct) {
							if (rawData) {
								return ComparatorIndexedColumnScalarStringDistinct{rawData, values, collate, CondSet};
							}
							return ComparatorIndexedOffsetScalarStringDistinct{offset, values, collate, CondSet};
						}
						if (rawData) {
							return ComparatorIndexedColumnScalarString{rawData, values, collate, CondSet};
						}
						return ComparatorIndexedOffsetScalarString{offset, values, collate, CondSet};
					}
				case CondLt:
				case CondLe:
				case CondGt:
				case CondGe:
				case CondRange:
				case CondLike:
					if (distinct) {
						if (rawData) {
							return ComparatorIndexedColumnScalarStringDistinct{rawData, values, collate, cond};
						}
						return ComparatorIndexedOffsetScalarStringDistinct{offset, values, collate, cond};
					}
					if (rawData) {
						return ComparatorIndexedColumnScalarString{rawData, values, collate, cond};
					}
					return ComparatorIndexedOffsetScalarString{offset, values, collate, cond};
				case CondEmpty:
				case CondDWithin:
				default:
					throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<key_string>()};
			}
		}
	}
}

template <>
[[nodiscard]] comparators::ComparatorIndexedVariant<PayloadValue> ComparatorIndexed<PayloadValue>::createImpl(
	CondType cond, const VariantArray& values, const void* /*rawData*/, bool distinct, bool isArray, const PayloadType& payloadType,
	const FieldsSet& fields, const CollateOpts& collate) {
	using namespace comparators;
	if (isArray) {
		throw Error{errQueryExec, "Array composite index"};
	}
	if (distinct) {
		throw Error{errQueryExec, "Distinct with composite index"};
	}
	switch (cond) {
		case CondEq:
		case CondSet:
		case CondAllSet:
			if (values.size() == 1) {
				return ComparatorIndexedComposite{values, collate, fields, payloadType, CondEq};
			} else if (cond == CondAllSet) {
				return ComparatorIndexedComposite{values, collate, fields, payloadType, CondAllSet};
			} else {
				return ComparatorIndexedComposite{values, collate, fields, payloadType, CondSet};
			}
		case CondLt:
		case CondLe:
		case CondGt:
		case CondGe:
		case CondRange:
			return ComparatorIndexedComposite{values, collate, fields, payloadType, cond};
		case CondLike:
		case CondEmpty:
		case CondAny:
		case CondDWithin:
		default:
			throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<PayloadValue>()};
	}
}

template <>
[[nodiscard]] comparators::ComparatorIndexedVariant<Point> ComparatorIndexed<Point>::createImpl(
	CondType cond, const VariantArray& values, const void* /*rawData*/, bool distinct, bool isArray, const PayloadType& payloadType,
	const FieldsSet& fields, const CollateOpts&) {
	using namespace comparators;
	if (fields.getTagsPathsLength() != 0) {
		switch (cond) {
			case CondDWithin:
				if (distinct) {
					return ComparatorIndexedJsonPathDWithinDistinct{fields, payloadType, values};
				} else {
					return ComparatorIndexedJsonPathDWithin{fields, payloadType, values};
				}
			case CondEmpty:
			case CondAny:
			case CondEq:
			case CondSet:
			case CondAllSet:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondRange:
			case CondLike:
			default:
				throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<Point>()};
		}
	} else if (isArray) {
		const auto offset = payloadType->Field(fields[0]).Offset();
		switch (cond) {
			case CondAny:
				if (!distinct) {
					throw Error{errQueryExec, "Condition %s with not array field", CondTypeToStr(cond)};
				}
				return ComparatorIndexedOffsetArrayAnyDistinct<Point>{offset};
			case CondDWithin:
				if (distinct) {
					return ComparatorIndexedOffsetArrayDWithinDistinct{offset, values};
				} else {
					return ComparatorIndexedOffsetArrayDWithin{offset, values};
				}
			case CondEmpty:
			case CondEq:
			case CondSet:
			case CondAllSet:
			case CondLt:
			case CondLe:
			case CondGt:
			case CondGe:
			case CondRange:
			case CondLike:
			default:
				throw Error{errQueryExec, "Condition %s with type %s", CondTypeToStr(cond), typeToStr<Point>()};
		}
	} else {
		throw Error{errQueryExec, "Condition %s with not array field", CondTypeToStr(cond)};
	}
}

}  // namespace reindexer
