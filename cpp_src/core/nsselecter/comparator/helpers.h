#pragma once

#include "core/index/string_map.h"
#include "core/keyvalue/key_string.h"
#include "core/type_consts_helpers.h"
#include "vendor/hopscotch/hopscotch_sc_map.h"
#include "vendor/hopscotch/hopscotch_sc_set.h"

namespace reindexer {

namespace comparators {

template <typename T>
struct [[nodiscard]] DataTypeImpl {
	using type = T;
};
template <>
struct [[nodiscard]] DataTypeImpl<key_string> {
	using type = p_string;
};

template <typename T>
using DataType = typename DataTypeImpl<T>::type;

class [[nodiscard]] key_string_set : public tsl::hopscotch_sc_set<key_string, hash_key_string, equal_key_string, less_key_string> {
public:
	key_string_set(const CollateOpts& opts)
		: tsl::hopscotch_sc_set<key_string, hash_key_string, equal_key_string, less_key_string>(
			  1000, hash_key_string(CollateMode(opts.mode)), equal_key_string(opts), std::allocator<key_string>(), less_key_string(opts)) {}
};

template <typename T>
class [[nodiscard]] key_string_map : public tsl::hopscotch_sc_map<key_string, T, hash_key_string, equal_key_string, less_key_string> {
public:
	key_string_map(const CollateOpts& opts)
		: tsl::hopscotch_sc_map<key_string, T, hash_key_string, equal_key_string, less_key_string>(
			  1000, hash_key_string(CollateMode(opts.mode)), equal_key_string(opts), std::allocator<key_string>(), less_key_string(opts)) {}
};

template <CondType Cond>
std::string_view CondToStr() {
	using namespace std::string_view_literals;
	if constexpr (Cond == CondEq) {
		return "="sv;
	} else if constexpr (Cond == CondLt) {
		return "<"sv;
	} else if constexpr (Cond == CondLe) {
		return "<="sv;
	} else if constexpr (Cond == CondGt) {
		return ">"sv;
	} else if constexpr (Cond == CondGe) {
		return ">="sv;
	} else if constexpr (Cond == CondSet) {
		return "IN"sv;
	} else if constexpr (Cond == CondAllSet) {
		return "ALLSET"sv;
	} else {
		static_assert(Cond == CondEq || Cond == CondLt || Cond == CondLe || Cond == CondGt || Cond == CondGe || Cond == CondSet ||
					  Cond == CondAllSet);
	}
}

template <typename T>
T GetValue(const Variant& value) {
	if constexpr (std::is_same_v<T, PayloadValue>) {
		return static_cast<const PayloadValue&>(value);
	} else if constexpr (std::is_same_v<T, Point>) {
		return static_cast<T>(value);
	} else if constexpr (std::is_same_v<T, key_string>) {
		return static_cast<key_string>(value.convert(KeyValueType::String{}));
	} else if constexpr (std::is_same_v<T, Variant>) {
		return value;
	} else {
		return value.convert(KeyValueType::From<T>()).template As<T>();
	}
}

template <typename T>
T GetValue(CondType cond, const VariantArray& values, size_t i) {
	if (values.size() <= i) {
		throw Error{errQueryExec, "Too many arguments for condition {}", CondTypeToStr(cond)};
	}
	const auto& val = values[i];
	assertrx_throw(!val.IsNullValue());
	return GetValue<T>(val);
}

}  // namespace comparators

}  // namespace reindexer
