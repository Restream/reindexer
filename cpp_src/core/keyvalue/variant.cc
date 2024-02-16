#include "variant.h"
#include <charconv>
#include <functional>

#include "core/payload/payloadiface.h"
#include "estl/overloaded.h"
#include "estl/tuple_utils.h"
#include "geometry.h"
#include "key_string.h"
#include "p_string.h"
#include "tools/compare.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"
#include "uuid.h"
#include "vendor/double-conversion/double-conversion.h"

namespace reindexer {

Variant::Variant(const PayloadValue &v) noexcept : variant_{0, 1, KeyValueType::Composite{}} { new (cast<void>()) PayloadValue(v); }

Variant::Variant(PayloadValue &&v) noexcept : variant_{0, 1, KeyValueType::Composite{}} { new (cast<void>()) PayloadValue(std::move(v)); }

Variant::Variant(const std::string &v) : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(make_key_string(v)); }

Variant::Variant(std::string &&v) : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(make_key_string(std::move(v))); }

Variant::Variant(std::string_view v) : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(make_key_string(v)); }

Variant::Variant(const key_string &v) noexcept : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(v); }

Variant::Variant(key_string &&v) noexcept : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(std::move(v)); }

Variant::Variant(const char *v) noexcept : Variant(p_string(v), Variant::no_hold_t{}) {}

Variant::Variant(p_string v, no_hold_t) noexcept : variant_{0, 0, KeyValueType::String{}} { *cast<p_string>() = v; }

Variant::Variant(p_string v, hold_t) : variant_{0, 0, KeyValueType::String{}} {
	if (v.type() == p_string::tagKeyString) {
		variant_.hold = 1;
		new (cast<void>()) key_string(v.getKeyString());
	} else {
		*cast<p_string>() = v;
	}
}
Variant::Variant(p_string v) noexcept : Variant(v, no_hold_t{}) {}

Variant::Variant(const VariantArray &values) : variant_{0, 1, KeyValueType::Tuple{}} {
	WrSerializer ser;
	ser.PutVarUint(values.size());
	for (const Variant &kv : values) {
		ser.PutVariant(kv);
	}
	new (cast<void>()) key_string(make_key_string(ser.Slice()));
}

Variant::Variant(Point p) noexcept : Variant{VariantArray{p}} {}

Variant::Variant(Uuid uuid) noexcept : uuid_() {
	if (uuid.data_[0] == 0 && uuid.data_[1] == 0) {
		uuid_.~UUID();
		new (&variant_) Var(0, 0, KeyValueType::Uuid{});
	} else {
		uuid_.isUuid = 1;
		uuid_.v0 = (uuid.data_[0] >> (64 - 7));
		for (unsigned i = 0; i < 7; ++i) {
			uuid_.vs[i] = (uuid.data_[0] >> (64 - 15 - 8 * i));
		}
		uuid_.v1 = (uuid.data_[1] & ((uint64_t(1) << 63) - uint64_t(1)));
		uuid_.v1 |= ((uuid.data_[0] & uint64_t(1)) << 63);
	}
}

static void serialize(WrSerializer &, const std::tuple<> &) noexcept {}

template <typename... Ts>
void serialize(WrSerializer &ser, const std::tuple<Ts...> &v) {
	ser.PutVariant(Variant{std::get<0>(v)});
	serialize(ser, tail(v));
}

template <typename... Ts>
Variant::Variant(const std::tuple<Ts...> &values) : variant_{0, 1, KeyValueType::Tuple{}} {
	WrSerializer ser;
	ser.PutVarUint(sizeof...(Ts));
	serialize(ser, values);
	new (cast<void>()) key_string(make_key_string(ser.Slice()));
}
template Variant::Variant(const std::tuple<int, std::string> &);
template Variant::Variant(const std::tuple<std::string, int> &);

template <typename T>
inline static void assertKeyType([[maybe_unused]] KeyValueType got) noexcept {
	assertf(got.Is<T>(), "Expected value '%s', but got '%s'", KeyValueType{T{}}.Name(), got.Name());
}

Variant::operator int() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::Int>(variant_.type);
	return variant_.value_int;
}

Variant::operator bool() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::Bool>(variant_.type);
	return variant_.value_bool;
}

Variant::operator int64_t() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::Int64>(variant_.type);
	return variant_.value_int64;
}

Variant::operator double() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::Double>(variant_.type);
	return variant_.value_double;
}

Variant::operator Point() const { return static_cast<Point>(getCompositeValues()); }
template <>
Point Variant::As<Point>() const {
	assertrx(!isUuid());
	if (!variant_.type.Is<KeyValueType::Tuple>()) throw Error(errParams, "Can't convert %s to Point", variant_.type.Name());
	return static_cast<Point>(getCompositeValues());
}
template <>
Uuid Variant::As<Uuid>() const {
	if (isUuid()) return Uuid{*this};
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Uuid) { return Uuid{*this}; }, [&](KeyValueType::String) { return Uuid{this->As<std::string>()}; },
		[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double, KeyValueType::Tuple,
				  KeyValueType::Composite, KeyValueType::Null, KeyValueType::Undefined>) -> Uuid {
			throw Error(errParams, "Can't convert %s to UUID", variant_.type.Name());
		});
}

void Variant::free() noexcept {
	assertrx(!isUuid());
	assertrx(variant_.hold == 1);
	variant_.type.EvaluateOneOf([&](OneOf<KeyValueType::String, KeyValueType::Tuple>) noexcept { this->cast<key_string>()->~key_string(); },
								[&](KeyValueType::Composite) noexcept { this->cast<PayloadValue>()->~PayloadValue(); },
								[](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Null,
										 KeyValueType::Undefined, KeyValueType::Double, KeyValueType::Uuid>) noexcept {});
	variant_.hold = 0;
}

void Variant::copy(const Variant &other) {
	assertrx(!isUuid());
	assertrx(!other.isUuid());
	assertrx(variant_.hold == 1);
	variant_.type.EvaluateOneOf(
		[&](OneOf<KeyValueType::String, KeyValueType::Tuple>) { new (this->cast<void>()) key_string(*other.cast<key_string>()); },
		[&](KeyValueType::Composite) { new (this->cast<void>()) PayloadValue(*other.cast<PayloadValue>()); },
		[&](KeyValueType::Int) noexcept { variant_.value_int = other.variant_.value_int; },
		[&](KeyValueType::Bool) noexcept { variant_.value_bool = other.variant_.value_bool; },
		[&](OneOf<KeyValueType::Int64, KeyValueType::Undefined>) noexcept { variant_.value_uint64 = other.variant_.value_uint64; },
		[&](KeyValueType::Double) noexcept { variant_.value_double = other.variant_.value_double; },
		[&](OneOf<KeyValueType::Null, KeyValueType::Uuid>) noexcept {});
}

Variant &Variant::EnsureHold() & {
	if (isUuid() || variant_.hold == 1) return *this;
	variant_.type.EvaluateOneOf([&](OneOf<KeyValueType::String, KeyValueType::Tuple>) { *this = Variant(this->operator key_string()); },
								[&](KeyValueType::Composite) { *this = Variant(this->operator const PayloadValue &()); },
								[](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Null,
										 KeyValueType::Undefined, KeyValueType::Double, KeyValueType::Uuid>) noexcept {});
	return *this;
}

template <>
std::string Variant::As<std::string>() const {
	using namespace std::string_literals;
	if (isUuid()) {
		return std::string{Uuid{*this}};
	} else {
		return variant_.type.EvaluateOneOf(
			[&](KeyValueType::Int) { return std::to_string(variant_.value_int); },
			[&](KeyValueType::Bool) { return variant_.value_bool ? "true"s : "false"s; },
			[&](KeyValueType::Int64) { return std::to_string(variant_.value_int64); },
			[&](KeyValueType::Double) { return std::to_string(variant_.value_double); },
			[&](KeyValueType::String) {
				const auto pstr = this->operator p_string();
				if (pstr.type() == p_string::tagCxxstr || pstr.type() == p_string::tagKeyString) {
					return *(pstr.getCxxstr());
				}
				return pstr.toString();
			},
			[&](KeyValueType::Null) { return "null"s; }, [&](KeyValueType::Composite) { return std::string(); },
			[&](KeyValueType::Tuple) {
				auto va = getCompositeValues();
				WrSerializer wrser;
				va.Dump(wrser);
				return std::string(wrser.Slice());
			},
			[&](KeyValueType::Uuid) { return std::string{Uuid{*this}}; }, [](KeyValueType::Undefined) -> std::string { abort(); });
	}
}

template <>
p_string Variant::As<p_string>() const {
	if (!isUuid() && variant_.type.Is<KeyValueType::String>()) {
		return this->operator p_string();
	} else {
		return p_string{make_key_string(As<std::string>())};
	}
}

template <>
std::string Variant::As<std::string>(const PayloadType &pt, const FieldsSet &fields) const {
	if (!isUuid() && variant_.type.Is<KeyValueType::Composite>()) {
		ConstPayload pl(pt, operator const PayloadValue &());
		VariantArray va;
		size_t tagsPathIdx = 0;
		for (auto field : fields) {
			bool fieldFromCjson = (field == IndexValueType::SetByJsonPath);
			VariantArray va1;
			if (fieldFromCjson) {
				assertrx(tagsPathIdx < fields.getTagsPathsLength());
				pl.GetByJsonPath(fields.getTagsPath(tagsPathIdx++), va1, variant_.type);
			} else {
				pl.Get(field, va1);
			}
			va.insert(va.end(), va1.begin(), va1.end());
		}
		WrSerializer wrser;
		va.Dump(wrser);
		return std::string(wrser.Slice());
	} else {
		return As<std::string>();
	}
}

template <typename T>
std::optional<T> tryParseAs(std::string_view str) noexcept {
	auto begin = str.data();
	const auto end = begin + str.size();
	while (begin != end && std::isspace(*begin)) {
		++begin;
	}
	T res;
	auto [ptr, err] = std::from_chars(begin, end, res);
	if (ptr == begin || err == std::errc::invalid_argument || err == std::errc::result_out_of_range) {
		return std::nullopt;
	}
	for (; ptr != end; ++ptr) {
		if (!std::isspace(*ptr)) {
			return std::nullopt;
		}
	}
	return res;
}

template <>
std::optional<double> tryParseAs<double>(std::string_view str) noexcept {
	if (str.empty()) return 0.0;
	using namespace double_conversion;
	static const StringToDoubleConverter converter{StringToDoubleConverter::ALLOW_LEADING_SPACES |
													   StringToDoubleConverter::ALLOW_TRAILING_SPACES |
													   StringToDoubleConverter::ALLOW_SPACES_AFTER_SIGN,
												   NAN, NAN, nullptr, nullptr};
	double res;
	try {
		int countOfCharsParsedAsDouble;
		res = converter.StringToDouble(str.data(), str.size(), &countOfCharsParsedAsDouble);
	} catch (...) {
		return std::nullopt;
	}
	if (std::isnan(res)) {
		return std::nullopt;
	}
	return res;
}

template <typename T>
T parseAs(std::string_view str) {
	const auto res = tryParseAs<T>(str);
	if (res) {
		return *res;
	} else {
		throw Error(errParams, "Can't convert '%s' to number", str);
	}
}

template <>
int Variant::As<int>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '%s' to number", std::string{Uuid{*this}}.data());
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> int { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept -> int { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept -> int { return variant_.value_double; },
		[&](KeyValueType::String) { return parseAs<int>(this->operator p_string()); },
		[](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return 0; },
		[&](KeyValueType::Uuid) -> int { throw Error(errParams, "Can't convert '%s' to number", std::string{Uuid{*this}}.data()); },
		[](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> int { abort(); });
}

template <>
bool Variant::As<bool>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '%s' to bool", std::string{Uuid{*this}}.data());
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept -> bool { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept -> bool { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept -> bool { return variant_.value_double; },
		[&](KeyValueType::String) noexcept { return std::string_view(this->operator p_string()) == "true"; },
		[](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return false; },
		[&](KeyValueType::Uuid) -> bool { throw Error(errParams, "Can't convert '%s' to bool", std::string{Uuid{*this}}.data()); },
		[](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> bool { abort(); });
}

template <>
int64_t Variant::As<int64_t>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '%s' to number", std::string{Uuid{*this}}.data());
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> int64_t { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept -> int64_t { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept -> int64_t { return variant_.value_double; },
		[&](KeyValueType::String) { return parseAs<int64_t>(this->operator p_string()); },
		[](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept -> int64_t { return 0; },
		[&](KeyValueType::Uuid) -> int64_t { throw Error(errParams, "Can't convert '%s' to number", std::string{Uuid{*this}}.data()); },
		[](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> int64_t { abort(); });
}

template <>
double Variant::As<double>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '%s' to number", std::string{Uuid{*this}}.data());
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> double { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept -> double { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept -> double { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept { return variant_.value_double; },
		[&](KeyValueType::String) { return parseAs<double>(this->operator p_string()); },
		[](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return 0.0; },
		[&](KeyValueType::Uuid) -> double { throw Error(errParams, "Can't convert '%s' to number", std::string{Uuid{*this}}.data()); },
		[](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> double { abort(); });
}

int Variant::Compare(const Variant &other, const CollateOpts &collateOpts) const {
	if (isUuid()) {
		assertrx(other.Type().Is<KeyValueType::Uuid>());
		return Uuid{*this}.Compare(Uuid{other});
	} else {
		assertrx(Type().IsSame(other.Type()));
		return variant_.type.EvaluateOneOf(
			[&](KeyValueType::Int) noexcept {
				return (variant_.value_int == other.variant_.value_int) ? 0 : (variant_.value_int > other.variant_.value_int) ? 1 : -1;
			},
			[&](KeyValueType::Bool) noexcept {
				return (variant_.value_bool == other.variant_.value_bool) ? 0 : (variant_.value_bool > other.variant_.value_bool) ? 1 : -1;
			},
			[&](KeyValueType::Int64) noexcept {
				return (variant_.value_int64 == other.variant_.value_int64)	 ? 0
					   : (variant_.value_int64 > other.variant_.value_int64) ? 1
																			 : -1;
			},
			[&](KeyValueType::Double) noexcept {
				return (variant_.value_double == other.variant_.value_double)  ? 0
					   : (variant_.value_double > other.variant_.value_double) ? 1
																			   : -1;
			},
			[&](KeyValueType::Tuple) -> int { throw Error(errParams, "KeyValueType::Tuple comparison is not implemented"); },
			[&](KeyValueType::String) { return collateCompare(this->operator p_string(), other.operator p_string(), collateOpts); },
			[&](KeyValueType::Uuid) { return Uuid{*this}.Compare(Uuid{other}); },
			[](KeyValueType::Null) -> int {
				throw Error{errParams, "Cannot compare empty values"};
			},
			[](OneOf<KeyValueType::Composite, KeyValueType::Undefined>) noexcept -> int { abort(); });
	}
}

int Variant::relaxCompareWithString(std::string_view str) const {
	thread_local char uuidStrBuf[Uuid::kStrFormLen];
	thread_local const std::string_view uuidStrBufView{uuidStrBuf, Uuid::kStrFormLen};
	if (isUuid()) {
		Uuid{*this}.PutToStr(uuidStrBuf);
		return uuidStrBufView == str ? 0 : (uuidStrBufView < str ? -1 : 1);
	} else {
		return variant_.type.EvaluateOneOf(
			[&](KeyValueType::Int) {
				const auto value = tryParseAs<int64_t>(str);
				if (value) {
					return compare(variant_.value_int, *value);
				} else {
					const double v = parseAs<double>(str);
					return compare(variant_.value_int, v);
				}
			},
			[&](KeyValueType::Int64) {
				const auto value = tryParseAs<int64_t>(str);
				if (value) {
					return compare(variant_.value_int64, *value);
				} else {
					const double v = parseAs<double>(str);
					return compare(variant_.value_int64, v);
				}
			},
			[&](KeyValueType::Double) {
				const double value = parseAs<double>(str);
				return compare(variant_.value_double, value);
			},
			[&](KeyValueType::Uuid) {
				Uuid{*this}.PutToStr(uuidStrBuf);
				return uuidStrBufView == str ? 0 : (uuidStrBufView < str ? -1 : 1);
			},
			[&](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined,
					  KeyValueType::Null>) -> int {
				throw Error(errParams, "Not comparable types: %s and %s", KeyValueType{KeyValueType::String{}}.Name(), Type().Name());
			});
	}
}

class Comparator {
public:
	explicit Comparator(const Variant &v1, const Variant &v2) noexcept : v1_{v1}, v2_{v2} {}
	int operator()(KeyValueType::Bool, KeyValueType::Bool) const noexcept { return compare(v1_.As<bool>(), v2_.As<bool>()); }
	int operator()(KeyValueType::Bool, KeyValueType::Int) const noexcept { return compare(v1_.As<bool>(), v2_.As<int>()); }
	int operator()(KeyValueType::Bool, KeyValueType::Int64) const noexcept { return compare(v1_.As<bool>(), v2_.As<int64_t>()); }
	int operator()(KeyValueType::Bool, KeyValueType::Double) const noexcept { return compare(v1_.As<bool>(), v2_.As<double>()); }
	int operator()(KeyValueType::Int, KeyValueType::Bool) const noexcept { return compare(v1_.As<int>(), v2_.As<bool>()); }
	int operator()(KeyValueType::Int, KeyValueType::Int) const noexcept { return compare(v1_.As<int>(), v2_.As<int>()); }
	int operator()(KeyValueType::Int, KeyValueType::Int64) const noexcept { return compare(v1_.As<int>(), v2_.As<int64_t>()); }
	int operator()(KeyValueType::Int, KeyValueType::Double) const noexcept { return compare(v1_.As<int>(), v2_.As<double>()); }
	int operator()(KeyValueType::Int64, KeyValueType::Bool) const noexcept { return compare(v1_.As<int64_t>(), v2_.As<bool>()); }
	int operator()(KeyValueType::Int64, KeyValueType::Int) const noexcept { return compare(v1_.As<int64_t>(), v2_.As<int>()); }
	int operator()(KeyValueType::Int64, KeyValueType::Int64) const noexcept { return compare(v1_.As<int64_t>(), v2_.As<int64_t>()); }
	int operator()(KeyValueType::Int64, KeyValueType::Double) const noexcept { return compare(v1_.As<int64_t>(), v2_.As<double>()); }
	int operator()(KeyValueType::Double, KeyValueType::Bool) const noexcept { return compare(v1_.As<double>(), v2_.As<bool>()); }
	int operator()(KeyValueType::Double, KeyValueType::Int) const noexcept { return compare(v1_.As<double>(), v2_.As<int>()); }
	int operator()(KeyValueType::Double, KeyValueType::Int64) const noexcept { return compare(v1_.As<double>(), v2_.As<int64_t>()); }
	int operator()(KeyValueType::Double, KeyValueType::Double) const noexcept { return compare(v1_.As<double>(), v2_.As<double>()); }
	int operator()(OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
						 KeyValueType::Uuid>,
				   OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
						 KeyValueType::Uuid>) const noexcept {
		assertrx(0);
		abort();
	}
	int operator()(OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>,
				   OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
						 KeyValueType::Uuid>) const noexcept {
		assertrx(0);
		abort();
	}
	int operator()(OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple,
						 KeyValueType::Uuid>,
				   OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) const noexcept {
		assertrx(0);
		abort();
	}

private:
	const Variant &v1_;
	const Variant &v2_;
};

template <WithString withString>
int Variant::RelaxCompare(const Variant &other, const CollateOpts &collateOpts) const {
	thread_local char uuidStrBuf[Uuid::kStrFormLen];
	thread_local const std::string_view uuidStrBufView{uuidStrBuf, Uuid::kStrFormLen};
	thread_local const p_string uuidStrBufPString{&uuidStrBufView};
	if (isUuid() || variant_.type.Is<KeyValueType::Uuid>()) {
		if (other.isUuid() || other.variant_.type.Is<KeyValueType::Uuid>()) {
			return Uuid{*this}.Compare(Uuid{other});
		} else if (other.variant_.type.Is<KeyValueType::String>()) {
			const auto otherUuid = Uuid::TryParse(other.As<p_string>());
			if (otherUuid) {
				return Uuid{*this}.Compare(*otherUuid);
			} else {
				Uuid{*this}.PutToStr(uuidStrBuf);
				return -other.Compare(Variant{uuidStrBufPString});
			}
		} else if constexpr (withString == WithString::Yes) {
			Uuid{*this}.PutToStr(uuidStrBuf);
			return -other.relaxCompareWithString(uuidStrBufView);
		} else {
			throw Error(errParams, "Not comparable types: %s and %s", Type().Name(), other.Type().Name());
		}
	} else if (other.isUuid() || other.variant_.type.Is<KeyValueType::Uuid>()) {
		if (variant_.type.Is<KeyValueType::String>()) {
			const auto uuid = Uuid::TryParse(As<p_string>());
			if (uuid) {
				return uuid->Compare(Uuid{other});
			} else {
				Uuid{other}.PutToStr(uuidStrBuf);
				return Compare(Variant{uuidStrBufPString});
			}
		} else if constexpr (withString == WithString::Yes) {
			Uuid{other}.PutToStr(uuidStrBuf);
			return relaxCompareWithString(uuidStrBufView);
		} else {
			throw Error(errParams, "Not comparable types: %s and %s", Type().Name(), other.Type().Name());
		}
	} else {
		if (variant_.type.IsSame(other.variant_.type)) {
			if (variant_.type.Is<KeyValueType::Tuple>()) {
				return getCompositeValues().RelaxCompare<withString>(other.getCompositeValues(), collateOpts);
			} else {
				return Compare(other, collateOpts);
			}
		} else if (variant_.type.IsNumeric() && other.variant_.type.IsNumeric()) {
			return KeyValueType::Visit(Comparator{*this, other}, variant_.type, other.variant_.type);
		} else {
			if constexpr (withString == WithString::Yes) {
				if (other.Type().Is<KeyValueType::String>()) {
					return relaxCompareWithString(other.operator p_string());
				} else if (Type().Is<KeyValueType::String>()) {
					return -other.relaxCompareWithString(this->operator p_string());
				}
			}
			throw Error(errParams, "Not comparable types: %s and %s", Type().Name(), other.Type().Name());
		}
	}
}

template int Variant::RelaxCompare<WithString::Yes>(const Variant &, const CollateOpts &) const;
template int Variant::RelaxCompare<WithString::No>(const Variant &, const CollateOpts &) const;

size_t Variant::Hash() const noexcept {
	if (isUuid()) {
		return std::hash<Uuid>()(Uuid{*this});
	} else {
		return variant_.type.EvaluateOneOf(
			[&](KeyValueType::Int) noexcept { return std::hash<int>()(variant_.value_int); },
			[&](KeyValueType::Bool) noexcept { return std::hash<bool>()(variant_.value_bool); },
			[&](KeyValueType::Int64) noexcept { return std::hash<int64_t>()(variant_.value_int64); },
			[&](KeyValueType::Double) noexcept { return std::hash<double>()(variant_.value_double); },
			[&](KeyValueType::String) noexcept { return std::hash<p_string>()(this->operator p_string()); },
			[&](KeyValueType::Uuid) noexcept { return std::hash<Uuid>()(Uuid{*this}); },
			[&](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null>) noexcept -> size_t {
#ifdef NDEBUG
				abort();
#else
				assertf(false, "Unexpected variant type: %s", variant_.type.Name());
#endif
			});
	}
}

void Variant::EnsureUTF8() const {
	if (!isUuid() && variant_.type.Is<KeyValueType::String>()) {
		const auto pstr = this->operator p_string();
		if (!utf8::is_valid(pstr.data(), pstr.data() + pstr.size())) {
			throw Error(errParams, "Invalid UTF8 string passed to index with CollateUTF8 mode");
		}
	}
}

Variant Variant::convert(KeyValueType type, const PayloadType *payloadType, const FieldsSet *fields) const & {
	if (Type().IsSame(type)) {
		return *this;
	}
	Variant dst(*this);
	dst.convert(type, payloadType, fields);
	return dst;
}

Variant &Variant::convert(KeyValueType type, const PayloadType *payloadType, const FieldsSet *fields) & {
	if (isUuid()) {
		type.EvaluateOneOf([&](KeyValueType::Uuid) noexcept {}, [&](KeyValueType::String) { *this = Variant{std::string{Uuid{*this}}}; },
						   [&](KeyValueType::Composite) {
							   assertrx_throw(payloadType && fields);
							   Variant tmp{VariantArray{std::move(*this)}};
							   tmp.convertToComposite(*payloadType, *fields);
							   *this = std::move(tmp);
						   },
						   [type](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double,
										KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null>) {
							   throw Error(errParams, "Can't convert Variant from type '%s' to type '%s'",
										   KeyValueType{KeyValueType::Uuid{}}.Name(), type.Name());
						   });
		return *this;
	}
	if (type.IsSame(variant_.type) || type.Is<KeyValueType::Null>() || variant_.type.Is<KeyValueType::Null>()) return *this;
	type.EvaluateOneOf(
		[&](KeyValueType::Int) { *this = Variant(As<int>()); }, [&](KeyValueType::Bool) { *this = Variant(As<bool>()); },
		[&](KeyValueType::Int64) { *this = Variant(As<int64_t>()); }, [&](KeyValueType::Double) { *this = Variant(As<double>()); },
		[&](KeyValueType::String) { *this = Variant(As<std::string>()); },
		[&](KeyValueType::Composite) {
			variant_.type.EvaluateOneOf(
				[&](KeyValueType::Tuple) {
					assertrx(payloadType && fields);
					convertToComposite(*payloadType, *fields);
				},
				[](KeyValueType::Composite) noexcept {},
				[&](OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::String,
						  KeyValueType::Uuid>) {
					assertrx(payloadType && fields);
					Variant tmp{VariantArray{std::move(*this)}};
					tmp.convertToComposite(*payloadType, *fields);
					*this = std::move(tmp);
				},
				[&](OneOf<KeyValueType::Undefined, KeyValueType::Null>) {
					throw Error(errParams, "Can't convert Variant from type '%s' to type '%s'", variant_.type.Name(), type.Name());
				});
		},
		[&](KeyValueType::Uuid) { *this = Variant{As<Uuid>()}; },
		[&](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null>) {
			throw Error(errParams, "Can't convert Variant from type '%s' to type '%s'", variant_.type.Name(), type.Name());
		});
	return *this;
}

void Variant::convertToComposite(const PayloadType &payloadType, const FieldsSet &fields) {
	assertrx(!isUuid());
	assertrx(variant_.type.Is<KeyValueType::Tuple>() && variant_.hold == 1);
	key_string val = *cast<key_string>();

	if (variant_.hold == 1) free();
	// Alloc usual payloadvalue + extra memory for hold string

	auto &pv = *new (cast<void>()) PayloadValue(payloadType.TotalSize() + val->size());
	variant_.hold = 1;
	variant_.type = KeyValueType::Composite{};

	// Copy serializer buffer with strings to extra payloadvalue memory
	char *data = reinterpret_cast<char *>(pv.Ptr() + payloadType.TotalSize());
	memcpy(data, val->data(), val->size());

	Serializer ser(std::string_view(data, val->size()));

	size_t count = ser.GetVarUint();
	if (count != fields.size()) {
		throw Error(errLogic, "Invalid count of arguments for composite index, expected %d, got %d", fields.size(), count);
	}

	Payload pl(payloadType, pv);

	for (auto field : fields) {
		if (field != IndexValueType::SetByJsonPath) {
			pl.Set(field, ser.GetVariant());
		} else {
			// TODO: will have to implement SetByJsonPath in PayloadIFace
			// or this "mixed" composite queries (by ordinary indexes + indexes
			// from cjson) won't work properly.
			throw Error(errConflict, "SetByJsonPath is not implemented yet");
		}
	}
}

VariantArray Variant::getCompositeValues() const {
	assertrx(!isUuid());
	assertrx(variant_.type.Is<KeyValueType::Tuple>());

	VariantArray res;
	Serializer ser(**cast<key_string>());
	size_t count = ser.GetVarUint();
	res.reserve(count);
	while (count--) {
		res.push_back(ser.GetVariant());
	}
	return res;
}

Variant::operator key_string() const {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::String>(variant_.type);
	if (variant_.hold == 1) {
		return *cast<key_string>();
	} else if (cast<p_string>()->type() == p_string::tagKeyString) {
		return cast<p_string>()->getKeyString();
	} else {
		return make_key_string(cast<p_string>()->data(), cast<p_string>()->size());
	}
}

Variant::operator p_string() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::String>(variant_.type);
	return (variant_.hold == 1) ? p_string(*cast<key_string>()) : *cast<p_string>();
}

Variant::operator std::string_view() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::String>(variant_.type);
	return (variant_.hold == 1) ? std::string_view(**cast<key_string>()) : *cast<p_string>();
}
Variant::operator const PayloadValue &() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::Composite>(variant_.type);
	assertrx(variant_.hold == 1);
	return *cast<PayloadValue>();
}

template <typename T>
void Variant::Dump(T &os, CheckIsStringPrintable checkPrintableString) const {
	if (isUuid()) {
		os << Uuid{*this};
	} else {
		variant_.type.EvaluateOneOf(
			[&](KeyValueType::String) {
				p_string str(*this);
				if (checkPrintableString == CheckIsStringPrintable::No || isPrintable(str)) {
					os << '\'' << std::string_view(str) << '\'';
				} else {
					os << "slice{len:" << str.length() << "}";
				}
			},
			[&](KeyValueType::Int) { os << this->operator int(); }, [&](KeyValueType::Bool) { os << this->operator bool(); },
			[&](KeyValueType::Int64) { os << this->operator int64_t(); }, [&](KeyValueType::Double) { os << this->operator double(); },
			[&](KeyValueType::Tuple) { getCompositeValues().Dump(os); }, [&](KeyValueType::Uuid) { os << Uuid{*this}; },
			[&](OneOf<KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null>) { os << "??"; });
	}
}

template void Variant::Dump(WrSerializer &, CheckIsStringPrintable) const;
template void Variant::Dump(std::ostream &, CheckIsStringPrintable) const;
template void Variant::Dump(std::stringstream &, CheckIsStringPrintable) const;

template <typename T>
void VariantArray::Dump(T &os, CheckIsStringPrintable checkPrintableString) const {
	os << '{';
	for (auto &arg : *this) {
		if (&arg != &at(0)) os << ", ";
		arg.Dump(os, checkPrintableString);
	}
	os << '}';
}

template void VariantArray::Dump(WrSerializer &, CheckIsStringPrintable) const;
template void VariantArray::Dump(std::ostream &, CheckIsStringPrintable) const;
template void VariantArray::Dump(std::stringstream &, CheckIsStringPrintable) const;

VariantArray::VariantArray(Point p) noexcept {
	emplace_back(p.X());
	emplace_back(p.Y());
}

VariantArray::operator Point() const {
	if (size() != 2) {
		throw Error(errParams, "Can't convert array of %d elements to Point", size());
	}
	return Point{(*this)[0].As<double>(), (*this)[1].As<double>()};
}

template <WithString withString>
int VariantArray::RelaxCompare(const VariantArray &other, const CollateOpts &collateOpts) const {
	auto lhsIt{cbegin()}, rhsIt{other.cbegin()};
	auto const lhsEnd{cend()}, rhsEnd{other.cend()};
	for (; lhsIt != lhsEnd && rhsIt != rhsEnd; ++lhsIt, ++rhsIt) {
		const auto res = lhsIt->RelaxCompare<withString>(*rhsIt, collateOpts);
		if (res != 0) return res;
	}
	if (lhsIt == lhsEnd) {
		if (rhsIt == rhsEnd) return 0;
		return -1;
	} else {
		return 1;
	}
}

template int VariantArray::RelaxCompare<WithString::Yes>(const VariantArray &, const CollateOpts &) const;
template int VariantArray::RelaxCompare<WithString::No>(const VariantArray &, const CollateOpts &) const;

}  // namespace reindexer
