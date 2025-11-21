#include "variant.h"
#include <charconv>
#include <functional>
#include <sstream>

#include "core/payload/payloadiface.h"
#include "estl/tuple_utils.h"
#include "tools/compare.h"
#include "tools/float_comparison.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "uuid.h"
#include "vendor/double-conversion/double-conversion.h"
#include "vendor/utf8cpp/utf8/core.h"

namespace reindexer {

Variant::Variant(p_string v, HoldT) : variant_{0, 0, KeyValueType::String{}} {
	if (v.type() == p_string::tagKeyString) {
		variant_.hold = 1;
		new (cast<void>()) key_string(v.getKeyString());
	} else {
		*cast<p_string>() = v;
	}
}

Variant::Variant(const VariantArray& values) : variant_{0, 1, KeyValueType::Tuple{}} {
	WrSerializer ser;
	ser.PutVarUint(values.size());
	for (const Variant& kv : values) {
		ser.PutVariant(kv);
	}
	new (cast<void>()) key_string(make_key_string(ser.Slice()));
}

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

static void serialize(WrSerializer&, const std::tuple<>&) noexcept {}

template <typename... Ts>
void serialize(WrSerializer& ser, const std::tuple<Ts...>& v) {
	ser.PutVariant(Variant{std::get<0>(v)});
	serialize(ser, tail(v));
}

template <typename... Ts>
Variant::Variant(const std::tuple<Ts...>& values) : variant_{0, 1, KeyValueType::Tuple{}} {
	WrSerializer ser;
	ser.PutVarUint(sizeof...(Ts));
	serialize(ser, values);
	new (cast<void>()) key_string(make_key_string(ser.Slice()));
}
template Variant::Variant(const std::tuple<int, std::string>&);
template Variant::Variant(const std::tuple<std::string, int>&);

template <typename T>
inline static void assertKeyType([[maybe_unused]] KeyValueType got) noexcept {
	assertf(got.Is<T>(), "Expected value '{}', but got '{}'", KeyValueType{T{}}.Name(), got.Name());
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

Variant::operator float() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::Float>(variant_.type);
	return variant_.value_float;
}

Variant::operator ConstFloatVectorView() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::FloatVector>(variant_.type);
	return ConstFloatVectorView::FromUint64(variant_.value_uint64);
}

Variant::operator FloatVectorView() noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::FloatVector>(variant_.type);
	assertrx(variant_.hold);
	return FloatVectorView::FromUint64(variant_.value_uint64);
}

Variant::operator Point() const { return static_cast<Point>(getCompositeValues()); }
template <>
Point Variant::As<Point>() const {
	assertrx(!isUuid());
	if (!variant_.type.Is<KeyValueType::Tuple>()) {
		throw Error(errParams, "Can't convert {} to Point", variant_.type.Name());
	}
	return static_cast<Point>(getCompositeValues());
}
template <>
Uuid Variant::As<Uuid>() const {
	if (isUuid()) {
		return Uuid{*this};
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Uuid) { return Uuid{*this}; }, [&](KeyValueType::String) { return Uuid{this->As<std::string>()}; },
		[&](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double, KeyValueType::Float,
							KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Null, KeyValueType::Undefined,
							KeyValueType::FloatVector> auto) -> Uuid {
			throw Error(errParams, "Can't convert {} to UUID", variant_.type.Name());
		});
}
template <>
ConstFloatVectorView Variant::As<ConstFloatVectorView>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert {} to Float Vector", KeyValueType{KeyValueType::Uuid{}}.Name());
	}
	if (variant_.type.Is<KeyValueType::FloatVector>()) {
		return ConstFloatVectorView{*this};
	} else {
		throw Error(errParams, "Can't convert {} to Float Vector", variant_.type.Name());
	}
}

void Variant::free() noexcept {
	assertrx(!isUuid());
	assertrx(variant_.hold == 1);
	variant_.type.EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::String, KeyValueType::Tuple> auto) noexcept { this->cast<key_string>()->~key_string(); },
		[&](KeyValueType::Composite) noexcept { this->cast<PayloadValue>()->~PayloadValue(); },
		[&](KeyValueType::FloatVector) noexcept { delete[] ConstFloatVectorView(*this).Data(); },
		[](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Null, KeyValueType::Undefined,
						   KeyValueType::Double, KeyValueType::Float, KeyValueType::Uuid> auto) noexcept {});
	variant_.hold = 0;
}

void Variant::copy(const Variant& other) {
	assertrx(!isUuid());
	assertrx(!other.isUuid());
	assertrx(variant_.hold == 1);
	assertrx(variant_.type.IsSame(other.Type()));
	variant_.type.EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::String, KeyValueType::Tuple> auto) {
			new (this->cast<void>()) key_string(*other.cast<key_string>());
		},
		[&](KeyValueType::Composite) { new (this->cast<void>()) PayloadValue(*other.cast<PayloadValue>()); },
		[&](KeyValueType::Int) noexcept { variant_.value_int = other.variant_.value_int; },
		[&](KeyValueType::Bool) noexcept { variant_.value_bool = other.variant_.value_bool; },
		[&](concepts::OneOf<KeyValueType::Int64, KeyValueType::Undefined> auto) noexcept {
			variant_.value_uint64 = other.variant_.value_uint64;
		},
		[&](KeyValueType::Double) noexcept { variant_.value_double = other.variant_.value_double; },
		[&](KeyValueType::Float) noexcept { variant_.value_float = other.variant_.value_float; },
		[&](concepts::OneOf<KeyValueType::Null, KeyValueType::Uuid> auto) noexcept {},
		[&](KeyValueType::FloatVector) noexcept {
			FloatVectorView dest{*this};
			ConstFloatVectorView src{other};
			assertrx(dest.Dimension() == src.Dimension());
			memcpy(dest.Data(), src.Data(), sizeof(FloatVectorView::DataType) * src.Dimension().Value());
		});
}

Variant& Variant::EnsureHold() & {
	if (isUuid() || variant_.hold == 1) {
		return *this;
	}
	variant_.type.EvaluateOneOf(
		[&](concepts::OneOf<KeyValueType::String, KeyValueType::Tuple> auto) { *this = Variant(this->operator key_string()); },
		[&](KeyValueType::Composite) { *this = Variant(this->operator const PayloadValue&()); },
		[&](KeyValueType::FloatVector) { *this = Variant(this->operator ConstFloatVectorView(), hold); },
		[](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Null, KeyValueType::Undefined,
						   KeyValueType::Double, KeyValueType::Float, KeyValueType::Uuid> auto) noexcept {});
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
			[&](KeyValueType::Double) { return double_to_str(variant_.value_double); },
			[&](KeyValueType::Float) { return float_to_str(variant_.value_float); },
			[&](KeyValueType::String) { return this->operator p_string().toString(); }, [&](KeyValueType::Null) { return "null"s; },
			[&](KeyValueType::FloatVector) { return float_vector_to_str(ConstFloatVectorView::FromUint64(variant_.value_uint64)); },
			[this](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined> auto) -> std::string {
				throw Error(errParams, "Can't convert '{}'-value to string", variant_.type.Name());
			},
			[&](KeyValueType::Tuple) {
				auto va = getCompositeValues();
				WrSerializer wrser;
				va.Dump(wrser);
				return std::string(wrser.Slice());
			},
			[&](KeyValueType::Uuid) { return std::string{Uuid{*this}}; });
	}
}

template <>
key_string Variant::As<key_string>() const {
	using namespace std::string_literals;
	if (isUuid()) {
		return key_string{Uuid{*this}};
	} else {
		return variant_.type.EvaluateOneOf([&](KeyValueType::Int) { return make_key_string(std::to_string(variant_.value_int)); },
										   [&](KeyValueType::Bool) {
											   static const key_string kTrueKeyString = make_key_string("true");
											   static const key_string kFalseKeyString = make_key_string("false");
											   return variant_.value_bool ? kTrueKeyString : kFalseKeyString;
										   },
										   [&](KeyValueType::Int64) { return make_key_string(std::to_string(variant_.value_int64)); },
										   [&](KeyValueType::Double) { return make_key_string(double_to_str(variant_.value_double)); },
										   [&](KeyValueType::Float) { return make_key_string(float_to_str(variant_.value_float)); },
										   [&](KeyValueType::String) { return this->operator p_string().getKeyString(); },
										   [&](KeyValueType::Null) {
											   static const key_string kNullKeyString = make_key_string("null");
											   return kNullKeyString;
										   },
										   [&](KeyValueType::FloatVector) {
											   WrSerializer wser;
											   float_vector_to_str(ConstFloatVectorView::FromUint64(variant_.value_uint64), wser);
											   return make_key_string(wser.Slice());
										   },
										   [this](concepts::OneOf<KeyValueType::Composite, KeyValueType::Undefined> auto) -> key_string {
											   throw Error(errParams, "Can't convert '{}'-value to string", variant_.type.Name());
										   },
										   [&](KeyValueType::Tuple) {
											   auto va = getCompositeValues();
											   WrSerializer wrser;
											   va.Dump(wrser);
											   return make_key_string(wrser.Slice());
										   },
										   [&](KeyValueType::Uuid) { return key_string{Uuid{*this}}; });
	}
}

template <>
p_string Variant::As<p_string>() const {
	assertrx_throw(!isUuid() && variant_.type.Is<KeyValueType::String>());
	return this->operator p_string();
}

template <>
std::string Variant::As<std::string>(const PayloadType& pt, const FieldsSet& fields) const {
	if (!isUuid() && variant_.type.Is<KeyValueType::Composite>()) {
		ConstPayload pl(pt, operator const PayloadValue&());
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
	if (str.empty()) {
		return 0.0;
	}
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
		throw Error(errParams, "Can't convert '{}' to number", str);
	}
}

template <>
int Variant::As<int>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}});
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> int { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept -> int { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept -> int { return variant_.value_double; },
		[&](KeyValueType::Float) noexcept -> int { return variant_.value_float; },
		[&](KeyValueType::String) { return parseAs<int>(this->operator p_string()); },
		[this](concepts::OneOf<KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null,
							   KeyValueType::FloatVector> auto) -> int {
			throw Error(errParams, "Can't convert '{}'-value to number", Type().Name());
		},
		[&](KeyValueType::Uuid) -> int { throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}}); });
}

static std::optional<bool> tryConvertToBool(const p_string& str) {
	using namespace std::string_view_literals;
	if (iequals(str, "true"sv)) {
		return true;
	} else if (iequals(str, "false"sv)) {
		return false;
	} else {
		const auto v = tryParseAs<int64_t>(str);
		if (v) {
			return v != 0;
		} else {
			const auto v = tryParseAs<double>(str);
			if (v) {
				return v != 0.0;
			} else {
				return std::nullopt;
			}
		}
	}
}

template <>
bool Variant::As<bool>() const {
	using namespace std::string_view_literals;
	if (isUuid()) {
		throw Error(errParams, "Can't convert '{}' to bool", std::string{Uuid{*this}});
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept -> bool { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept -> bool { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept -> bool { return !fp::IsZero(variant_.value_double); },
		[&](KeyValueType::Float) noexcept -> bool { return !fp::IsZero(variant_.value_float); },
		[&](KeyValueType::String) {
			const auto p_str = operator p_string();
			const auto res = tryConvertToBool(p_str);
			if (res.has_value()) {
				return *res;
			} else {
				throw Error(errParams, "Can't convert '{}' to bool", std::string_view(p_str));
			}
		},
		[this](concepts::OneOf<KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null,
							   KeyValueType::FloatVector> auto) -> bool {
			throw Error(errParams, "Can't convert '{}'-value to bool", Type().Name());
		},
		[&](KeyValueType::Uuid) -> bool { throw Error(errParams, "Can't convert '{}' to bool", std::string{Uuid{*this}}); });
}

template <>
int64_t Variant::As<int64_t>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}});
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> int64_t { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept -> int64_t { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept -> int64_t { return variant_.value_double; },
		[&](KeyValueType::Float) noexcept -> int64_t { return variant_.value_float; },
		[&](KeyValueType::String) { return parseAs<int64_t>(this->operator p_string()); },
		[this](concepts::OneOf<KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null,
							   KeyValueType::FloatVector> auto) -> int64_t {
			throw Error(errParams, "Can't convert '{}'-value to number", Type().Name());
		},
		[&](KeyValueType::Uuid) -> int64_t { throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}}); });
}

template <>
double Variant::As<double>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}});
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> double { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept -> double { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept -> double { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept { return variant_.value_double; },
		[&](KeyValueType::Float) noexcept -> double { return variant_.value_float; },
		[&](KeyValueType::String) { return parseAs<double>(this->operator p_string()); },
		[this](concepts::OneOf<KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null,
							   KeyValueType::FloatVector> auto) -> double {
			throw Error(errParams, "Can't convert '{}'-value to number", Type().Name());
		},
		[&](KeyValueType::Uuid) -> double { throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}}); });
}

template <>
float Variant::As<float>() const {
	if (isUuid()) {
		throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}});
	}
	return variant_.type.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> float { return variant_.value_bool; },
		[&](KeyValueType::Int) noexcept -> float { return variant_.value_int; },
		[&](KeyValueType::Int64) noexcept -> float { return variant_.value_int64; },
		[&](KeyValueType::Double) noexcept -> float { return variant_.value_double; },
		[&](KeyValueType::Float) noexcept { return variant_.value_float; },
		[&](KeyValueType::String) -> float { return parseAs<double>(this->operator p_string()); },
		[this](concepts::OneOf<KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null,
							   KeyValueType::FloatVector> auto) -> float {
			throw Error(errParams, "Can't convert '{}'-value to number", Type().Name());
		},
		[&](KeyValueType::Uuid) -> float { throw Error(errParams, "Can't convert '{}' to number", std::string{Uuid{*this}}); });
}

template <NotComparable notComparable>
ComparationResult Variant::compareImpl(const Variant& other, const CollateOpts& collateOpts) const {
	if (!Type().IsSame(other.Type())) {
		if constexpr (notComparable == NotComparable::Return) {
			return ComparationResult::NotComparable;
		} else {
			throw Error(errParams, "Unable to compare '{}' and '{}'", Type().Name(), other.Type().Name());
		}
	}

	if (isUuid()) {
		return Uuid{*this}.Compare(Uuid{other});
	} else {
		return variant_.type.EvaluateOneOf(
			[&](KeyValueType::Int) noexcept {
				return (variant_.value_int == other.variant_.value_int)	 ? ComparationResult::Eq
					   : (variant_.value_int > other.variant_.value_int) ? ComparationResult::Gt
																		 : ComparationResult::Lt;
			},
			[&](KeyValueType::Bool) noexcept {
				return (variant_.value_bool == other.variant_.value_bool)  ? ComparationResult::Eq
					   : (variant_.value_bool > other.variant_.value_bool) ? ComparationResult::Gt
																		   : ComparationResult::Lt;
			},
			[&](KeyValueType::Int64) noexcept {
				return (variant_.value_int64 == other.variant_.value_int64)	 ? ComparationResult::Eq
					   : (variant_.value_int64 > other.variant_.value_int64) ? ComparationResult::Gt
																			 : ComparationResult::Lt;
			},
			[&](KeyValueType::Double) noexcept {
				// Exact floating comparison for hash-tables compatibility
				return fp::ExactlyEqual(variant_.value_double, other.variant_.value_double) ? ComparationResult::Eq
					   : (variant_.value_double > other.variant_.value_double)				? ComparationResult::Gt
																							: ComparationResult::Lt;
			},
			[&](KeyValueType::Float) noexcept {
				// Exact floating comparison for hash-tables compatibility
				return fp::ExactlyEqual(variant_.value_float, other.variant_.value_float) ? ComparationResult::Eq
					   : (variant_.value_float > other.variant_.value_float)			  ? ComparationResult::Gt
																						  : ComparationResult::Lt;
			},
			[&](KeyValueType::Tuple) -> ComparationResult {
				auto lhs = cast<key_string>();
				auto rhs = other.cast<key_string>();
				if (std::string_view(*lhs) == std::string_view(*rhs)) {
					return ComparationResult::Eq;
				}
				// Less/Greater comparison is not implemented for Tuple values here
				if constexpr (notComparable == NotComparable::Return) {
					return ComparationResult::NotComparable;
				} else {
					throw Error(errParams, "KeyValueType::Tuple comparison is not implemented");
				}
			},
			[&](KeyValueType::String) { return collateCompare(this->operator p_string(), other.operator p_string(), collateOpts); },
			[&](KeyValueType::Uuid) { return Uuid{*this}.Compare(Uuid{other}); },
			[](KeyValueType::Null) -> ComparationResult {
				if constexpr (notComparable == NotComparable::Return) {
					return ComparationResult::NotComparable;
				} else {
					throw Error{errParams, "Cannot compare empty values"};
				}
			},
			[](KeyValueType::Composite) -> ComparationResult {
				throw Error{errParams, "Cannot compare composite variants without payload type"};
			},
			[&](KeyValueType::FloatVector) -> ComparationResult {
				const auto lhs = ConstFloatVectorView{*this}.Span();
				const auto rhs = ConstFloatVectorView{other}.Span();
				const auto leftSize = lhs.size();
				const auto rightSize = rhs.size();
				const size_t s = std::min(leftSize, rightSize);
				for (size_t i = 0; i < s; ++i) {
					if (lhs[i] < rhs[i]) {
						return ComparationResult::Lt;
					} else if (lhs[i] > rhs[i]) {
						return ComparationResult::Gt;
					}
				}
				if (leftSize == rightSize) {
					return ComparationResult::Eq;
				} else {
					return leftSize < rightSize ? ComparationResult::Lt : ComparationResult::Gt;
				}
			},
			[](KeyValueType::Undefined) -> ComparationResult {
				if constexpr (notComparable == NotComparable::Return) {
					return ComparationResult::NotComparable;
				} else {
					throw Error(errParams, "Unexpected 'undefined' variant");
				}
			});
	}
}
template ComparationResult Variant::compareImpl<NotComparable::Return>(const Variant&, const CollateOpts&) const;
template ComparationResult Variant::compareImpl<NotComparable::Throw>(const Variant&, const CollateOpts&) const;

template <NotComparable notComparable>
ComparationResult Variant::relaxCompareWithString(std::string_view str) const noexcept(notComparable == NotComparable::Return) {
	thread_local char uuidStrBuf[Uuid::kStrFormLen];
	thread_local const std::string_view uuidStrBufView{uuidStrBuf, Uuid::kStrFormLen};
	if (isUuid()) {
		Uuid{*this}.PutToStr(uuidStrBuf);
		return uuidStrBufView == str ? ComparationResult::Eq : (uuidStrBufView < str ? ComparationResult::Lt : ComparationResult::Gt);
	} else {
		return variant_.type.EvaluateOneOf(
			[&](KeyValueType::Int) {
				const auto value = tryParseAs<int64_t>(str);
				if (value) {
					return compare(variant_.value_int, *value);
				} else {
					if constexpr (notComparable == NotComparable::Return) {
						const auto v = tryParseAs<double>(str);
						if (v) {
							return compare(variant_.value_int, *v);
						} else {
							return ComparationResult::NotComparable;
						}
					} else {
						const double v = parseAs<double>(str);
						return compare(variant_.value_int, v);
					}
				}
			},
			[&](KeyValueType::Int64) {
				const auto value = tryParseAs<int64_t>(str);
				if (value) {
					return compare(variant_.value_int64, *value);
				} else {
					if constexpr (notComparable == NotComparable::Return) {
						const auto v = tryParseAs<double>(str);
						if (v) {
							return compare(variant_.value_int64, *v);
						} else {
							return ComparationResult::NotComparable;
						}
					} else {
						const double v = parseAs<double>(str);
						return compare(variant_.value_int64, v);
					}
				}
			},
			[&](KeyValueType::Double) {
				if constexpr (notComparable == NotComparable::Return) {
					const auto v = tryParseAs<double>(str);
					if (v) {
						return compare(variant_.value_double, *v);
					} else {
						return ComparationResult::NotComparable;
					}
				} else {
					const double v = parseAs<double>(str);
					return compare(variant_.value_double, v);
				}
			},
			[&](KeyValueType::Float) {
				if constexpr (notComparable == NotComparable::Return) {
					const std::optional<float> v = tryParseAs<double>(str);
					if (v) {
						return compare(variant_.value_float, *v);
					} else {
						return ComparationResult::NotComparable;
					}
				} else {
					const float v = parseAs<double>(str);
					return compare(variant_.value_float, v);
				}
			},
			[&](KeyValueType::Uuid) {
				Uuid{*this}.PutToStr(uuidStrBuf);
				return uuidStrBufView == str ? ComparationResult::Eq
											 : (uuidStrBufView < str ? ComparationResult::Lt : ComparationResult::Gt);
			},
			[&](KeyValueType::Bool) {
				using namespace std::string_view_literals;
				if (iequals(str, "true"sv)) {
					return compare(int(variant_.value_bool), 1);
				} else if (iequals(str, "false"sv)) {
					return compare(int(variant_.value_bool), 0);
				} else {
					const auto value = tryParseAs<int64_t>(str);
					if (value) {
						return compare(int64_t(variant_.value_bool), *value);
					} else {
						if constexpr (notComparable == NotComparable::Return) {
							const auto v = tryParseAs<double>(str);
							if (v) {
								return compare(int(variant_.value_bool), *v);
							} else {
								return ComparationResult::NotComparable;
							}
						} else {
							const double v = parseAs<double>(str);
							return compare(int(variant_.value_bool), v);
						}
					}
				}
			},
			[&](concepts::OneOf<KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined,
								KeyValueType::Null, KeyValueType::FloatVector> auto) -> ComparationResult {
				if constexpr (notComparable == NotComparable::Return) {
					return ComparationResult::NotComparable;
				} else {
					throw Error(errParams, "Not comparable types: {} and {}", KeyValueType{KeyValueType::String{}}.Name(), Type().Name());
				}
			});
	}
}

class [[nodiscard]] Comparator {
public:
	explicit Comparator(const Variant& v1, const Variant& v2) noexcept : v1_{v1}, v2_{v2} {}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Bool, KeyValueType::Bool) const noexcept {
		return compare(v1_.As<bool>(), v2_.As<bool>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Bool, KeyValueType::Int) const noexcept {
		return compare(int(v1_.As<bool>()), v2_.As<int>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Bool, KeyValueType::Int64) const noexcept {
		return compare(int64_t(v1_.As<bool>()), v2_.As<int64_t>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Bool, KeyValueType::Double) const noexcept {
		return compare(double(v1_.As<bool>()), v2_.As<double>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Bool, KeyValueType::Float) const noexcept {
		return compare(double(v1_.As<bool>()), v2_.As<float>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int, KeyValueType::Bool) const noexcept {
		return compare(v1_.As<int>(), int(v2_.As<bool>()));
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int, KeyValueType::Int) const noexcept {
		return compare(v1_.As<int>(), v2_.As<int>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int, KeyValueType::Int64) const noexcept {
		return compare(int64_t(v1_.As<int>()), v2_.As<int64_t>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int, KeyValueType::Double) const noexcept {
		return compare(v1_.As<int>(), v2_.As<double>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int, KeyValueType::Float) const noexcept {
		return compare(v1_.As<int>(), v2_.As<float>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int64, KeyValueType::Bool) const noexcept {
		return compare(v1_.As<int64_t>(), int64_t(v2_.As<bool>()));
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int64, KeyValueType::Int) const noexcept {
		return compare(v1_.As<int64_t>(), int64_t(v2_.As<int>()));
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int64, KeyValueType::Int64) const noexcept {
		return compare(v1_.As<int64_t>(), v2_.As<int64_t>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int64, KeyValueType::Double) const noexcept {
		return compare(v1_.As<int64_t>(), v2_.As<double>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Int64, KeyValueType::Float) const noexcept {
		return compare(v1_.As<int64_t>(), v2_.As<float>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Double, KeyValueType::Bool) const noexcept {
		return compare(v1_.As<double>(), double(v2_.As<bool>()));
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Double, KeyValueType::Int) const noexcept {
		return compare(v1_.As<double>(), v2_.As<int>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Double, KeyValueType::Int64) const noexcept {
		return compare(v1_.As<double>(), v2_.As<int64_t>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Double, KeyValueType::Double) const noexcept {
		return compare(v1_.As<double>(), v2_.As<double>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Double, KeyValueType::Float) const noexcept {
		return compare(v1_.As<double>(), v2_.As<float>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Float, KeyValueType::Bool) const noexcept {
		return compare(v1_.As<float>(), double(v2_.As<bool>()));
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Float, KeyValueType::Int) const noexcept {
		return compare(v1_.As<float>(), v2_.As<int>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Float, KeyValueType::Int64) const noexcept {
		return compare(v1_.As<float>(), v2_.As<int64_t>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Float, KeyValueType::Float) const noexcept {
		return compare(v1_.As<float>(), v2_.As<float>());
	}
	RX_ALWAYS_INLINE ComparationResult operator()(KeyValueType::Float, KeyValueType::Double) const noexcept {
		return compare(v1_.As<float>(), v2_.As<double>());
	}

	RX_ALWAYS_INLINE ComparationResult
	operator()(concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
							   KeyValueType::Tuple, KeyValueType::Uuid, KeyValueType::FloatVector> auto,
			   concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
							   KeyValueType::Tuple, KeyValueType::Uuid, KeyValueType::FloatVector> auto) const noexcept {
		assertrx(0);
		abort();
	}
	RX_ALWAYS_INLINE ComparationResult
	operator()(concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto,
			   concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
							   KeyValueType::Tuple, KeyValueType::Uuid, KeyValueType::FloatVector> auto) const noexcept {
		assertrx(0);
		abort();
	}
	RX_ALWAYS_INLINE ComparationResult
	operator()(concepts::OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite,
							   KeyValueType::Tuple, KeyValueType::Uuid, KeyValueType::FloatVector> auto,
			   concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float> auto)
		const noexcept {
		assertrx(0);
		abort();
	}

private:
	const Variant& v1_;
	const Variant& v2_;
};

template <WithString withString, NotComparable notComparable>
ComparationResult Variant::relaxCompareImpl(const Variant& other, NullsHandling tupleNullsHandling, const CollateOpts& collateOpts) const {
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
				return -other.compareImpl<notComparable>(Variant{uuidStrBufPString, noHold}, collateOpts);
			}
		} else if constexpr (withString == WithString::Yes) {
			Uuid{*this}.PutToStr(uuidStrBuf);
			return -other.relaxCompareWithString<notComparable>(uuidStrBufView);
		} else {
			if constexpr (notComparable == NotComparable::Return) {
				return ComparationResult::NotComparable;
			} else {
				throw Error(errParams, "Not comparable types: {} and {}", Type().Name(), other.Type().Name());
			}
		}
	} else if (other.isUuid() || other.variant_.type.Is<KeyValueType::Uuid>()) {
		if (variant_.type.Is<KeyValueType::String>()) {
			const auto uuid = Uuid::TryParse(As<p_string>());
			if (uuid) {
				return uuid->Compare(Uuid{other});
			} else {
				Uuid{other}.PutToStr(uuidStrBuf);
				return compareImpl<notComparable>(Variant{uuidStrBufPString, noHold}, collateOpts);
			}
		} else if constexpr (withString == WithString::Yes) {
			Uuid{other}.PutToStr(uuidStrBuf);
			return relaxCompareWithString<notComparable>(uuidStrBufView);
		} else {
			if constexpr (notComparable == NotComparable::Return) {
				return ComparationResult::NotComparable;
			} else {
				throw Error(errParams, "Not comparable types: {} and {}", Type().Name(), other.Type().Name());
			}
		}
	} else {
		if (variant_.type.IsSame(other.variant_.type)) {
			if (variant_.type.Is<KeyValueType::Tuple>()) {
				switch (tupleNullsHandling) {
					case NullsHandling::NotComparable:
						return getCompositeValues().RelaxCompare<withString, notComparable, NullsHandling::NotComparable>(
							other.getCompositeValues(), collateOpts);
					case NullsHandling::AlwaysLess:
					default:
						return getCompositeValues().RelaxCompare<withString, notComparable, NullsHandling::AlwaysLess>(
							other.getCompositeValues(), collateOpts);
				}
			} else {
				return compareImpl<notComparable>(other, collateOpts);
			}
		} else if (variant_.type.IsNumeric() && other.variant_.type.IsNumeric()) {
			return KeyValueType::Visit(Comparator{*this, other}, variant_.type, other.variant_.type);
		} else {
			if constexpr (withString == WithString::Yes) {
				if (other.Type().Is<KeyValueType::String>()) {
					return relaxCompareWithString<notComparable>(other.operator p_string());
				} else if (Type().Is<KeyValueType::String>()) {
					return -other.relaxCompareWithString<notComparable>(this->operator p_string());
				}
			}
			if constexpr (notComparable == NotComparable::Return) {
				return ComparationResult::NotComparable;
			} else {
				throw Error(errParams, "Not comparable types: {} and {}", Type().Name(), other.Type().Name());
			}
		}
	}
}

template ComparationResult Variant::relaxCompareImpl<WithString::Yes, NotComparable::Return>(const Variant&, NullsHandling,
																							 const CollateOpts&) const;
template ComparationResult Variant::relaxCompareImpl<WithString::No, NotComparable::Return>(const Variant&, NullsHandling,
																							const CollateOpts&) const;
template ComparationResult Variant::relaxCompareImpl<WithString::Yes, NotComparable::Throw>(const Variant&, NullsHandling,
																							const CollateOpts&) const;
template ComparationResult Variant::relaxCompareImpl<WithString::No, NotComparable::Throw>(const Variant&, NullsHandling,
																						   const CollateOpts&) const;

size_t Variant::Hash() const noexcept {
	if (isUuid()) {
		return std::hash<Uuid>()(Uuid{*this});
	} else {
		return variant_.type.EvaluateOneOf([&](KeyValueType::Int) noexcept { return std::hash<int>()(variant_.value_int); },
										   [&](KeyValueType::Bool) noexcept { return std::hash<bool>()(variant_.value_bool); },
										   [&](KeyValueType::Int64) noexcept { return std::hash<int64_t>()(variant_.value_int64); },
										   [&](KeyValueType::Double) noexcept { return std::hash<double>()(variant_.value_double); },
										   [&](KeyValueType::Float) noexcept { return std::hash<float>()(variant_.value_float); },
										   [&](KeyValueType::String) noexcept { return std::hash<p_string>()(this->operator p_string()); },
										   [&](KeyValueType::Uuid) noexcept { return std::hash<Uuid>()(Uuid{*this}); },
										   [](KeyValueType::Null) noexcept { return std::hash<int>()(0); },
										   [&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined,
															   KeyValueType::FloatVector> auto) noexcept -> size_t {
#ifdef NDEBUG
											   abort();
#else
											   assertf(false, "Unexpected variant type: {}", variant_.type.Name());
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

Variant Variant::convert(KeyValueType type, const PayloadType* payloadType, const FieldsSet* fields) const& {
	if (Type().IsSame(type)) {
		return *this;
	}
	Variant dst(*this);
	std::ignore = dst.convert(type, payloadType, fields);
	return dst;
}

Variant& Variant::convert(KeyValueType type, const PayloadType* payloadType, const FieldsSet* fields) & {
	if (isUuid()) {
		type.EvaluateOneOf(
			[&](KeyValueType::Uuid) noexcept {}, [&](KeyValueType::String) { *this = Variant{std::string{Uuid{*this}}}; },
			[&](KeyValueType::Composite) {
				assertrx_throw(payloadType && fields);
				Variant tmp{VariantArray{std::move(*this)}};
				tmp.convertToComposite(*payloadType, *fields);
				*this = std::move(tmp);
			},
			[type](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double, KeyValueType::Float,
								   KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::FloatVector> auto) {
				throw Error(errParams, "Can't convert Variant from type '{}' to type '{}'", KeyValueType{KeyValueType::Uuid{}}.Name(),
							type.Name());
			});
		return *this;
	}
	if (type.IsSame(variant_.type) || variant_.type.Is<KeyValueType::Null>()) {
		return *this;
	}
	type.EvaluateOneOf(
		[&](KeyValueType::Int) { *this = Variant(As<int>()); }, [&](KeyValueType::Bool) { *this = Variant(As<bool>()); },
		[&](KeyValueType::Int64) { *this = Variant(As<int64_t>()); }, [&](KeyValueType::Double) { *this = Variant(As<double>()); },
		[&](KeyValueType::Float) { *this = Variant(As<float>()); }, [&](KeyValueType::String) { *this = Variant(As<key_string>()); },
		[&](KeyValueType::Composite) {
			variant_.type.EvaluateOneOf(
				[&](KeyValueType::Tuple) {
					assertrx_throw(payloadType && fields);
					convertToComposite(*payloadType, *fields);
				},
				[](KeyValueType::Composite) noexcept {},
				[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
									KeyValueType::String, KeyValueType::Uuid> auto) {
					assertrx_throw(payloadType && fields);
					Variant tmp{VariantArray{std::move(*this)}};
					tmp.convertToComposite(*payloadType, *fields);
					*this = std::move(tmp);
				},
				[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Null, KeyValueType::FloatVector> auto) {
					throw Error(errParams, "Can't convert Variant from type '{}' to type '{}'", variant_.type.Name(), type.Name());
				});
		},
		[&](KeyValueType::Uuid) { *this = Variant{As<Uuid>()}; },
		[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::FloatVector> auto) {
			throw Error(errParams, "Can't convert Variant from type '{}' to type '{}'", variant_.type.Name(), type.Name());
		});
	return *this;
}

std::optional<Variant> Variant::tryConvert(KeyValueType type, const PayloadType* payloadType, const FieldsSet* fields) const& {
	if (Type().IsSame(type)) {
		return *this;
	} else {
		Variant tmp = *this;
		if (tmp.tryConvert(type, payloadType, fields)) {
			return std::optional<Variant>{std::move(tmp)};
		} else {
			return std::nullopt;
		}
	}
}

bool Variant::tryConvert(KeyValueType type, const PayloadType* payloadType, const FieldsSet* fields) & {
	using namespace std::string_view_literals;
	if (isUuid()) {
		return type.EvaluateOneOf(
			[&](KeyValueType::Uuid) noexcept { return true; },
			[&](KeyValueType::String) {
				*this = Variant{std::string{Uuid{*this}}};
				return true;
			},
			[&](KeyValueType::Composite) {
				assertrx_throw(payloadType && fields);
				try {
					Variant tmp{VariantArray{std::move(*this)}};
					tmp.convertToComposite(*payloadType, *fields);
					*this = std::move(tmp);
					return true;
				} catch (...) {
					return false;
				}
			},
			[](concepts::OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Double, KeyValueType::Float,
							   KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null, KeyValueType::FloatVector> auto) {
				return false;
			});
	}
	if (type.IsSame(variant_.type) || type.Is<KeyValueType::Null>() || variant_.type.Is<KeyValueType::Null>()) {
		return true;
	}
	const bool res = type.EvaluateOneOf(
		[&](KeyValueType::Int) {
			return variant_.type.EvaluateOneOf(
				[&](KeyValueType::Bool) noexcept {
					variant_.value_int = variant_.value_bool;
					return true;
				},
				[](KeyValueType::Int) noexcept { return true; },
				[&](KeyValueType::Int64) noexcept {
					if (variant_.value_int64 < std::numeric_limits<int>::min() || variant_.value_int64 > std::numeric_limits<int>::max()) {
						return false;
					} else {
						variant_.value_int = variant_.value_int64;
						return true;
					}
				},
				[&](KeyValueType::Double) noexcept {
					if (variant_.value_double < std::numeric_limits<int>::min() ||
						variant_.value_double > std::numeric_limits<int>::max()) {
						return false;
					} else {
						variant_.value_int = variant_.value_double;
						return true;
					}
				},
				[&](KeyValueType::Float) noexcept {
					if (variant_.value_float < std::numeric_limits<int>::min() ||
						variant_.value_float > float(std::numeric_limits<int>::max())) {
						return false;
					} else {
						variant_.value_int = variant_.value_float;
						return true;
					}
				},
				[&](KeyValueType::String) noexcept {
					const auto res = tryParseAs<int>(operator p_string());
					if (res) {
						*this = Variant{*res};
						return true;
					} else {
						return false;
					}
				},
				[&](KeyValueType::Null) noexcept {
					variant_.value_int = 0;
					return true;
				},
				[](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
								   KeyValueType::FloatVector> auto) noexcept { return false; });
		},
		[&](KeyValueType::Bool) {
			return variant_.type.EvaluateOneOf(
				[](KeyValueType::Bool) noexcept { return true; },
				[&](KeyValueType::Int) noexcept {
					variant_.value_bool = bool(variant_.value_int);
					return true;
				},
				[&](KeyValueType::Int64) noexcept {
					variant_.value_bool = bool(variant_.value_int64);
					return true;
				},
				[&](KeyValueType::Double) noexcept {
					variant_.value_bool = !fp::IsZero(variant_.value_double);
					return true;
				},
				[&](KeyValueType::Float) noexcept {
					variant_.value_bool = !fp::IsZero(variant_.value_float);
					return true;
				},
				[&](KeyValueType::String) noexcept {
					const auto res = tryConvertToBool(operator p_string());
					if (res.has_value()) {
						*this = Variant{*res};
						return true;
					} else {
						return false;
					}
				},
				[&](KeyValueType::Null) noexcept {
					variant_.value_bool = false;
					return true;
				},
				[](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
								   KeyValueType::FloatVector> auto) noexcept { return false; });
		},
		[&](KeyValueType::Int64) {
			return variant_.type.EvaluateOneOf(
				[&](KeyValueType::Bool) noexcept {
					variant_.value_int64 = variant_.value_bool;
					return true;
				},
				[&](KeyValueType::Int) noexcept {
					variant_.value_int64 = variant_.value_int;
					return true;
				},
				[](KeyValueType::Int64) noexcept { return true; },
				[&](KeyValueType::Double) noexcept {
					variant_.value_int64 = variant_.value_double;
					return true;
				},
				[&](KeyValueType::Float) noexcept {
					variant_.value_int64 = variant_.value_float;
					return true;
				},
				[&](KeyValueType::String) noexcept {
					const auto res = tryParseAs<int64_t>(operator p_string());
					if (res) {
						*this = Variant{*res};
						return true;
					} else {
						return false;
					}
				},
				[&](KeyValueType::Null) noexcept {
					variant_.value_int64 = 0;
					return true;
				},
				[](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
								   KeyValueType::FloatVector> auto) noexcept { return false; });
		},
		[&](KeyValueType::Double) {
			return variant_.type.EvaluateOneOf(
				[&](KeyValueType::Bool) noexcept {
					variant_.value_double = variant_.value_bool;
					return true;
				},
				[&](KeyValueType::Int) noexcept {
					variant_.value_double = variant_.value_int;
					return true;
				},
				[&](KeyValueType::Int64) noexcept {
					variant_.value_double = variant_.value_int64;
					return true;
				},
				[](KeyValueType::Double) noexcept { return true; },
				[&](KeyValueType::Float) noexcept {
					variant_.value_double = variant_.value_float;
					return true;
				},
				[&](KeyValueType::String) noexcept {
					const auto res = tryParseAs<double>(operator p_string());
					if (res) {
						*this = Variant{*res};
						return true;
					} else {
						return false;
					}
				},
				[&](KeyValueType::Null) noexcept {
					variant_.value_double = 0.0;
					return true;
				},
				[](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
								   KeyValueType::FloatVector> auto) noexcept { return false; });
		},
		[&](KeyValueType::Float) {
			return variant_.type.EvaluateOneOf(
				[&](KeyValueType::Bool) noexcept {
					variant_.value_float = variant_.value_bool;
					return true;
				},
				[&](KeyValueType::Int) noexcept {
					variant_.value_float = variant_.value_int;
					return true;
				},
				[&](KeyValueType::Int64) noexcept {
					variant_.value_float = variant_.value_int64;
					return true;
				},
				[&](KeyValueType::Double) noexcept {
					variant_.value_float = variant_.value_double;
					return true;
				},
				[](KeyValueType::Float) noexcept { return true; },
				[&](KeyValueType::String) noexcept {
					const std::optional<float> res = tryParseAs<double>(operator p_string());
					if (res) {
						*this = Variant{*res};
						return true;
					} else {
						return false;
					}
				},
				[&](KeyValueType::Null) noexcept {
					variant_.value_float = 0.0;
					return true;
				},
				[](concepts::OneOf<KeyValueType::Uuid, KeyValueType::Composite, KeyValueType::Tuple, KeyValueType::Undefined,
								   KeyValueType::FloatVector> auto) noexcept { return false; });
		},
		[&](KeyValueType::String) {
			*this = Variant{As<std::string>()};
			return true;
		},
		[&](KeyValueType::Composite) {
			return variant_.type.EvaluateOneOf(
				[&](KeyValueType::Tuple) {
					assertrx_throw(payloadType && fields);
					try {
						convertToComposite(*payloadType, *fields);
						return true;
					} catch (...) {
						return false;
					}
				},
				[](KeyValueType::Composite) noexcept { return true; },
				[&](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
									KeyValueType::String, KeyValueType::Uuid> auto) {
					assertrx_throw(payloadType && fields);
					try {
						Variant tmp{VariantArray{std::move(*this)}};
						tmp.convertToComposite(*payloadType, *fields);
						*this = std::move(tmp);
						return true;
					} catch (...) {
						return false;
					}
				},
				[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Null, KeyValueType::FloatVector> auto) { return false; });
		},
		[&](KeyValueType::Uuid) {
			return variant_.type.EvaluateOneOf(
				[](KeyValueType::Uuid) noexcept { return true; },
				[&](KeyValueType::String) noexcept {
					const auto res = Uuid::TryParse(operator p_string());
					if (res) {
						*this = Variant{*res};
						return true;
					} else {
						return false;
					}
				},
				[](concepts::OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float,
								   KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null,
								   KeyValueType::FloatVector> auto) noexcept { return false; });
		},
		[&](KeyValueType::FloatVector) {
			return variant_.type.EvaluateOneOf(
				[](KeyValueType::FloatVector) noexcept { return true; },
				[](concepts::OneOf<KeyValueType::Null, KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double,
								   KeyValueType::Float, KeyValueType::String, KeyValueType::Uuid, KeyValueType::Tuple,
								   KeyValueType::Composite, KeyValueType::Undefined> auto) noexcept { return false; });
		},
		[&](concepts::OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null> auto) { return false; });
	if (res && !isUuid()) {
		variant_.type = type;
	}
	return res;
}

void Variant::convertToComposite(const PayloadType& payloadType, const FieldsSet& fields) {
	assertrx(!isUuid());
	assertrx(variant_.type.Is<KeyValueType::Tuple>() && variant_.hold == 1);
	key_string val = *cast<key_string>();

	if (variant_.hold == 1) {
		free();
	}
	// Alloc usual payloadvalue + extra memory for hold string

	auto strSz = val.size();
	auto& pv = *new (cast<void>()) PayloadValue(payloadType.TotalSize() + strSz);
	variant_.hold = 1;
	variant_.type = KeyValueType::Composite{};

	// Copy serializer buffer with strings to extra payloadvalue memory
	char* data = reinterpret_cast<char*>(pv.Ptr() + payloadType.TotalSize());
	memcpy(data, val.data(), strSz);

	Serializer ser(std::string_view(data, strSz));

	size_t count = ser.GetVarUInt();
	if (count != fields.size()) {
		throw Error(errLogic, "Invalid count of arguments for composite index, expected {}, got {}", fields.size(), count);
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
	Serializer ser(*cast<key_string>());
	size_t count = ser.GetVarUInt();
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
	}
	return cast<p_string>()->getKeyString();
}

Variant::operator p_string() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::String>(variant_.type);
	return (variant_.hold == 1) ? p_string(*cast<key_string>()) : *cast<p_string>();
}

Variant::operator std::string_view() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::String>(variant_.type);
	return (variant_.hold == 1) ? std::string_view(*cast<key_string>()) : *cast<p_string>();
}
Variant::operator const PayloadValue&() const noexcept {
	assertrx(!isUuid());
	assertKeyType<KeyValueType::Composite>(variant_.type);
	assertrx(variant_.hold == 1);
	return *cast<PayloadValue>();
}

template <typename T>
void Variant::Dump(T& os, const PayloadType& pt, const FieldsSet& fieldsSet, CheckIsStringPrintable checkPrintableString) const {
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
			[&](KeyValueType::Float) { os << this->operator float(); }, [&](KeyValueType::Tuple) { getCompositeValues().Dump(os); },
			[&](KeyValueType::Uuid) { os << Uuid{*this}; }, [&](KeyValueType::FloatVector) { os << "[??]"; },
			[&](KeyValueType::Composite) { os << (pt ? As<std::string>(pt, fieldsSet) : "??"); },
			[&](concepts::OneOf<KeyValueType::Undefined, KeyValueType::Null> auto) { os << "??"; });
	}
}

template <typename T>
void Variant::Dump(T& os, CheckIsStringPrintable checkPrintableString) const {
	Dump(os, PayloadType{}, FieldsSet{}, checkPrintableString);
}

template void Variant::Dump(WrSerializer&, CheckIsStringPrintable) const;
template void Variant::Dump(std::ostream&, CheckIsStringPrintable) const;
template void Variant::Dump(std::stringstream&, CheckIsStringPrintable) const;

template <typename T>
void VariantArray::Dump(T& os, const PayloadType& pt, const FieldsSet& fieldsSet, CheckIsStringPrintable checkPrintableString) const {
	os << '{';
	for (auto& arg : *this) {
		if (&arg != &at(0)) {
			os << ", ";
		}
		arg.Dump(os, pt, fieldsSet, checkPrintableString);
	}
	os << '}';
}

template <typename T>
void VariantArray::Dump(T& os, CheckIsStringPrintable checkPrintableString) const {
	Dump(os, PayloadType{}, FieldsSet{}, checkPrintableString);
}

template void VariantArray::Dump(WrSerializer&, CheckIsStringPrintable) const;
template void VariantArray::Dump(std::ostream&, CheckIsStringPrintable) const;
template void VariantArray::Dump(std::stringstream&, CheckIsStringPrintable) const;

template <typename T>
static std::string dumpImpl(T&& obj, const PayloadType& pt, const FieldsSet& fieldsSet, CheckIsStringPrintable checkPrintableString) {
	std::stringstream ss;
	obj.Dump(ss, pt, fieldsSet, checkPrintableString);
	return ss.str();
}

std::string Variant::Dump(CheckIsStringPrintable checkPrintableString) const {
	return dumpImpl(*this, PayloadType{}, FieldsSet{}, checkPrintableString);
}
std::string Variant::Dump(const PayloadType& pt, const FieldsSet& fieldsSet, CheckIsStringPrintable checkPrintableString) const {
	return dumpImpl(*this, pt, fieldsSet, checkPrintableString);
}
std::string VariantArray::Dump(CheckIsStringPrintable checkPrintableString) const {
	return dumpImpl(*this, PayloadType{}, FieldsSet{}, checkPrintableString);
}
std::string VariantArray::Dump(const PayloadType& pt, const FieldsSet& fieldsSet, CheckIsStringPrintable checkPrintableString) const {
	return dumpImpl(*this, pt, fieldsSet, checkPrintableString);
}

VariantArray::operator Point() const {
	if (size() != 2) {
		throw Error(errParams, "Can't convert array of {} elements to Point", size());
	}
	return Point{(*this)[0].As<double>(), (*this)[1].As<double>()};
}

template <NotComparable notComparable, NullsHandling nullsHandling>
ComparationResult VariantArray::Compare(const VariantArray& other, const CollateOpts& collateOpts) const {
	if (IsArrayValue() != other.IsArrayValue()) {
		if constexpr (notComparable == NotComparable::Return) {
			return ComparationResult::NotComparable;
		} else {
			throw Error(errParams, "Can't compare array and non-array");
		}
	}
	if (IsObjectValue() != other.IsObjectValue()) {
		if constexpr (notComparable == NotComparable::Return) {
			return ComparationResult::NotComparable;
		} else {
			throw Error(errParams, "Can't compare object and non-object");
		}
	}

	auto lhsIt{cbegin()}, rhsIt{other.cbegin()};
	const auto lhsEnd{cend()}, rhsEnd{other.cend()};
	for (; lhsIt != lhsEnd && rhsIt != rhsEnd; ++lhsIt, ++rhsIt) {
		if (auto res = variant_compare_helpers::handleNulls<nullsHandling>(*lhsIt, *rhsIt); res) {
			return *res;
		}
		const auto res = lhsIt->Compare<notComparable, nullsHandling>(*rhsIt, collateOpts);
		if (res != ComparationResult::Eq) {
			return res;
		}
	}
	if (lhsIt == lhsEnd) {
		return (rhsIt == rhsEnd) ? ComparationResult::Eq : ComparationResult::Lt;
	}
	return ComparationResult::Gt;
}

template ComparationResult VariantArray::Compare<NotComparable::Return, NullsHandling::NotComparable>(const VariantArray&,
																									  const CollateOpts&) const;
template ComparationResult VariantArray::Compare<NotComparable::Return, NullsHandling::AlwaysLess>(const VariantArray&,
																								   const CollateOpts&) const;
template ComparationResult VariantArray::Compare<NotComparable::Throw, NullsHandling::NotComparable>(const VariantArray&,
																									 const CollateOpts&) const;
template ComparationResult VariantArray::Compare<NotComparable::Throw, NullsHandling::AlwaysLess>(const VariantArray&,
																								  const CollateOpts&) const;

}  // namespace reindexer
