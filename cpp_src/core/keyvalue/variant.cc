#include "variant.h"
#include <charconv>
#include <functional>

#include "core/payload/payloadiface.h"
#include "estl/tuple_utils.h"
#include "geometry.h"
#include "key_string.h"
#include "p_string.h"
#include "tools/compare.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"
#include "vendor/double-conversion/double-conversion.h"

namespace reindexer {

Variant::Variant(const PayloadValue &v) : type_(KeyValueType::Composite{}), hold_(true) { new (cast<void>()) PayloadValue(v); }

Variant::Variant(PayloadValue &&v) : type_(KeyValueType::Composite{}), hold_(true) { new (cast<void>()) PayloadValue(std::move(v)); }

Variant::Variant(const std::string &v) : type_(KeyValueType::String{}), hold_(true) { new (cast<void>()) key_string(make_key_string(v)); }

Variant::Variant(const key_string &v) : type_(KeyValueType::String{}), hold_(true) { new (cast<void>()) key_string(v); }
Variant::Variant(const char *v) : Variant(p_string(v)) {}
Variant::Variant(p_string v, bool enableHold) : type_(KeyValueType::String{}) {
	if (v.type() == p_string::tagKeyString && enableHold) {
		hold_ = true;
		new (cast<void>()) key_string(v.getKeyString());
	} else {
		*cast<p_string>() = v;
	}
}

Variant::Variant(const VariantArray &values) : type_{KeyValueType::Tuple{}} {
	WrSerializer ser;
	ser.PutVarUint(values.size());
	for (const Variant &kv : values) {
		ser.PutVariant(kv);
	}
	new (cast<void>()) key_string(make_key_string(ser.Slice()));
	hold_ = true;
}

Variant::Variant(Point p) : Variant{VariantArray{p}} {}

void serialize(WrSerializer &, const std::tuple<> &) noexcept {}

template <typename... Ts>
void serialize(WrSerializer &ser, const std::tuple<Ts...> &v) {
	ser.PutVariant(Variant{std::get<0>(v)});
	serialize(ser, tail(v));
}

template <typename... Ts>
Variant::Variant(const std::tuple<Ts...> &values) : type_{KeyValueType::Tuple{}} {
	WrSerializer ser;
	ser.PutVarUint(sizeof...(Ts));
	serialize(ser, values);
	new (cast<void>()) key_string(make_key_string(ser.Slice()));
	hold_ = true;
}
template Variant::Variant(const std::tuple<int, std::string> &);
template Variant::Variant(const std::tuple<std::string, int> &);

template <typename T>
inline static void assertKeyType([[maybe_unused]] KeyValueType got) noexcept {
	assertf(got.Is<T>(), "Expected value '%s', but got '%s'", KeyValueType{T{}}.Name(), got.Name());
}

Variant::operator int() const noexcept {
	assertKeyType<KeyValueType::Int>(type_);
	return value_int;
}

Variant::operator bool() const noexcept {
	assertKeyType<KeyValueType::Bool>(type_);
	return value_bool;
}

Variant::operator int64_t() const noexcept {
	assertKeyType<KeyValueType::Int64>(type_);
	return value_int64;
}

Variant::operator double() const noexcept {
	assertKeyType<KeyValueType::Double>(type_);
	return value_double;
}

Variant::operator Point() const { return static_cast<Point>(getCompositeValues()); }
template <>
Point Variant::As<Point>() const {
	if (!type_.Is<KeyValueType::Tuple>()) throw Error(errParams, "Can't convert %s to Point", type_.Name());
	return static_cast<Point>(getCompositeValues());
}

void Variant::free() noexcept {
	assertrx(hold_);
	type_.EvaluateOneOf([&](OneOf<KeyValueType::String, KeyValueType::Tuple>) noexcept { this->cast<key_string>()->~key_string(); },
						[&](KeyValueType::Composite) noexcept { this->cast<PayloadValue>()->~PayloadValue(); },
						[](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Null, KeyValueType::Undefined,
								 KeyValueType::Double>) noexcept {});
	hold_ = false;
}

void Variant::copy(const Variant &other) {
	assertrx(hold_);
	type_.EvaluateOneOf(
		[&](OneOf<KeyValueType::String, KeyValueType::Tuple>) { new (this->cast<void>()) key_string(*other.cast<key_string>()); },
		[&](KeyValueType::Composite) { new (this->cast<void>()) PayloadValue(*other.cast<PayloadValue>()); },
		[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Null, KeyValueType::Undefined,
				  KeyValueType::Double>) noexcept { value_uint64 = other.value_uint64; });
}

Variant &Variant::EnsureHold() {
	if (hold_) return *this;
	type_.EvaluateOneOf([&](OneOf<KeyValueType::String, KeyValueType::Tuple>) { *this = Variant(this->operator key_string()); },
						[&](KeyValueType::Composite) { *this = Variant(this->operator const PayloadValue &()); },
						[](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Bool, KeyValueType::Null, KeyValueType::Undefined,
								 KeyValueType::Double>) noexcept {});
	return *this;
}

template <>
std::string Variant::As<std::string>() const {
	using namespace std::string_literals;
	return type_.EvaluateOneOf(
		[&](KeyValueType::Int) { return std::to_string(value_int); }, [&](KeyValueType::Bool) { return value_bool ? "true"s : "false"s; },
		[&](KeyValueType::Int64) { return std::to_string(value_int64); },
		[&](KeyValueType::Double) { return std::to_string(value_double); },
		[&](KeyValueType::String) {
			if (this->operator p_string().type() == p_string::tagCxxstr || this->operator p_string().type() == p_string::tagKeyString) {
				return *(this->operator p_string().getCxxstr());
			}
			return this->operator p_string().toString();
		},
		[&](KeyValueType::Null) { return "null"s; }, [&](KeyValueType::Composite) { return std::string(); },
		[&](KeyValueType::Tuple) {
			auto va = getCompositeValues();
			WrSerializer wrser;
			va.Dump(wrser);
			return std::string(wrser.Slice());
		},
		[](KeyValueType::Undefined) -> std::string { abort(); });
}

template <>
std::string Variant::As<std::string>(const PayloadType &pt, const FieldsSet &fields) const {
	if (type_.Is<KeyValueType::Composite>()) {
		ConstPayload pl(pt, operator const PayloadValue &());
		VariantArray va;
		size_t tagsPathIdx = 0;
		for (auto field : fields) {
			bool fieldFromCjson = (field == IndexValueType::SetByJsonPath);
			VariantArray va1;
			if (fieldFromCjson) {
				assertrx(tagsPathIdx < fields.getTagsPathsLength());
				pl.GetByJsonPath(fields.getTagsPath(tagsPathIdx++), va1, type_);
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
T parseAs(std::string_view str) {
	const auto end = str.data() + str.size();
	T res;
	auto [ptr, err] = std::from_chars(str.data(), end, res);
	if (ptr == str.data() || err == std::errc::invalid_argument || err == std::errc::result_out_of_range) {
		throw Error(errParams, "Can't convert '%s' to number", str);
	}
	for (; ptr != end; ++ptr) {
		if (!std::isspace(*ptr)) {
			throw Error(errParams, "Can't convert '%s' to number", str);
		}
	}
	return res;
}

template <>
double parseAs<double>(std::string_view str) {
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
		throw Error{errParams, "Can't convert '%s' to number", str};
	}
	if (std::isnan(res)) {
		throw Error{errParams, "Can't convert '%s' to number", str};
	}
	return res;
}

template <>
int Variant::As<int>() const {
	return type_.EvaluateOneOf([&](KeyValueType::Bool) noexcept -> int { return value_bool; },
							   [&](KeyValueType::Int) noexcept { return value_int; },
							   [&](KeyValueType::Int64) noexcept -> int { return value_int64; },
							   [&](KeyValueType::Double) noexcept -> int { return value_double; },
							   [&](KeyValueType::String) -> int { return parseAs<int>(this->operator p_string()); },
							   [](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return 0; },
							   [](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> int { abort(); });
}

template <>
bool Variant::As<bool>() const {
	using namespace std::string_view_literals;
	return type_.EvaluateOneOf([&](KeyValueType::Bool) noexcept { return value_bool; },
							   [&](KeyValueType::Int) noexcept -> bool { return value_int; },
							   [&](KeyValueType::Int64) noexcept -> bool { return value_int64; },
							   [&](KeyValueType::Double) noexcept -> bool { return value_double; },
							   [&](KeyValueType::String) noexcept { return std::string_view(this->operator p_string()) == "true"sv; },
							   [](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return false; },
							   [](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> bool { abort(); });
}

template <>
int64_t Variant::As<int64_t>() const {
	return type_.EvaluateOneOf([&](KeyValueType::Bool) noexcept -> int64_t { return value_bool; },
							   [&](KeyValueType::Int) noexcept -> int64_t { return value_int; },
							   [&](KeyValueType::Int64) noexcept { return value_int64; },
							   [&](KeyValueType::Double) noexcept -> int64_t { return value_double; },
							   [&](KeyValueType::String) { return parseAs<int64_t>(this->operator p_string()); },
							   [](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept -> int64_t { return 0; },
							   [](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> int64_t { abort(); });
}

template <>
double Variant::As<double>() const {
	return type_.EvaluateOneOf(
		[&](KeyValueType::Bool) noexcept -> double { return value_bool; }, [&](KeyValueType::Int) noexcept -> double { return value_int; },
		[&](KeyValueType::Int64) noexcept -> double { return value_int64; }, [&](KeyValueType::Double) noexcept { return value_double; },
		[&](KeyValueType::String) { return parseAs<double>(this->operator p_string()); },
		[](OneOf<KeyValueType::Composite, KeyValueType::Tuple>) noexcept { return 0.0; },
		[](OneOf<KeyValueType::Undefined, KeyValueType::Null>) noexcept -> double { abort(); });
}

int Variant::Compare(const Variant &other, const CollateOpts &collateOpts) const {
	assertrx(other.Type().IsSame(type_));
	return Type().EvaluateOneOf(
		[&](KeyValueType::Int) noexcept { return (value_int == other.value_int)	 ? 0
												 : (value_int > other.value_int) ? 1
																				 : -1; },
		[&](KeyValueType::Bool) noexcept { return (value_bool == other.value_bool)	? 0
												  : (value_bool > other.value_bool) ? 1
																					: -1; },
		[&](KeyValueType::Int64) noexcept { return (value_int64 == other.value_int64)  ? 0
												   : (value_int64 > other.value_int64) ? 1
																					   : -1; },
		[&](KeyValueType::Double) noexcept {
			return (value_double == other.value_double) ? 0 : (value_double > other.value_double) ? 1 : -1;
		},
		[&](KeyValueType::Tuple) { return getCompositeValues() == other.getCompositeValues() ? 0 : 1; },
		[&](KeyValueType::String) { return collateCompare(this->operator p_string(), other.operator p_string(), collateOpts); },
		[](KeyValueType::Null) -> int {
			throw Error{errParams, "Cannot compare empty values"};
		},
		[](OneOf<KeyValueType::Composite, KeyValueType::Undefined>) noexcept -> int { abort(); });
}

int Variant::relaxCompareWithString(std::string_view str) const {
	return Type().EvaluateOneOf(
		[&](KeyValueType::Int) {
			try {
				const int64_t value = parseAs<int64_t>(str);
				return compare(value_int, value);
			} catch (...) {
				const double value = parseAs<double>(str);
				return compare(value_int, value);
			}
		},
		[&](KeyValueType::Int64) {
			try {
				const int64_t value = parseAs<int64_t>(str);
				return compare(value_int64, value);
			} catch (...) {
				const double value = parseAs<double>(str);
				return compare(value_int64, value);
			}
		},
		[&](KeyValueType::Double) {
			const double value = parseAs<double>(str);
			return compare(value_double, value);
		},
		[&](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined,
				  KeyValueType::Null>) -> int {
			throw Error(errParams, "Not comparable types: %s and %s", KeyValueType{KeyValueType::String{}}.Name(), Type().Name());
		});
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
	int operator()(OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple>,
				   OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple>)
		const noexcept {
		assertrx(0);
		abort();
	}
	int operator()(OneOf<KeyValueType::Bool, KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>,
				   OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple>)
		const noexcept {
		assertrx(0);
		abort();
	}
	int operator()(OneOf<KeyValueType::String, KeyValueType::Null, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Tuple>,
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
	if (Type().IsSame(other.Type())) {
		if (Type().Is<KeyValueType::Tuple>()) {
			return getCompositeValues().RelaxCompare<withString>(other.getCompositeValues(), collateOpts);
		} else {
			return Compare(other, collateOpts);
		}
	}
	if (Type().IsNumeric() && other.Type().IsNumeric()) {
		return KeyValueType::Visit(Comparator{*this, other}, Type(), other.Type());
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

template int Variant::RelaxCompare<WithString::Yes>(const Variant &, const CollateOpts &) const;
template int Variant::RelaxCompare<WithString::No>(const Variant &, const CollateOpts &) const;

size_t Variant::Hash() const noexcept {
	return Type().EvaluateOneOf(
		[&](KeyValueType::Int) noexcept { return std::hash<int>()(value_int); },
		[&](KeyValueType::Bool) noexcept { return std::hash<bool>()(value_bool); },
		[&](KeyValueType::Int64) noexcept { return std::hash<int64_t>()(value_int64); },
		[&](KeyValueType::Double) noexcept { return std::hash<double>()(value_double); },
		[&](KeyValueType::String) noexcept { return std::hash<p_string>()(this->operator p_string()); },
		[&](OneOf<KeyValueType::Tuple, KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null>) noexcept -> size_t {
#ifdef NDEBUG
			abort();
#else
			assertf(false, "Unexpected variant type: %s", Type().Name());
#endif
		});
}

void Variant::EnsureUTF8() const {
	if (type_.Is<KeyValueType::String>()) {
		if (!utf8::is_valid(operator p_string().data(), operator p_string().data() + operator p_string().size())) {
			throw Error(errParams, "Invalid UTF8 string passed to index with CollateUTF8 mode");
		}
	}
}

Variant Variant::convert(KeyValueType type, const PayloadType *payloadType, const FieldsSet *fields) const {
	if (!type_.IsSame(type)) {
		Variant dst(*this);
		return dst.convert(type, payloadType, fields);
	}
	return *this;
}

Variant &Variant::convert(KeyValueType type, const PayloadType *payloadType, const FieldsSet *fields) {
	if (type.IsSame(type_) || type.Is<KeyValueType::Null>() || type_.Is<KeyValueType::Null>()) return *this;
	type.EvaluateOneOf([&](KeyValueType::Int) { *this = Variant(As<int>()); }, [&](KeyValueType::Bool) { *this = Variant(As<bool>()); },
					   [&](KeyValueType::Int64) { *this = Variant(As<int64_t>()); },
					   [&](KeyValueType::Double) { *this = Variant(As<double>()); },
					   [&](KeyValueType::String) { *this = Variant(As<std::string>()); },
					   [&](KeyValueType::Composite) {
						   if (type_.Is<KeyValueType::Tuple>()) {
							   assertrx(payloadType && fields);
							   convertToComposite(payloadType, fields);
						   } else {
							   throw Error(errParams, "Can't convert Variant from type '%s' to type '%s'", type_.Name(), type.Name());
						   }
					   },
					   [&](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Null>) {
						   throw Error(errParams, "Can't convert Variant from type '%s' to type '%s'", type_.Name(), type.Name());
					   });
	type_ = type;
	return *this;
}

void Variant::convertToComposite(const PayloadType *payloadType, const FieldsSet *fields) {
	assertrx(type_.Is<KeyValueType::Tuple>() && hold_);
	key_string val = *cast<key_string>();

	if (hold_) free();
	// Alloc usual payloadvalue + extra memory for hold string

	auto &pv = *new (cast<void>()) PayloadValue(payloadType->TotalSize() + val->size());
	hold_ = true;
	type_ = KeyValueType::Composite{};

	// Copy serializer buffer with strings to extra payloadvalue memory
	char *data = reinterpret_cast<char *>(pv.Ptr() + payloadType->TotalSize());
	memcpy(data, val->data(), val->size());

	Serializer ser(std::string_view(data, val->size()));

	size_t count = ser.GetVarUint();
	if (count != fields->size()) {
		throw Error(errLogic, "Invalid count of arguments for composite index, expected %d, got %d", fields->size(), count);
	}

	Payload pl(*payloadType, pv);

	for (auto field : *fields) {
		if (field != IndexValueType::SetByJsonPath) {
			pl.Set(field, {ser.GetVariant()});
		} else {
			// TODO: will have to implement SetByJsonPath in PayloadIFace
			// or this "mixed" composite queries (by ordinary indexes + indexes
			// from cjson) won't work properly.
			throw Error(errConflict, "SetByJsonPath is not implemented yet");
		}
	}
}

VariantArray Variant::getCompositeValues() const {
	VariantArray res;

	assertrx(type_.Is<KeyValueType::Tuple>());

	Serializer ser(**cast<key_string>());
	size_t count = ser.GetVarUint();
	res.reserve(count);
	while (count--) {
		res.push_back(ser.GetVariant());
	}
	return res;
}

Variant::operator key_string() const {
	assertKeyType<KeyValueType::String>(type_);
	if (hold_) {
		return *cast<key_string>();
	} else if (cast<p_string>()->type() == p_string::tagKeyString) {
		return cast<p_string>()->getKeyString();
	} else {
		return make_key_string(cast<p_string>()->data(), cast<p_string>()->size());
	}
}

Variant::operator p_string() const noexcept {
	assertKeyType<KeyValueType::String>(type_);
	return hold_ ? p_string(*cast<key_string>()) : *cast<p_string>();
}

Variant::operator std::string_view() const noexcept {
	assertKeyType<KeyValueType::String>(type_);
	return hold_ ? std::string_view(**cast<key_string>()) : *cast<p_string>();
}
Variant::operator const PayloadValue &() const noexcept {
	assertKeyType<KeyValueType::Composite>(type_);
	assertrx(hold_);
	return *cast<PayloadValue>();
}

template <typename T>
void Variant::Dump(T &os) const {
	Type().EvaluateOneOf(
		[&](KeyValueType::String) {
			p_string str(*this);
			if (isPrintable(str)) {
				os << '\'' << std::string_view(str) << '\'';
			} else {
				os << "slice{len:" << str.length() << "}";
			}
		},
		[&](KeyValueType::Int) { os << this->operator int(); }, [&](KeyValueType::Bool) { os << this->operator bool(); },
		[&](KeyValueType::Int64) { os << this->operator int64_t(); }, [&](KeyValueType::Double) { os << this->operator double(); },
		[&](KeyValueType::Tuple) { getCompositeValues().Dump(os); },
		[&](OneOf<KeyValueType::Composite, KeyValueType::Undefined, KeyValueType::Null>) { os << "??"; });
}

template void Variant::Dump(WrSerializer &) const;
template void Variant::Dump(std::ostream &) const;

template <typename T>
void VariantArray::Dump(T &os) const {
	os << '{';
	for (auto &arg : *this) {
		if (&arg != &at(0)) os << ", ";
		arg.Dump(os);
	}
	os << '}';
}

template void VariantArray::Dump(WrSerializer &) const;
template void VariantArray::Dump(std::ostream &) const;

VariantArray::VariantArray(Point p) noexcept {
	emplace_back(p.x);
	emplace_back(p.y);
}

VariantArray::operator Point() const {
	if (size() != 2) {
		throw Error(errParams, "Can't convert array of %d elements to Point", size());
	}
	return {(*this)[0].As<double>(), (*this)[1].As<double>()};
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
