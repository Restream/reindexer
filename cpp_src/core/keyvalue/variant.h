#pragma once

#include <optional>

#include "core/indexopts.h"
#include "core/key_value_type.h"
#include "core/payload/payloadvalue.h"
#include "estl/comparation_result.h"
#include "estl/h_vector.h"
#include "float_vector.h"
#include "geometry.h"
#include "p_string.h"

namespace reindexer {

class WrSerializer;
class Serializer;
class PayloadType;
class FieldsSet;
class VariantArray;
class Uuid;
class Variant;

enum class [[nodiscard]] WithString : bool { No = false, Yes = true };
enum class [[nodiscard]] NotComparable : bool { Throw = false, Return = true };
enum class [[nodiscard]] CheckIsStringPrintable : bool { No = false, Yes = true };
enum class [[nodiscard]] NullsHandling : int8_t { NotComparable, AlwaysLess };

constexpr auto kDefaultNullsHandling = NullsHandling::AlwaysLess;
constexpr auto kWhereCompareNullHandling = NullsHandling::NotComparable;

namespace variant_compare_helpers {
template <NullsHandling nullsHandling>
RX_ALWAYS_INLINE static std::optional<ComparationResult> handleNulls(const Variant& lhs, const Variant& rhs) noexcept;
}  // namespace variant_compare_helpers

class [[nodiscard]] Variant {
	friend Uuid;

public:
	struct [[nodiscard]] NoHoldT {};
	struct [[nodiscard]] HoldT {};

	static constexpr HoldT hold{};
	static constexpr NoHoldT noHold{};

	Variant() noexcept : variant_{0, 0, KeyValueType::Null{}, uint64_t{}} {}
	explicit Variant(int v) noexcept : variant_{0, 0, KeyValueType::Int{}, v} {}
	explicit Variant(bool v) noexcept : variant_{0, 0, KeyValueType::Bool{}, v} {}
	explicit Variant(int64_t v) noexcept : variant_{0, 0, KeyValueType::Int64{}, v} {}
	explicit Variant(float v) noexcept : variant_{0, 0, KeyValueType::Float{}, v} {}
	explicit Variant(double v) noexcept : variant_{0, 0, KeyValueType::Double{}, v} {}
	explicit Variant(const char* v) noexcept : Variant(p_string(v), noHold) {}
	Variant(p_string v, NoHoldT) noexcept : variant_{0, 0, KeyValueType::String{}} { *cast<p_string>() = v; }
	Variant(p_string v, HoldT);
	explicit Variant(p_string v) noexcept : Variant(v, noHold) {}
	explicit Variant(const std::string& v) : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(make_key_string(v)); }
	explicit Variant(std::string_view v) : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(make_key_string(v)); }
	explicit Variant(const key_string& v) noexcept : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(v); }
	explicit Variant(key_string&& v) noexcept : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(std::move(v)); }
	explicit Variant(const PayloadValue& v) noexcept : variant_{0, 1, KeyValueType::Composite{}} { new (cast<void>()) PayloadValue(v); }
	explicit Variant(PayloadValue&& v) noexcept : variant_{0, 1, KeyValueType::Composite{}} {
		new (cast<void>()) PayloadValue(std::move(v));
	}
	explicit Variant(const VariantArray& values);
	explicit Variant(Point);
	explicit Variant(Uuid) noexcept;
	explicit Variant(ConstFloatVectorView v, NoHoldT = noHold) noexcept : variant_{0, 0, KeyValueType::FloatVector{}, v.Payload()} {}
	Variant(ConstFloatVectorView v, HoldT) : Variant(FloatVector(v)) {}
	Variant(FloatVector&& v) noexcept : variant_{0, 1, KeyValueType::FloatVector{}, ConstFloatVectorView(v).Payload()} {
		std::move(v).Release();
	}
	Variant(const Variant& other) : uuid_{other.uuid_} {
		if (!isUuid()) {
			uuid_.~UUID();
			new (&variant_) Var{other.variant_};
			if (variant_.hold != 0) {
				copy(other);
			}
		}
	}
	Variant(Variant&& other) noexcept : uuid_{other.uuid_} {
		if (!isUuid()) {
			uuid_.~UUID();
			new (&variant_) Var{other.variant_};
			other.variant_.hold = 0;
		}
	}
	template <typename... Ts>
	Variant(const std::tuple<Ts...>&);

	~Variant() {
		if (!isUuid() && variant_.hold != 0) {
			free();
		}
	}

	Variant& operator=(Variant&& other) & noexcept {
		if (this == &other) {
			return *this;
		}
		if (isUuid()) {
			if (other.isUuid()) {
				uuid_ = other.uuid_;
			} else {
				uuid_.~UUID();
				new (&variant_) Var{other.variant_};
				other.variant_.hold = 0;
			}
		} else {
			if (variant_.hold != 0) {
				free();
			}
			if (other.isUuid()) {
				variant_.~Var();
				new (&uuid_) UUID{other.uuid_};
			} else {
				variant_ = other.variant_;
				other.variant_.hold = 0;
			}
		}
		return *this;
	}
	Variant& operator=(const Variant& other) & {
		if (this != &other) {
			Variant tmp{other};
			operator=(std::move(tmp));
		}
		return *this;
	}

	explicit operator int() const noexcept;
	explicit operator bool() const noexcept;
	explicit operator int64_t() const noexcept;
	explicit operator double() const noexcept;
	explicit operator float() const noexcept;

	explicit operator p_string() const noexcept;
	explicit operator std::string_view() const noexcept;
	explicit operator const PayloadValue&() const noexcept;
	explicit operator key_string() const;
	explicit operator Point() const;
	explicit operator ConstFloatVectorView() const noexcept;
	explicit operator FloatVectorView() noexcept;

	template <typename T>
	T As() const;

	template <typename T>
	T As(const PayloadType&, const FieldsSet&) const;

	bool operator==(const Variant& other) const {
		return Compare<NotComparable::Return, kDefaultNullsHandling>(other) == ComparationResult::Eq;
	}
	bool operator!=(const Variant& other) const { return !operator==(other); }
	bool operator<(const Variant& other) const {
		return Compare<NotComparable::Throw, kDefaultNullsHandling>(other) == ComparationResult::Lt;
	}
	bool operator>(const Variant& other) const {
		return Compare<NotComparable::Throw, kDefaultNullsHandling>(other) == ComparationResult::Gt;
	}
	bool operator>=(const Variant& other) const {
		return Compare<NotComparable::Throw, kDefaultNullsHandling>(other) & ComparationResult::Ge;
	}
	bool operator<=(const Variant& other) const {
		return Compare<NotComparable::Throw, kDefaultNullsHandling>(other) & ComparationResult::Le;
	}

	template <NotComparable notComparable, NullsHandling nullsHandling>
	ComparationResult Compare(const Variant& other, const CollateOpts& collateOpts = CollateOpts()) const {
		if (auto res = variant_compare_helpers::handleNulls<nullsHandling>(*this, other); res) {
			return *res;
		}
		return compareImpl<notComparable>(other, collateOpts);
	}
	template <WithString withString, NotComparable notComparable, NullsHandling nullsHandling>
	ComparationResult RelaxCompare(const Variant& other, const CollateOpts& collateOpts = CollateOpts()) const {
		if (auto res = variant_compare_helpers::handleNulls<nullsHandling>(*this, other); res) {
			return *res;
		}
		return relaxCompareImpl<withString, notComparable>(other, nullsHandling, collateOpts);
	}
	size_t Hash() const noexcept;
	void EnsureUTF8() const;
	Variant& EnsureHold() &;
	Variant EnsureHold() && { return std::move(EnsureHold()); }

	KeyValueType Type() const noexcept {
		if (isUuid()) {
			return KeyValueType::Uuid{};
		}
		return variant_.type;
	}

	Variant& convert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) &;
	Variant convert(KeyValueType type, const PayloadType* pt = nullptr, const FieldsSet* fs = nullptr) && {
		return std::move(convert(type, pt, fs));
	}
	Variant convert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) const&;
	std::optional<Variant> tryConvert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) const&;
	bool tryConvert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) &;
	auto tryConvert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) const&& = delete;
	VariantArray getCompositeValues() const;

	bool IsNullValue() const noexcept { return Type().Is<KeyValueType::Null>(); }

	template <typename T>
	void Dump(T& os, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	template <typename T>
	void Dump(T& os, const PayloadType&, const FieldsSet&, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	std::string Dump(CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	std::string Dump(const PayloadType&, const FieldsSet&, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;

	class [[nodiscard]] Less {
	public:
		Less(const CollateOpts& collate) noexcept : collate_{&collate} {}
		bool operator()(const Variant& lhs, const Variant& rhs) const {
			return lhs.Compare<NotComparable::Throw, NullsHandling::NotComparable>(rhs, *collate_) == ComparationResult::Lt;
		}

	private:
		const CollateOpts* collate_;
	};

	class [[nodiscard]] EqualTo {
	public:
		EqualTo(const CollateOpts& collate) noexcept : collate_{&collate} {}
		bool operator()(const Variant& lhs, const Variant& rhs) const {
			return lhs.Compare<NotComparable::Throw, NullsHandling::NotComparable>(rhs, *collate_) == ComparationResult::Eq;
		}

	private:
		const CollateOpts* collate_;
	};
	bool DoHold() const noexcept { return !isUuid() && variant_.hold; }

private:
	template <NotComparable>
	ComparationResult compareImpl(const Variant& other, const CollateOpts& collateOpts) const;
	template <WithString, NotComparable>
	ComparationResult relaxCompareImpl(const Variant& other, NullsHandling tupleNullsHandling, const CollateOpts& collateOpts) const;

	bool isUuid() const noexcept { return uuid_.isUuid != 0; }
	void convertToComposite(const PayloadType&, const FieldsSet&);
	void free() noexcept;
	void copy(const Variant& other);
	template <typename T>
	const T* cast() const noexcept {
		assertrx(!isUuid());
		return reinterpret_cast<const T*>(&variant_.value_uint64);
	}
	template <typename T>
	T* cast() noexcept {
		assertrx(!isUuid());
		return reinterpret_cast<T*>(&variant_.value_uint64);
	}
	template <NotComparable notComparable>
	ComparationResult relaxCompareWithString(std::string_view) const noexcept(notComparable == NotComparable::Return);

	struct [[nodiscard]] Var {
		Var(uint8_t isu, uint8_t h, KeyValueType t) noexcept : isUuid{isu}, hold{h}, type{t} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, bool b) noexcept : isUuid{isu}, hold{h}, type{t}, value_bool{b} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, int i) noexcept : isUuid{isu}, hold{h}, type{t}, value_int{i} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, int64_t i) noexcept : isUuid{isu}, hold{h}, type{t}, value_int64{i} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, uint64_t u) noexcept : isUuid{isu}, hold{h}, type{t}, value_uint64{u} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, double d) noexcept : isUuid{isu}, hold{h}, type{t}, value_double{d} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, float d) noexcept : isUuid{isu}, hold{h}, type{t}, value_float{d} {}

		uint8_t isUuid : 1;
		uint8_t hold : 1;
		KeyValueType type;
		union {
			bool value_bool;
			int value_int;
			int64_t value_int64;
			uint64_t value_uint64;
			double value_double;
			float value_float;
		};
	};
	struct [[nodiscard]] UUID {
		uint8_t isUuid : 1;
		uint8_t v0 : 7;
		uint8_t vs[7];
		uint64_t v1;
	};
	static_assert(sizeof(UUID) == sizeof(Var));
	static_assert(alignof(UUID) == alignof(Var));
	union {
		Var variant_;
		UUID uuid_;
	};
};

static_assert(sizeof(Variant) == 16);

template <>
int Variant::As<int>() const;
template <>
int64_t Variant::As<int64_t>() const;
template <>
double Variant::As<double>() const;
template <>
float Variant::As<float>() const;
template <>
bool Variant::As<bool>() const;
template <>
std::string Variant::As<std::string>() const;
template <>
key_string Variant::As<key_string>() const;
template <>
ConstFloatVectorView Variant::As<ConstFloatVectorView>() const;

class [[nodiscard]] VariantArray : public h_vector<Variant, 2> {
	using Base = h_vector<Variant, 2>;

public:
	VariantArray() noexcept = default;
	VariantArray(const VariantArray&) = default;
	VariantArray(VariantArray&&) = default;
	VariantArray& operator=(const VariantArray&) = default;
	VariantArray& operator=(VariantArray&&) = default;

	explicit VariantArray(Point p) noexcept {
		emplace_back(p.X());
		emplace_back(p.Y());
	}
	explicit operator Point() const;
	VariantArray& MarkArray(bool v = true) & noexcept {
		isArrayValue = v;
		return *this;
	}
	VariantArray&& MarkArray(bool v = true) && noexcept {
		isArrayValue = v;
		return std::move(*this);
	}
	void MarkObject() noexcept { isObjectValue = true; }
	using Base::Base;
	bool operator==(const VariantArray& other) const noexcept {
		const static CollateOpts opts;
		return CompareNoExcept<kDefaultNullsHandling>(other, opts) == ComparationResult::Eq;
	}
	bool operator!=(const VariantArray& other) const noexcept { return !operator==(other); }
	template <bool FreeHeapMemory = true>
	void clear() noexcept {
		isArrayValue = isObjectValue = false;
		Base::clear<FreeHeapMemory>();
	}
	size_t Hash() const noexcept {
		size_t ret = this->size();
		for (auto& v : *this) {
			ret = (ret * 127) ^ v.Hash();
		}
		return ret;
	}
	bool IsArrayValue() const noexcept { return isArrayValue || (!isObjectValue && size() > 1); }
	bool IsObjectValue() const noexcept { return isObjectValue; }
	bool IsNullValue() const noexcept { return size() == 1 && front().IsNullValue(); }
	template <typename T>
	void Dump(T& os, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	template <typename T>
	void Dump(T& os, const PayloadType&, const FieldsSet&, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	std::string Dump(CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	std::string Dump(const PayloadType&, const FieldsSet&, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	template <WithString withString, NotComparable notComparable, NullsHandling nullsHandling>
	ComparationResult RelaxCompare(const VariantArray& other, const CollateOpts& collateOpts = CollateOpts{}) const {
		// This method do not check object/array flags
		auto lhsIt{cbegin()}, rhsIt{other.cbegin()};
		const auto lhsEnd{cend()}, rhsEnd{other.cend()};
		for (; lhsIt != lhsEnd && rhsIt != rhsEnd; ++lhsIt, ++rhsIt) {
			const auto res = lhsIt->RelaxCompare<withString, notComparable, nullsHandling>(*rhsIt, collateOpts);
			if (res != ComparationResult::Eq) {
				return res;
			}
		}
		if (lhsIt == lhsEnd) {
			return (rhsIt == rhsEnd) ? ComparationResult::Eq : ComparationResult::Lt;
		}
		return ComparationResult::Gt;
	}
	template <NotComparable, NullsHandling>
	ComparationResult Compare(const VariantArray& other, const CollateOpts& = CollateOpts{}) const;
	template <NullsHandling nullsHandling>
	ComparationResult CompareNoExcept(const VariantArray& other, const CollateOpts& opts) const noexcept {
		try {
			return Compare<NotComparable::Return, nullsHandling>(other, opts);
		} catch (...) {
			return ComparationResult::NotComparable;
		}
	}
	void EnsureHold() {
		for (Variant& v : *this) {
			v.EnsureHold();
		}
	}
	template <typename... Ts>
	static VariantArray Create(Ts&&... vs) {
		return VariantArray{Variant{std::forward<Ts>(vs)}...};
	}
	template <typename T>
	static VariantArray Create(std::initializer_list<T> vs) {
		VariantArray res;
		res.reserve(vs.size());
		for (auto& v : vs) {
			res.emplace_back(v);
		}
		return res;
	}
	void Clear() noexcept {
		clear<false>();
		isArrayValue = false;
		isObjectValue = false;
	}

private:
	bool isArrayValue = false;
	bool isObjectValue = false;
};

// TODO: #1352 Improve allocations count: either store Point as 2 floats, or add void* with static size array
// Current implementation requires 3 allocations for each point
inline Variant::Variant(Point p) : Variant{VariantArray{p}} {}

namespace variant_compare_helpers {
template <NullsHandling nullsHandling>
RX_ALWAYS_INLINE static std::optional<ComparationResult> handleNulls(const Variant& lhs, const Variant& rhs) noexcept {
	if constexpr (nullsHandling == NullsHandling::AlwaysLess) {
		if (lhs.IsNullValue() && rhs.IsNullValue()) {
			return ComparationResult::Eq;
		}
		if (lhs.IsNullValue()) {
			return ComparationResult::Lt;
		}
		if (rhs.IsNullValue()) {
			return ComparationResult::Gt;
		}
	}
	return std::nullopt;
}
}  // namespace variant_compare_helpers

namespace concepts {

template <typename T>
concept ConvertibleToVariant = std::is_constructible_v<Variant, T>;

template <typename T>
concept ConvertibleToVariantArray = std::is_constructible_v<VariantArray, T>;

}  // namespace concepts
}  // namespace reindexer

namespace std {
template <>
struct [[nodiscard]] hash<reindexer::Variant> {
	size_t operator()(const reindexer::Variant& kv) const { return kv.Hash(); }
};
}  // namespace std
