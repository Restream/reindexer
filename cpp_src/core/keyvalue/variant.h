#pragma once

#include <optional>

#include "core/indexopts.h"
#include "core/key_value_type.h"
#include "core/payload/payloadvalue.h"
#include "estl/comparation_result.h"
#include "estl/h_vector.h"
#include "geometry.h"
#include "p_string.h"

namespace reindexer {

class WrSerializer;
class Serializer;
class PayloadType;
class FieldsSet;
class VariantArray;
class Uuid;

enum class WithString : bool { No = false, Yes = true };
enum class NotComparable : bool { Throw = false, Return = true };
enum class CheckIsStringPrintable : bool { No = false, Yes = true };

class Variant {
	friend Uuid;

public:
	struct no_hold_t {};
	struct hold_t {};

	Variant() noexcept : variant_{0, 0, KeyValueType::Null{}, uint64_t{}} {}
	explicit Variant(int v) noexcept : variant_{0, 0, KeyValueType::Int{}, v} {}
	explicit Variant(bool v) noexcept : variant_{0, 0, KeyValueType::Bool{}, v} {}
	explicit Variant(int64_t v) noexcept : variant_{0, 0, KeyValueType::Int64{}, v} {}
	explicit Variant(double v) noexcept : variant_{0, 0, KeyValueType::Double{}, v} {}
	explicit Variant(const char* v) noexcept : Variant(p_string(v), Variant::no_hold_t{}) {}
	Variant(p_string v, no_hold_t) noexcept : variant_{0, 0, KeyValueType::String{}} { *cast<p_string>() = v; }
	Variant(p_string v, hold_t);
	explicit Variant(p_string v) noexcept : Variant(v, no_hold_t{}) {}
	explicit Variant(const std::string& v) : variant_{0, 1, KeyValueType::String{}} { new (cast<void>()) key_string(make_key_string(v)); }
	explicit Variant(std::string&& v) : variant_{0, 1, KeyValueType::String{}} {
		new (cast<void>()) key_string(make_key_string(std::move(v)));
	}
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

	explicit operator p_string() const noexcept;
	explicit operator std::string_view() const noexcept;
	explicit operator const PayloadValue&() const noexcept;
	explicit operator key_string() const;
	explicit operator Point() const;

	template <typename T>
	[[nodiscard]] T As() const;

	template <typename T>
	[[nodiscard]] T As(const PayloadType&, const FieldsSet&) const;

	bool operator==(const Variant& other) const {
		return Type().IsSame(other.Type()) && Compare<NotComparable::Throw>(other) == ComparationResult::Eq;
	}
	bool operator!=(const Variant& other) const { return !operator==(other); }
	bool operator<(const Variant& other) const { return Compare<NotComparable::Throw>(other) == ComparationResult::Lt; }
	bool operator>(const Variant& other) const { return Compare<NotComparable::Throw>(other) == ComparationResult::Gt; }
	bool operator>=(const Variant& other) const { return Compare<NotComparable::Throw>(other) & ComparationResult::Ge; }
	bool operator<=(const Variant& other) const { return Compare<NotComparable::Throw>(other) & ComparationResult::Le; }

	template <NotComparable notComparable>
	ComparationResult Compare(const Variant& other, const CollateOpts& collateOpts = CollateOpts()) const;
	template <WithString, NotComparable>
	ComparationResult RelaxCompare(const Variant& other, const CollateOpts& collateOpts = CollateOpts()) const;
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
	[[nodiscard]] Variant convert(KeyValueType type, const PayloadType* pt = nullptr, const FieldsSet* fs = nullptr) && {
		return std::move(convert(type, pt, fs));
	}
	[[nodiscard]] Variant convert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) const&;
	[[nodiscard]] std::optional<Variant> tryConvert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) const&;
	[[nodiscard]] bool tryConvert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) &;
	auto tryConvert(KeyValueType type, const PayloadType* = nullptr, const FieldsSet* = nullptr) const&& = delete;
	VariantArray getCompositeValues() const;

	bool IsNullValue() const noexcept { return Type().Is<KeyValueType::Null>(); }

	template <typename T>
	void Dump(T& os, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	std::string Dump(CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;

	class Less {
	public:
		Less(const CollateOpts& collate) noexcept : collate_{&collate} {}
		[[nodiscard]] bool operator()(const Variant& lhs, const Variant& rhs) const {
			return lhs.Compare<NotComparable::Throw>(rhs, *collate_) == ComparationResult::Lt;
		}

	private:
		const CollateOpts* collate_;
	};

	class EqualTo {
	public:
		EqualTo(const CollateOpts& collate) noexcept : collate_{&collate} {}
		[[nodiscard]] bool operator()(const Variant& lhs, const Variant& rhs) const {
			return lhs.Compare<NotComparable::Throw>(rhs, *collate_) == ComparationResult::Eq;
		}

	private:
		const CollateOpts* collate_;
	};

private:
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

	struct Var {
		Var(uint8_t isu, uint8_t h, KeyValueType t) noexcept : isUuid{isu}, hold{h}, type{t} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, bool b) noexcept : isUuid{isu}, hold{h}, type{t}, value_bool{b} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, int i) noexcept : isUuid{isu}, hold{h}, type{t}, value_int{i} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, int64_t i) noexcept : isUuid{isu}, hold{h}, type{t}, value_int64{i} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, uint64_t u) noexcept : isUuid{isu}, hold{h}, type{t}, value_uint64{u} {}
		Var(uint8_t isu, uint8_t h, KeyValueType t, double d) noexcept : isUuid{isu}, hold{h}, type{t}, value_double{d} {}

		uint8_t isUuid : 1;
		uint8_t hold : 1;
		KeyValueType type;
		union {
			bool value_bool;
			int value_int;
			int64_t value_int64;
			uint64_t value_uint64;
			double value_double;
			// runtime cast
			// p_string value_string;
			// PayloadValue value_composite;
			// key_string h_value_string;
		};
	};
	struct UUID {
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

extern template ComparationResult Variant::RelaxCompare<WithString::Yes, NotComparable::Throw>(const Variant&, const CollateOpts&) const;
extern template ComparationResult Variant::RelaxCompare<WithString::No, NotComparable::Throw>(const Variant&, const CollateOpts&) const;
extern template ComparationResult Variant::RelaxCompare<WithString::Yes, NotComparable::Return>(const Variant&, const CollateOpts&) const;
extern template ComparationResult Variant::RelaxCompare<WithString::No, NotComparable::Return>(const Variant&, const CollateOpts&) const;
extern template ComparationResult Variant::Compare<NotComparable::Return>(const Variant&, const CollateOpts&) const;
extern template ComparationResult Variant::Compare<NotComparable::Throw>(const Variant&, const CollateOpts&) const;

template <>
int Variant::As<int>() const;
template <>
int64_t Variant::As<int64_t>() const;
template <>
double Variant::As<double>() const;
template <>
bool Variant::As<bool>() const;
template <>
std::string Variant::As<std::string>() const;

class VariantArray : public h_vector<Variant, 2> {
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
	using Base::operator==;
	using Base::operator!=;
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
	KeyValueType ArrayType() const noexcept { return empty() ? KeyValueType::Null{} : front().Type(); }
	template <typename T>
	void Dump(T& os, CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	std::string Dump(CheckIsStringPrintable checkPrintableString = CheckIsStringPrintable::Yes) const;
	template <WithString, NotComparable>
	ComparationResult RelaxCompare(const VariantArray& other, const CollateOpts& = CollateOpts{}) const;
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
			res.emplace_back(std::move(v));
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

extern template ComparationResult VariantArray::RelaxCompare<WithString::Yes, NotComparable::Return>(const VariantArray&,
																									 const CollateOpts&) const;
extern template ComparationResult VariantArray::RelaxCompare<WithString::No, NotComparable::Return>(const VariantArray&,
																									const CollateOpts&) const;
extern template ComparationResult VariantArray::RelaxCompare<WithString::Yes, NotComparable::Throw>(const VariantArray&,
																									const CollateOpts&) const;
extern template ComparationResult VariantArray::RelaxCompare<WithString::No, NotComparable::Throw>(const VariantArray&,
																								   const CollateOpts&) const;

}  // namespace reindexer
namespace std {
template <>
struct hash<reindexer::Variant> {
	size_t operator()(const reindexer::Variant& kv) const { return kv.Hash(); }
};
}  // namespace std
