#pragma once

#include "core/indexopts.h"
#include "core/key_value_type.h"
#include "estl/h_vector.h"
#include "tools/errors.h"

namespace reindexer {

class WrSerializer;
class Serializer;
class PayloadValue;
class PayloadType;
class FieldsSet;
class VariantArray;
class key_string;
struct p_string;
struct Point;

class Variant {
public:
	Variant() noexcept : type_(KeyValueType::Null{}), value_uint64() {}
	explicit Variant(int v) noexcept : type_(KeyValueType::Int{}), value_int(v) {}
	explicit Variant(bool v) noexcept : type_(KeyValueType::Bool{}), value_bool(v) {}
	explicit Variant(int64_t v) noexcept : type_(KeyValueType::Int64{}), value_int64(v) {}
	explicit Variant(double v) noexcept : type_(KeyValueType::Double{}), value_double(v) {}
	explicit Variant(const char *v);
	explicit Variant(p_string v, bool enableHold = true);
	explicit Variant(const std::string &v);
	explicit Variant(const key_string &v);
	explicit Variant(const PayloadValue &v);
	explicit Variant(PayloadValue &&v);
	explicit Variant(const VariantArray &values);
	explicit Variant(Point);
	Variant(const Variant &other) : type_(other.type_), hold_(other.hold_) {
		if (hold_)
			copy(other);
		else
			value_uint64 = other.value_uint64;
	}
	Variant(Variant &&other) noexcept : type_(other.type_), hold_(other.hold_), value_uint64(other.value_uint64) { other.hold_ = false; }
	~Variant() {
		if (hold_) free();
	}
	template <typename... Ts>
	Variant(const std::tuple<Ts...> &);

	Variant &operator=(const Variant &other) {
		if (this != &other) {
			if (hold_) free();
			type_ = other.type_;
			hold_ = other.hold_;
			if (hold_)
				copy(other);
			else
				value_uint64 = other.value_uint64;
		}
		return *this;
	}

	Variant &operator=(Variant &&other) noexcept {
		if (this != &other) {
			if (hold_) free();
			type_ = other.type_;
			hold_ = other.hold_;
			value_uint64 = other.value_uint64;
			other.hold_ = false;
		}
		return *this;
	}

	explicit operator int() const noexcept;
	explicit operator bool() const noexcept;
	explicit operator int64_t() const noexcept;
	explicit operator double() const noexcept;

	explicit operator p_string() const noexcept;
	explicit operator std::string_view() const noexcept;
	explicit operator const PayloadValue &() const noexcept;
	explicit operator key_string() const;
	explicit operator Point() const;

	template <typename T>
	T As() const;

	template <typename T>
	T As(const PayloadType &, const FieldsSet &) const;

	bool operator==(const Variant &other) const { return Type().IsSame(other.Type()) && Compare(other) == 0; }
	bool operator!=(const Variant &other) const { return !operator==(other); }
	bool operator<(const Variant &other) const { return Compare(other) < 0; }
	bool operator>(const Variant &other) const { return Compare(other) > 0; }
	bool operator>=(const Variant &other) const { return Compare(other) >= 0; }

	int Compare(const Variant &other, const CollateOpts &collateOpts = CollateOpts()) const;
	int RelaxCompare(const Variant &other, const CollateOpts &collateOpts = CollateOpts()) const;
	size_t Hash() const noexcept;
	void EnsureUTF8() const;
	Variant &EnsureHold();

	KeyValueType Type() const noexcept { return type_; }

	Variant &convert(KeyValueType type, const PayloadType * = nullptr, const FieldsSet * = nullptr);
	Variant convert(KeyValueType type, const PayloadType * = nullptr, const FieldsSet * = nullptr) const;
	VariantArray getCompositeValues() const;

	bool IsNullValue() const noexcept { return type_.Is<KeyValueType::Null>(); }

	template <typename T>
	void Dump(T &os) const;

protected:
	void convertToComposite(const PayloadType *, const FieldsSet *);
	void free() noexcept;
	void copy(const Variant &other);
	template <typename T>
	const T *cast() const noexcept {
		return reinterpret_cast<const T *>(&value_uint64);
	}
	template <typename T>
	T *cast() noexcept {
		return reinterpret_cast<T *>(&value_uint64);
	}

	KeyValueType type_;
	bool hold_ = false;
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
	int relaxCompareWithString(std::string_view) const;
};

class VariantArray : public h_vector<Variant, 2> {
public:
	VariantArray() noexcept = default;
	explicit VariantArray(Point) noexcept;
	explicit operator Point() const;
	void MarkArray() noexcept { isArrayValue = true; }
	void MarkObject() noexcept { isObjectValue = true; }
	using h_vector<Variant, 2>::h_vector;
	using h_vector<Variant, 2>::operator==;
	using h_vector<Variant, 2>::operator!=;
	size_t Hash() const noexcept {
		size_t ret = this->size();
		for (size_t i = 0; i < this->size(); ++i) ret = (ret * 127) ^ this->at(i).Hash();
		return ret;
	}
	bool IsArrayValue() const noexcept { return isArrayValue || (!isObjectValue && size() > 1); }
	bool IsObjectValue() const noexcept { return isObjectValue; }
	bool IsNullValue() const noexcept { return size() == 1 && front().IsNullValue(); }
	KeyValueType ArrayType() const noexcept { return empty() ? KeyValueType::Null{} : front().Type(); }
	template <typename T>
	void Dump(T &os) const;
	int RelaxCompare(const VariantArray &other, const CollateOpts & = CollateOpts{}) const;

private:
	bool isArrayValue = false;
	bool isObjectValue = false;
};

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

}  // namespace reindexer
namespace std {
template <>
struct hash<reindexer::Variant> {
public:
	size_t operator()(const reindexer::Variant &kv) const { return kv.Hash(); }
};
}  // namespace std
