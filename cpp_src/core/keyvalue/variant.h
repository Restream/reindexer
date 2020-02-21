#pragma once

#include "core/indexopts.h"
#include "core/type_consts.h"
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

class Variant {
public:
	Variant() : type_(KeyValueNull) {}
	explicit Variant(int v) : type_(KeyValueInt), value_int(v) {}
	explicit Variant(bool v) : type_(KeyValueBool), value_bool(v) {}
	explicit Variant(int64_t v) : type_(KeyValueInt64), value_int64(v) {}
	explicit Variant(double v) : type_(KeyValueDouble), value_double(v) {}
	explicit Variant(const char *v);
	explicit Variant(p_string v, bool enableHold = true);
	explicit Variant(const string &v);
	explicit Variant(const key_string &v);
	explicit Variant(const PayloadValue &v);
	Variant(const VariantArray &values);
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

	explicit operator int() const;
	explicit operator bool() const;
	explicit operator int64_t() const;
	explicit operator double() const;

	explicit operator p_string() const;
	explicit operator string_view() const;
	explicit operator const PayloadValue &() const;
	explicit operator key_string() const;

	template <typename T>
	T As() const;

	bool operator==(const Variant &other) const { return Compare(other) == 0; }
	bool operator!=(const Variant &other) const { return Compare(other) != 0; }
	bool operator<(const Variant &other) const { return Compare(other) < 0; }
	bool operator>(const Variant &other) const { return Compare(other) > 0; }
	bool operator>=(const Variant &other) const { return Compare(other) >= 0; }

	int Compare(const Variant &other, const CollateOpts &collateOpts = CollateOpts()) const;
	int RelaxCompare(const Variant &other, const CollateOpts &collateOpts = CollateOpts()) const;
	size_t Hash() const;
	void EnsureUTF8() const;
	Variant &EnsureHold();

	KeyValueType Type() const { return type_; }
	static const char *TypeName(KeyValueType t);

	Variant &convert(KeyValueType type, const PayloadType * = nullptr, const FieldsSet * = nullptr);
	Variant convert(KeyValueType type, const PayloadType * = nullptr, const FieldsSet * = nullptr) const;
	VariantArray getCompositeValues() const;

	bool IsNullValue() const;

	void Dump(WrSerializer &wrser) const;

protected:
	void convertToComposite(const PayloadType *, const FieldsSet *);
	void free();
	void copy(const Variant &other);
	template <typename T>
	const T *cast() const {
		return reinterpret_cast<const T *>(&value_uint64);
	}
	template <typename T>
	T *cast() {
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
	int relaxCompareWithString(string_view) const;
};	// namespace reindexer

class VariantArray : public h_vector<Variant, 2> {
public:
	using h_vector<Variant, 2>::h_vector;
	using h_vector<Variant, 2>::operator==;
	using h_vector<Variant, 2>::operator!=;
	size_t Hash() const {
		size_t ret = this->size();
		for (size_t i = 0; i < this->size(); ++i) ret = (ret * 127) ^ this->at(i).Hash();
		return ret;
	}
	bool IsNullValue() const;
	void Dump(WrSerializer &wrser) const;
};

}  // namespace reindexer
namespace std {
template <>
struct hash<reindexer::Variant> {
public:
	size_t operator()(const reindexer::Variant &kv) const { return kv.Hash(); }
};
}  // namespace std
