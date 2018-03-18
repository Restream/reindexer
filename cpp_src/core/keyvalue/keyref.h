#pragma once

#include "core/payload/payloadvalue.h"
#include "core/type_consts.h"
#include "estl/h_vector.h"
#include "key_string.h"
#include "keyarray.h"
#include "p_string.h"
#include "tools/errors.h"
namespace reindexer {

class KeyValue;

class KeyRef {
public:
	KeyRef() : type(KeyValueEmpty) {}
	explicit KeyRef(const int &v) : type(KeyValueInt), value_int(v) {}
	explicit KeyRef(const int64_t &v) : type(KeyValueInt64), value_int64(v) {}
	explicit KeyRef(const double &v) : type(KeyValueDouble), value_double(v) {}
	explicit KeyRef(const key_string &v) : type(KeyValueString), value_string(v.get()) {}
	explicit KeyRef(p_string v) : type(KeyValueString), value_string(v) {}
	explicit KeyRef(const PayloadValue &v) : type(KeyValueComposite), value_composite(&v) {}
	KeyRef(const KeyRef &other) : type(other.type), value_int64(other.value_int64) {}
	KeyRef &operator=(const KeyRef &other) {
		if (this != &other) {
			type = other.type;
			value_int64 = other.value_int64;
		}
		return *this;
	}

	inline static void assertKeyType(KeyValueType got, KeyValueType exp) {
		(void)got, (void)exp;
		assertf(exp == got, "Expected value '%s', but got '%s'", TypeName(exp), TypeName(got));
	}

	explicit operator int() const {
		assertKeyType(type, KeyValueInt);
		return value_int;
	}
	explicit operator int64_t() const {
		assertKeyType(type, KeyValueInt64);
		return value_int64;
	}
	explicit operator double() const {
		assertKeyType(type, KeyValueDouble);
		return value_double;
	}

	explicit operator p_string() const {
		assertKeyType(type, KeyValueString);
		return value_string;
	}
	explicit operator Slice() const {
		assertKeyType(type, KeyValueString);
		return Slice(value_string.data(), value_string.size());
	}

	explicit operator const PayloadValue *() const {
		assertKeyType(type, KeyValueComposite);
		return value_composite;
	}
	explicit operator const PayloadValue &() const {
		assertKeyType(type, KeyValueComposite);
		return *value_composite;
	}

	explicit operator key_string() const {
		assertKeyType(type, KeyValueString);
		return make_key_string(value_string.data(), value_string.length());
	}
	template <typename T>
	T As() const;

	bool operator==(const KeyRef &other) const { return Compare(other) == 0; }
	bool operator!=(const KeyRef &other) const { return Compare(other) != 0; }
	bool operator<(const KeyRef &other) const { return Compare(other) < 0; }
	bool operator>(const KeyRef &other) const { return Compare(other) > 0; }
	bool operator>=(const KeyRef &other) const { return Compare(other) >= 0; }

	int Compare(const KeyRef &other, CollateMode collateMode = CollateMode::CollateNone) const;
	size_t Hash() const;
	void EnsureUTF8 () const;

	KeyValueType Type() const { return type; }
	static const char *TypeName(KeyValueType t);

protected:
	KeyValueType type;
	union {
		int value_int;
		int64_t value_int64;
		double value_double;
		p_string value_string;
		const PayloadValue *value_composite;
	};
};

using KeyRefs = KeyArray<KeyRef, 8>;

}  // namespace reindexer
