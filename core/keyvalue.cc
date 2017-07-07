#include "core/keyvalue.h"
#include <inttypes.h>
#include <stdio.h>
#include <functional>
#include "tools/errors.h"

namespace reindexer {

using std::to_string;
using std::stoi;
using std::stoull;
using std::stod;
using std::hash;

string KeyValue::toString() const {
	switch (type) {
		case KeyValueInt:
			return to_string(value_int);
		case KeyValueInt64:
			return to_string(value_int64);
		case KeyValueDouble:
			return to_string(value_double);
		case KeyValueString:
			return *value_string;
		default:
			abort();
	}
}

int KeyValue::toInt() const {
	try {
		switch (type) {
			case KeyValueInt:
				return value_int;
			case KeyValueInt64:
				return value_int64;
			case KeyValueDouble:
				return (int)value_double;
			case KeyValueString:
				return stoi(*value_string);
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", value_string->c_str());
	}
}

int64_t KeyValue::toInt64() const {
	try {
		switch (type) {
			case KeyValueInt:
				return value_int;
			case KeyValueInt64:
				return value_int64;
			case KeyValueDouble:
				return (int64_t)value_double;
			case KeyValueString:
				return stoull(*value_string);
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", value_string->c_str());
	}
}

double KeyValue::toDouble() const {
	try {
		switch (type) {
			case KeyValueInt:
				return (double)value_int;
			case KeyValueInt64:
				return (double)value_int64;
			case KeyValueDouble:
				return value_double;
			case KeyValueString:
				return stod(*value_string);
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", value_string->c_str());
	}
}

int KeyValue::convert(KeyValueType _type) {
	if (_type == type) return 0;
	switch (_type) {
		case KeyValueInt:
			value_int = toInt();
			break;
		case KeyValueInt64:
			value_int64 = toInt64();
			break;
		case KeyValueDouble:
			value_double = toDouble();
			break;
		case KeyValueString:
			value_string = make_shared<string>(toString());
			break;
		default:
			assertf(0, "Can't convert KeyValue from type '%s' to to type '%s'", TypeName(type), TypeName(_type));
	}
	type = _type;
	return 0;
}

const char *KeyValue::TypeName(KeyValueType t) {
	switch (t) {
		case KeyValueInt:
			return "int";
		case KeyValueInt64:
			return "int64";
		case KeyValueDouble:
			return "double";
		case KeyValueString:
			return "string";
		case KeyValueComposite:
			return "<composite>";
		case KeyValueEmpty:
			return "<empty>";
		case KeyValueUndefined:
			return "<unknown>";
	}
	return "<invalid type>";
}
bool KeyValue::operator<(const KeyValue &v2) const { return (KeyRef)(*this) < (KeyRef)v2; }
bool KeyValue::operator==(const KeyValue &v2) const { return (KeyRef)(*this) == (KeyRef)v2; }

KeyRef::operator KeyValue() const {
	switch (type) {
		case KeyValueInt:
			return KeyValue(value_int);
		case KeyValueInt64:
			return KeyValue(value_int64);
		case KeyValueDouble:
			return KeyValue(value_double);
		case KeyValueString:
			return KeyValue(make_shared<string>(value_string.data(), value_string.length()));
		case KeyValueComposite:
			return KeyValue(*value_composite);
		case KeyValueEmpty:
		case KeyValueUndefined:
			return KeyValue();
	}
	assert(0);
}

bool KeyRef::EQ(const KeyRef &other) const {
	switch (Type()) {
		case KeyValueInt:
			return (value_int == other.value_int);
		case KeyValueInt64:
			return (value_int64 == other.value_int64);
		case KeyValueDouble:
			return (value_double == other.value_double);
		case KeyValueString: {
			int l1 = value_string.length();
			int l2 = other.value_string.length();
			if (l1 != l2) return false;
			return memcmp(value_string.data(), other.value_string.data(), l1) == 0;
		}
		default:
			abort();
	}
}

bool KeyRef::Less(const KeyRef &other) const {
	switch (Type()) {
		case KeyValueInt:
			return (value_int < other.value_int);
		case KeyValueInt64:
			return (value_int64 < other.value_int64);
		case KeyValueDouble:
			return (value_double < other.value_double);
		case KeyValueString: {
			int l1 = value_string.length();
			int l2 = other.value_string.length();
			int res = memcmp(value_string.data(), other.value_string.data(), std::min(l1, l2));
			return res ? (res < 0) : (l1 < l2);
		}
		default:
			abort();
	}
}

size_t KeyRef::Hash() const {
	switch (Type()) {
		case KeyValueInt:
			return hash<int>()(value_int);
		case KeyValueInt64:
			return hash<int64_t>()((int64_t)value_int64);
		case KeyValueDouble:
			return hash<double>()((double)value_double);
		case KeyValueString:
			return hash<p_string>()((p_string)value_string);
		default:
			abort();
	}
}

KeyValue::operator KeyRef() const {
	switch (type) {
		case KeyValueInt:
			return KeyRef(value_int);
		case KeyValueInt64:
			return KeyRef(value_int64);
		case KeyValueDouble:
			return KeyRef(value_double);
		case KeyValueString:
			return KeyRef(value_string);
		case KeyValueComposite:
			return KeyRef(value_composite);
		case KeyValueEmpty:
		case KeyValueUndefined:
			return KeyRef();
	}
	assert(0);
}

}  // namespace reindexer
