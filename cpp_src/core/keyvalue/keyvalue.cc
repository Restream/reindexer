#include "keyvalue.h"
#include <cstring>
#include "tools/errors.h"

namespace reindexer {

KeyValue::KeyValue(const KeyRef &other) : KeyRef(other) {
	if (type == KeyValueComposite) h_value_composite = static_cast<PayloadValue>(other);
	if (type == KeyValueString) h_value_string = static_cast<key_string>(other);
	relink();
}

KeyValue::KeyValue(const KeyValue &other) : KeyRef(other) {
	h_value_composite = other.h_value_composite;
	h_value_string = other.h_value_string;
	relink();
}

KeyValue &KeyValue::operator=(const KeyValue &other) {
	if (&other != this) {
		KeyRef::operator=(other);
		h_value_composite = other.h_value_composite;
		h_value_string = other.h_value_string;
		relink();
	}
	return *this;
}

string KeyValue::toString() const {
	switch (type) {
		case KeyValueInt:
			return std::to_string(value_int);
		case KeyValueInt64:
			return std::to_string(value_int64);
		case KeyValueDouble:
			return std::to_string(value_double);
		case KeyValueString:
			return *h_value_string;
		case KeyValueComposite:
			return string();
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
				return int(value_double);
			case KeyValueString: {
				size_t idx = 0;
				auto res = std::stoi(*h_value_string, &idx);
				if (idx != h_value_string->length()) {
					throw std::exception();
				}
				return res;
			}
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", h_value_string->c_str());
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
				return int64_t(value_double);
			case KeyValueString: {
				size_t idx = 0;
				auto res = std::stoull(*h_value_string, &idx);
				if (idx != h_value_string->length()) {
					throw std::exception();
				}
				return res;
			}
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", h_value_string->c_str());
	}
}

double KeyValue::toDouble() const {
	try {
		switch (type) {
			case KeyValueInt:
				return double(value_int);
			case KeyValueInt64:
				return double(value_int64);
			case KeyValueDouble:
				return value_double;
			case KeyValueString:
				return std::stod(*h_value_string);
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", h_value_string->c_str());
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
		case KeyValueString: {
			auto s = toString();
			h_value_string = make_key_string(s.data(), s.length());
			relink();
		} break;
		default:
			assertf(0, "Can't convert KeyValue from type '%s' to to type '%s'", TypeName(type), TypeName(_type));
	}
	type = _type;
	return 0;
}

}  // namespace reindexer
