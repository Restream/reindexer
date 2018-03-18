#include "keyref.h"
#include <functional>

#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"

namespace reindexer {

using std::hash;

template <>
string KeyRef::As<string>() const {
	switch (type) {
		case KeyValueInt:
			return std::to_string(value_int);
		case KeyValueInt64:
			return std::to_string(value_int64);
		case KeyValueDouble:
			return std::to_string(value_double);
		case KeyValueString:
			if (value_string.type() == p_string::tagCxxstr) {
				return *value_string.getCxxstr();
			}
			return value_string.toString();
		case KeyValueComposite:
			return string();
		default:
			abort();
	}
}

template <>
int KeyRef::As<int>() const {
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
				auto res = std::stoi(value_string.data(), &idx);
				if (idx != value_string.length()) {
					throw std::exception();
				}
				return res;
			}
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", value_string.data());
	}
}

template <>
int64_t KeyRef::As<int64_t>() const {
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
				auto res = std::stoull(value_string.data(), &idx);
				if (idx != value_string.length()) {
					throw std::exception();
				}
				return res;
			}
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", value_string.data());
	}
}

template <>
double KeyRef::As<double>() const {
	try {
		switch (type) {
			case KeyValueInt:
				return double(value_int);
			case KeyValueInt64:
				return double(value_int64);
			case KeyValueDouble:
				return value_double;
			case KeyValueString:
				return std::stod(value_string.data());
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", value_string.data());
	}
}

int KeyRef::Compare(const KeyRef &other, CollateMode collateMode) const {
	switch (Type()) {
		case KeyValueInt:
			if (value_int == other.value_int)
				return 0;
			else if (value_int > other.value_int)
				return 1;
			else
				return -1;
		case KeyValueInt64:
			if (value_int64 == other.value_int64)
				return 0;
			else if (value_int64 > other.value_int64)
				return 1;
			else
				return -1;
		case KeyValueDouble:
			if (value_double == other.value_double)
				return 0;
			else if (value_double > other.value_double)
				return 1;
			else
				return -1;
		case KeyValueString: {
			size_t l1 = value_string.length();
			size_t l2 = other.value_string.length();

			Slice lhs(value_string.data(), l1);
			Slice rhs(other.value_string.data(), l2);

			return collateCompare(lhs, rhs, collateMode);
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
			return hash<int64_t>()(int64_t(value_int64));
		case KeyValueDouble:
			return hash<double>()(double(value_double));
		case KeyValueString:
			return hash<p_string>()(p_string(value_string));
		default:
			abort();
	}
}

void KeyRef::EnsureUTF8() const {
	if (type == KeyValueString) {
		if (!utf8::is_valid(value_string.data(), value_string.data() + value_string.size())) {
			throw Error(errParams, "Invalid UTF8 string passed to index with CollateUTF8 mode");
		}
	}
}

const char *KeyRef::TypeName(KeyValueType t) {
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

}  // namespace reindexer
