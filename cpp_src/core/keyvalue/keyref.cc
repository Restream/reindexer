#include "keyref.h"
#include <functional>

#include "tools/stringstools.h"

namespace reindexer {

using std::hash;

int KeyRef::Compare(const KeyRef &other, int collateMode) const {
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
