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

int KeyValue::convert(KeyValueType _type) {
	if (_type == type) return 0;
	switch (_type) {
		case KeyValueInt:
			value_int = As<int>();
			break;
		case KeyValueInt64:
			value_int64 = As<int64_t>();
			break;
		case KeyValueDouble:
			value_double = As<double>();
			break;
		case KeyValueString: {
			auto s = As<string>();
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
