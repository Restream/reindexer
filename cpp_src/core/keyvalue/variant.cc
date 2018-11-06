#include "variant.h"
#include <functional>

#include "core/payload/payloadiface.h"
#include "key_string.h"
#include "p_string.h"
#include "tools/serializer.h"
#include "tools/stringstools.h"
#include "utf8cpp/utf8.h"
#include "variant.h"

namespace reindexer {

using std::hash;

Variant::Variant(const PayloadValue &v) : type_(KeyValueComposite), hold_(true) { new (cast<void>()) PayloadValue(v); }

Variant::Variant(const string &v) : type_(KeyValueString), hold_(true) { new (cast<void>()) key_string(make_key_string(v)); }

Variant::Variant(const key_string &v) : type_(KeyValueString), hold_(true) { new (cast<void>()) key_string(v); }
Variant::Variant(const char *v) : Variant(p_string(v)) {}
Variant::Variant(const p_string &v, bool enableHold) : type_(KeyValueString) {
	if (v.type() == p_string::tagKeyString && enableHold) {
		hold_ = true;
		new (cast<void>()) key_string(v.getKeyString());
	} else {
		*cast<p_string>() = v;
	}
}

Variant::Variant(const VariantArray &values) {
	WrSerializer ser;
	ser.PutVarUint(values.size());
	for (const Variant &kv : values) {
		ser.PutVariant(kv);
	}
	new (cast<void>()) key_string(make_key_string(ser.Slice()));
	type_ = KeyValueTuple;
	hold_ = true;
}

void Variant::free() {
	assert(hold_);
	switch (type_) {
		case KeyValueString:
		case KeyValueTuple:
			cast<key_string>()->~key_string();
			break;
		case KeyValueComposite:
			cast<PayloadValue>()->~PayloadValue();
			break;
		default:
			break;
	}
	hold_ = false;
}

void Variant::copy(const Variant &other) {
	assert(hold_);
	switch (type_) {
		case KeyValueString:
		case KeyValueTuple:
			new (cast<void>()) key_string(*other.cast<key_string>());
			break;
		case KeyValueComposite:
			new (cast<void>()) PayloadValue(*other.cast<PayloadValue>());
			break;
		default:
			value_uint64 = other.value_uint64;
			break;
	}
}

Variant &Variant::EnsureHold() {
	if (hold_) return *this;

	switch (type_) {
		case KeyValueString:
		case KeyValueTuple:
			*this = Variant(operator key_string());
			break;
		case KeyValueComposite:
			*this = Variant(operator const PayloadValue &());
			break;
		default:
			break;
	}
	return *this;
}

template <>
string Variant::As<string>() const {
	switch (type_) {
		case KeyValueInt:
			return std::to_string(value_int);
		case KeyValueBool:
			return value_bool ? "true" : "false";
		case KeyValueInt64:
			return std::to_string(value_int64);
		case KeyValueDouble:
			return std::to_string(value_double);
		case KeyValueString:
			if (operator p_string().type() == p_string::tagCxxstr || operator p_string().type() == p_string::tagKeyString) {
				return *operator p_string().getCxxstr();
			}
			return operator p_string().toString();
		case KeyValueNull:
			return "null";
		case KeyValueComposite:
			return string();
		case KeyValueTuple: {
			auto va = getCompositeValues();
			WrSerializer wrser;
			va.Dump(wrser);
			return wrser.Slice().ToString();
		}
		default:
			abort();
	}
}

template <>
int Variant::As<int>() const {
	try {
		switch (type_) {
			case KeyValueBool:
				return value_bool;
			case KeyValueInt:
				return value_int;
			case KeyValueInt64:
				return value_int64;
			case KeyValueDouble:
				return int(value_double);
			case KeyValueString: {
				size_t idx = 0;
				auto res = std::stoi(operator p_string().data(), &idx);
				if (idx != operator p_string().length()) {
					throw std::exception();
				}
				return res;
			}
			case KeyValueComposite:
			case KeyValueTuple:
				return 0;
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", operator p_string().data());
	}
}

template <>
bool Variant::As<bool>() const {
	try {
		switch (type_) {
			case KeyValueBool:
				return value_bool;
			case KeyValueInt:
				return value_int;
			case KeyValueInt64:
				return value_int64;
			case KeyValueDouble:
				return bool(value_double);
			case KeyValueString:
				return string_view(operator p_string()) == "true";
			case KeyValueComposite:
			case KeyValueTuple:
				return 0;
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to bool\n", operator p_string().data());
	}
}

template <>
int64_t Variant::As<int64_t>() const {
	try {
		switch (type_) {
			case KeyValueBool:
				return value_bool;
			case KeyValueInt:
				return value_int;
			case KeyValueInt64:
				return value_int64;
			case KeyValueDouble:
				return int64_t(value_double);
			case KeyValueString: {
				size_t idx = 0;
				auto res = std::stoull(operator p_string().data(), &idx);
				if (idx != operator p_string().length()) {
					throw std::exception();
				}
				return res;
			}
			case KeyValueComposite:
			case KeyValueTuple:
			case KeyValueNull:
				return 0;
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", operator p_string().data());
	}
}

template <>
double Variant::As<double>() const {
	try {
		switch (type_) {
			case KeyValueBool:
				return double(value_bool);
			case KeyValueInt:
				return double(value_int);
			case KeyValueInt64:
				return double(value_int64);
			case KeyValueDouble:
				return value_double;
			case KeyValueString:
				return std::stod(operator p_string().data());
			case KeyValueComposite:
			case KeyValueTuple:
				return 0.0;
			default:
				abort();
		}
	} catch (...) {
		throw Error(errParams, "Can't convert %s to number\n", operator p_string().data());
	}
}

int Variant::Compare(const Variant &other, const CollateOpts &collateOpts) const {
	assert(other.Type() == type_);
	switch (Type()) {
		case KeyValueInt:
			return (value_int == other.value_int) ? 0 : (value_int > other.value_int) ? 1 : -1;
		case KeyValueBool:
			return (value_bool == other.value_bool) ? 0 : (value_bool > other.value_bool) ? 1 : -1;
		case KeyValueInt64:
			return (value_int64 == other.value_int64) ? 0 : (value_int64 > other.value_int64) ? 1 : -1;
		case KeyValueDouble:
			return (value_double == other.value_double) ? 0 : (value_double > other.value_double) ? 1 : -1;
		case KeyValueTuple:
			return getCompositeValues() == other.getCompositeValues() ? 0 : 1;
		case KeyValueString:
			return collateCompare(operator p_string(), other.operator p_string(), collateOpts);
		default:
			abort();
	}
}

size_t Variant::Hash() const {
	switch (Type()) {
		case KeyValueInt:
			return hash<int>()(value_int);
		case KeyValueBool:
			return hash<bool>()(value_bool);
		case KeyValueInt64:
			return hash<int64_t>()(value_int64);
		case KeyValueDouble:
			return hash<double>()(value_double);
		case KeyValueString:
			return hash<p_string>()(operator p_string());
		default:
			abort();
	}
}

void Variant::EnsureUTF8() const {
	if (type_ == KeyValueString) {
		if (!utf8::is_valid(operator p_string().data(), operator p_string().data() + operator p_string().size())) {
			throw Error(errParams, "Invalid UTF8 string passed to index with CollateUTF8 mode");
		}
	}
}

Variant &Variant::convert(KeyValueType type, const PayloadType *payloadType, const FieldsSet *fields) {
	if (type == type_ || type == KeyValueNull || type_ == KeyValueNull) return *this;
	switch (type) {
		case KeyValueInt:
			*this = Variant(As<int>());
			break;
		case KeyValueBool:
			*this = Variant(As<bool>());
			break;
		case KeyValueInt64:
			*this = Variant(As<int64_t>());
			break;
		case KeyValueDouble:
			*this = Variant(As<double>());
			break;
		case KeyValueString:
			*this = Variant(As<string>());
			break;
		case KeyValueComposite:
			if (type_ == KeyValueTuple) {
				assert(payloadType && fields);
				convertToComposite(payloadType, fields);
				break;
			}
			// fall through
		default:
			throw Error(errParams, "Can't convert Variant from type '%s' to to type '%s'", TypeName(type_), TypeName(type));
	}
	type_ = type;
	return *this;
}

void Variant::convertToComposite(const PayloadType *payloadType, const FieldsSet *fields) {
	assert(type_ == KeyValueTuple && hold_);
	key_string val = *cast<key_string>();

	if (hold_) free();
	// Alloc usual payloadvalue + extra memory for hold string

	auto &pv = *new (cast<void>()) PayloadValue(payloadType->TotalSize() + val->size());
	hold_ = true;
	type_ = KeyValueComposite;

	// Copy serializer buffer with strings to extra payloadvalue memory
	char *data = reinterpret_cast<char *>(pv.Ptr() + payloadType->TotalSize());
	memcpy(data, val->data(), val->size());

	Serializer ser(string_view(data, val->size()));

	size_t count = ser.GetVarUint();
	if (count != fields->size()) {
		throw Error(errLogic, "Invalid count of arguments for composite index, expected %d, got %d", int(fields->size()), int(count));
	}

	Payload pl(*payloadType, pv);

	for (auto &field : *fields) {
		if (field != IndexValueType::SetByJsonPath) {
			pl.Set(field, {ser.GetVariant()});
		} else {
			// TODO: will have to implement SetByJsonPath in PayloadIFace
			// or this "mixed" composite queries (by ordinary indexes + indexes
			// from cjson) won't work properly.
			throw Error(errConflict, "SetByJsonPath is not implemented yet");
		}
	}
}

VariantArray Variant::getCompositeValues() const {
	VariantArray res;

	assert(type_ == KeyValueTuple);

	Serializer ser(**cast<key_string>());
	size_t count = ser.GetVarUint();
	res.reserve(count);
	while (count--) {
		res.push_back(ser.GetVariant());
	}
	return res;
}

const char *Variant::TypeName(KeyValueType t) {
	switch (t) {
		case KeyValueInt:
			return "int";
		case KeyValueBool:
			return "bool";
		case KeyValueInt64:
			return "int64";
		case KeyValueDouble:
			return "double";
		case KeyValueString:
			return "string";
		case KeyValueComposite:
			return "<composite>";
		case KeyValueTuple:
			return "<tuple>";
		case KeyValueNull:
			return "<null>";
		case KeyValueUndefined:
			return "<unknown>";
	}
	return "<invalid type>";
}

Variant::operator key_string() const {
	assertKeyType(type_, KeyValueString);
	return hold_ ? *cast<key_string>() : make_key_string(cast<p_string>()->data(), cast<p_string>()->size());
}
Variant::operator p_string() const {
	assertKeyType(type_, KeyValueString);
	return hold_ ? p_string(*cast<key_string>()) : *cast<p_string>();
}

Variant::operator string_view() const {
	assertKeyType(type_, KeyValueString);
	return hold_ ? string_view(**cast<key_string>()) : *cast<p_string>();
}
Variant::operator const PayloadValue &() const {
	assertKeyType(type_, KeyValueComposite);
	assert(hold_);
	return *cast<PayloadValue>();
}

static bool isPrintable(p_string str) {
	if (str.length() > 256) {
		return false;
	}

	for (int i = 0; i < int(str.length()); i++) {
		if (unsigned(str.data()[i]) < 0x20) {
			return false;
		}
	}
	return true;
}

void VariantArray::Dump(WrSerializer &wrser) const {
	wrser << '{';

	for (auto &arg : *this) {
		if (&arg != &at(0)) {
			wrser << ", ";
		}
		switch (arg.Type()) {
			case KeyValueString: {
				p_string str(arg);
				if (isPrintable(str)) {
					wrser << '\'' << string_view(str) << '\'';
				} else {
					wrser << "slice{len:" << str.length() << "}";
				}
				break;
			}
			case KeyValueInt:
				wrser << int(arg);
				break;
			case KeyValueBool:
				wrser << bool(arg);
				break;
			case KeyValueInt64:
				wrser << int64_t(arg);
				break;
			default:
				wrser << "??";
				break;
		}
	}
	wrser << '}';
}

}  // namespace reindexer
