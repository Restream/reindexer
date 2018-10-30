#include <string.h>
#include <tools/errors.h>
#include <cmath>
#include <vector>
#include "gason/gason.h"
#include "stringstools.h"
#include "tools/serializer.h"

namespace reindexer {

void jsonValueToString(JsonValue o, WrSerializer &ser, int shift, int indent) {
	bool enableEol = shift != 0 || indent != 0;
	switch (o.getTag()) {
		case JSON_NUMBER: {
			double value = o.toNumber();
			double intpart;
			if (std::modf(value, &intpart) == 0.0) {
				ser << int64_t(value);
			} else {
				ser << value;
			}
			break;
		}
		case JSON_STRING:
			ser.PrintJsonString(o.toString());
			break;
		case JSON_ARRAY:
			if (!o.toNode()) {
				ser << "[]";
				break;
			}
			ser << '[';
			if (enableEol) ser << '\n';

			for (auto i : o) {
				ser.Printf("%*s", indent + shift, "");
				jsonValueToString(i->value, ser, shift, indent + shift);
				if (i->next) ser << ',';
				if (enableEol) ser << '\n';
			}
			ser.Printf("%*s]", indent, "");
			break;
		case JSON_OBJECT:
			if (!o.toNode()) {
				ser << "{}";
				break;
			}
			ser << '{';
			if (enableEol) ser << '\n';

			for (auto i : o) {
				ser.Printf("%*s", indent + shift, "");
				ser.PrintJsonString(i->key);
				ser << ": ";
				jsonValueToString(i->value, ser, shift, indent + shift);
				if (i->next) ser << ',';
				if (enableEol) ser << '\n';
			}
			ser.Printf("%*s}", indent, "");
			break;
		case JSON_TRUE:
			ser << "true";
			break;
		case JSON_FALSE:
			ser << "false";
			break;
		case JSON_NULL:
			ser << "null";
			break;
	}
}

void prettyPrintJSON(string json, WrSerializer &ser, int shift) {
	char *endptr;
	JsonValue value;
	JsonAllocator allocator;
	jsonParse(const_cast<char *>(json.c_str()), &endptr, &value, allocator);
	jsonValueToString(value, ser, shift, 0);
}

string stringifyJson(const JsonNode *elem) {
	WrSerializer ser;
	jsonValueToString(elem->value, ser, 0, 0);

	return ser.Slice().ToString();
}

void parseJsonField(const char *name, string &ref, const JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_STRING) {
		ref = elem->value.toString();
	} else
		throw Error(errParseJson, "Expected string setting '%s'", name);
}

void parseJsonField(const char *name, bool &ref, const JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_TRUE) {
		ref = true;
	} else if (elem->value.getTag() == JSON_FALSE) {
		ref = false;
	} else
		throw Error(errParseJson, "Expected value `true` of `false` for setting '%s'", name);
}

template <typename T>
void parseJsonField(const char *name, T &ref, const JsonNode *elem, double min, double max) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_NUMBER) {
		T v = elem->value.toNumber();
		if (v < min || v > max) {
			throw Error(errParseJson, "Value of setting '%s' is out of range [%g,%g]", name, min, max);
		}
		ref = v;
	} else
		throw Error(errParseJson, "Expected type number for setting '%s'", name);
}

template <typename T>
void parseJsonField(const char *name, T &ref, const JsonNode *elem) {
	if (strcmp(name, elem->key)) return;
	if (elem->value.getTag() == JSON_NUMBER) {
		ref = elem->value.toNumber();
	} else
		throw Error(errParseJson, "Expected type number for setting '%s'", name);
}

template void parseJsonField(const char *, int &, const JsonNode *, double, double);
template void parseJsonField(const char *, size_t &, const JsonNode *, double, double);
template void parseJsonField(const char *, double &, const JsonNode *, double, double);
template void parseJsonField(const char *, double &, const JsonNode *);
template void parseJsonField(const char *, int &, const JsonNode *);

}  // namespace reindexer
