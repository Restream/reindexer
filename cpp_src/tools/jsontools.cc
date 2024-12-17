#include "jsontools.h"
#include <string.h>
#include <cmath>
#include "tools/serializer.h"

namespace reindexer {

void jsonValueToString(gason::JsonValue o, WrSerializer& ser, int shift, int indent, bool escapeStrings) {
	using namespace std::string_view_literals;
	bool enableEol = (shift != 0) || (indent != 0);
	switch (o.getTag()) {
		case gason::JSON_NUMBER:
			ser << int64_t(o.toNumber());
			break;
		case gason::JSON_DOUBLE:
			ser << o.toDouble();
			break;
		case gason::JSON_STRING:
			if (escapeStrings) {
				ser.PrintJsonString(o.toString());
			} else {
				ser << o.toString();
			}
			break;
		case gason::JSON_ARRAY:
			if (!o.toNode()) {
				ser << "[]";
				break;
			}
			ser << '[';
			if (enableEol) {
				ser << '\n';
			}

			for (const auto& i : o) {
				ser.Fill(' ', indent + shift);
				jsonValueToString(i.value, ser, shift, indent + shift);
				if (i.next) {
					ser << ',';
				}
				if (enableEol) {
					ser << '\n';
				}
			}
			ser.Fill(' ', indent);
			ser << ']';
			break;
		case gason::JSON_OBJECT:
			if (!o.toNode()) {
				ser << "{}";
				break;
			}
			ser << '{';
			if (enableEol) {
				ser << '\n';
			}

			for (const auto& i : o) {
				ser.Fill(' ', indent + shift);
				ser.PrintJsonString(i.key);
				ser << ": ";
				jsonValueToString(i.value, ser, shift, indent + shift);
				if (i.next) {
					ser << ',';
				}
				if (enableEol) {
					ser << '\n';
				}
			}
			ser.Fill(' ', indent);
			ser << '}';
			break;
		case gason::JSON_TRUE:
			ser << true;
			break;
		case gason::JSON_FALSE:
			ser << false;
			break;
		case gason::JSON_NULL:
			ser << "null"sv;
			break;
		case gason::JSON_EMPTY:
			break;	// do nothing
		default:
			throw Error(errLogic, "Unexpected json tag: %d", int(o.getTag()));
	}
}

void prettyPrintJSON(span<char> json, WrSerializer& ser, int shift) {
	gason::JsonParser parser;
	jsonValueToString(parser.Parse(json).value, ser, shift, 0);
}

std::string stringifyJson(const gason::JsonNode& elem, bool escapeStrings) {
	WrSerializer ser;
	jsonValueToString(elem.value, ser, 0, 0, escapeStrings);

	return std::string(ser.Slice());
}

}  // namespace reindexer
