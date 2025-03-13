#include "jsontools.h"
#include "tools/serializer.h"

namespace reindexer {

void jsonValueToString(gason::JsonValue o, WrSerializer& ser, int shift, int indent, bool escapeStrings) {
	using namespace std::string_view_literals;
	bool enableEol = (shift != 0) || (indent != 0);
	switch (o.getTag()) {
		case gason::JsonTag::NUMBER:
			ser << o.toNumber();
			break;
		case gason::JsonTag::DOUBLE:
			ser << o.toDouble();
			break;
		case gason::JsonTag::STRING:
			if (escapeStrings) {
				ser.PrintJsonString(o.toString());
			} else {
				ser << o.toString();
			}
			break;
		case gason::JsonTag::ARRAY:
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
		case gason::JsonTag::OBJECT:
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
		case gason::JsonTag::JTRUE:
			ser << true;
			break;
		case gason::JsonTag::JFALSE:
			ser << false;
			break;
		case gason::JsonTag::JSON_NULL:
			ser << "null"sv;
			break;
		case gason::JsonTag::EMPTY:
			break;	// do nothing
		default:
			throw Error(errLogic, "Unexpected json tag: {}", int(o.getTag()));
	}
}

void prettyPrintJSON(std::span<char> json, WrSerializer& ser, int shift) {
	gason::JsonParser parser;
	jsonValueToString(parser.Parse(json).value, ser, shift, 0);
}

void prettyPrintJSON(std::string_view json, WrSerializer& ser, int shift) {
	gason::JsonParser parser;
	jsonValueToString(parser.Parse(json).value, ser, shift, 0);
}

std::string stringifyJson(const gason::JsonNode& elem, bool escapeStrings) {
	WrSerializer ser;
	jsonValueToString(elem.value, ser, 0, 0, escapeStrings);

	return std::string(ser.Slice());
}

}  // namespace reindexer
