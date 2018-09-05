
#include "iotools.h"

namespace reindexer_tool {

static std::vector<char> charsForEscaping = {'\n', '\t', ',', '\0', '\\'};

string escapeName(const string_view& str) {
	string dst = "";

	dst.reserve(str.length());

	for (auto it = str.begin(); it != str.end(); it++) {
		auto findIt = std::find(charsForEscaping.begin(), charsForEscaping.end(), *it);

		if (findIt != charsForEscaping.end()) {
			dst.push_back('\\');
		}

		dst.push_back(*it);
	}

	return dst;
}

string unescapeName(const string_view& str) {
	string dst = "";

	dst.reserve(str.length());

	for (auto it = str.begin(); it != str.end(); it++) {
		if (*it == '\\') {
			it++;

			auto findIt = std::find(charsForEscaping.begin(), charsForEscaping.end(), *it);
			if (findIt == charsForEscaping.end()) {
				dst.push_back('\\');
			}
		}

		dst.push_back(*it);
	}

	return dst;
}
const int kJsonShiftWidth = 4;

void prettyPrint(JsonValue o, int indent, WrSerializer& ser) {
	switch (o.getTag()) {
		case JSON_NUMBER:
			ser.Printf("%g", o.toNumber());
			break;
		case JSON_STRING:
			ser.PrintJsonString(o.toString());
			break;
		case JSON_ARRAY:
			if (!o.toNode()) {
				ser.Printf("[]");
				break;
			}
			ser.Printf("[\n");
			for (auto i : o) {
				ser.Printf("%*s", indent + kJsonShiftWidth, "");
				prettyPrint(i->value, indent + kJsonShiftWidth, ser);
				ser.Printf(i->next ? ",\n" : "\n");
			}
			ser.Printf("%*s]", indent, "");
			break;
		case JSON_OBJECT:
			if (!o.toNode()) {
				ser.Printf("{}");
				break;
			}
			ser.Printf("{\n");
			for (auto i : o) {
				ser.Printf("%*s", indent + kJsonShiftWidth, "");
				ser.PrintJsonString(i->key);
				ser.Printf(": ");
				prettyPrint(i->value, indent + kJsonShiftWidth, ser);
				ser.Printf(i->next ? ",\n" : "\n");
			}
			ser.Printf("%*s}", indent, "");
			break;
		case JSON_TRUE:
			ser.Printf("true");
			break;
		case JSON_FALSE:
			ser.Printf("false");
			break;
		case JSON_NULL:
			ser.Printf("null");
			break;
	}
}

void prettyPrintJSON(string json, WrSerializer& ser) {
	char* endptr;
	JsonValue value;
	JsonAllocator allocator;
	jsonParse(const_cast<char*>(json.c_str()), &endptr, &value, allocator);
	prettyPrint(value, 0, ser);
}

}  // namespace reindexer_tool
