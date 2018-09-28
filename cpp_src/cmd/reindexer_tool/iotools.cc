
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
}  // namespace reindexer_tool
