
#include "iotools.h"

namespace reindexer_tool {

string escapeName(const string_view& str) {
	string dst = "";

	dst.reserve(str.length());

	for (auto it = str.begin(); it != str.end(); it++) {
		if (*it < 0x20 || unsigned(*it) >= 0x80 || *it == '\\') {
			char tmpbuf[16];
			snprintf(tmpbuf, sizeof(tmpbuf), "\\%02X", unsigned(*it) & 0xFF);
			dst += tmpbuf;
		} else
			dst.push_back(*it);
	}

	return dst;
}

string unescapeName(const string_view& str) {
	string dst = "";

	dst.reserve(str.length());

	for (auto it = str.begin(); it != str.end(); it++) {
		if (*it == '\\' && ++it != str.end() && it + 1 != str.end()) {
			char tmpbuf[16], *endp;
			tmpbuf[0] = *it++;
			tmpbuf[1] = *it;
			tmpbuf[2] = 0;
			dst += char(strtol(tmpbuf, &endp, 16));
		} else
			dst.push_back(*it);
	}

	return dst;
}
}  // namespace reindexer_tool
