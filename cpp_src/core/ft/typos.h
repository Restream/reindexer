#pragma once

#include <functional>
#include <string>
#include "estl/string_view.h"

namespace reindexer {

using std::string;
using std::wstring;

struct typos_context {
	wstring utf16Word, utf16Typo;
	string typo;
};
const int kMaxTyposInWord = 4;

void mktypos(typos_context *ctx, const wstring &word, int level, int maxTyposLen, std::function<void(string_view, int)> callback);
void mktypos(typos_context *ctx, string_view word, int level, int maxTyposLen, std::function<void(string_view, int)> callback);

}  // namespace reindexer
