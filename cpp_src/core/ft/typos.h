#pragma once

#include <functional>
#include <string>
#include <string_view>

namespace reindexer {

struct typos_context {
	using CallBack = std::function<void(std::string_view, int)>;
	std::wstring utf16Word, utf16Typo;
	std::string typo;
};
const int kMaxTyposInWord = 4;

void mktypos(typos_context *ctx, const std::wstring &word, int level, int maxTyposLen, const typos_context::CallBack &callback);
void mktypos(typos_context *ctx, std::string_view word, int level, int maxTyposLen, const typos_context::CallBack &callback);

}  // namespace reindexer
