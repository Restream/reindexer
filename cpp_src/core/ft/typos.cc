
#include "typos.h"
#include "tools/stringstools.h"

namespace reindexer {

template <unsigned level>
static void mktyposInternal(typos_context* ctx, const std::wstring& word, int maxTyposLen, const typos_context::CallBack& callback,
							typos_context::TyposVec& positions, const std::wstring& wordOriginal) {
	static_assert(level <= typos_context::TyposVec::capacity(), "Positions array must be able to store all the typos");
	if constexpr (level == 0) {
		return;
	}
	if (word.length() < 3 || int(word.length()) > maxTyposLen) {
		return;
	}
	ctx->utf16Typo.assign(word.data() + 1, word.size() - 1);

	for (unsigned i = 0;; ++i) {
		utf16_to_utf8(ctx->utf16Typo, ctx->typo);
		auto pos = i;
		for (auto p : positions) {
			if (unsigned(p) <= i) {
				++pos;
			}
		}
		positions.emplace_back(pos);
		callback(ctx->typo, level, positions, wordOriginal);
		positions.pop_back();
		if (i >= ctx->utf16Typo.length()) {
			break;
		}
		ctx->utf16Typo[i] = word[i];
		if constexpr (level > 1) {
			pos = i + 1;
			for (auto p : positions) {
				if (unsigned(p) <= i + 1) {
					--pos;
				}
			}
			positions.emplace_back(i + 1);
			mktyposInternal<level - 1>(ctx + 1, ctx->utf16Typo, maxTyposLen, callback, positions, wordOriginal);
			positions.pop_back();
		}
	}
}

void mktypos(typos_context* ctx, const std::wstring& word, int level, int maxTyposLen, const typos_context::CallBack& callback) {
	utf16_to_utf8(word, ctx->typo);
	typos_context::TyposVec positions;
	callback(ctx->typo, level, positions, word);
	switch (level) {
		case 0:
			mktyposInternal<0>(ctx, word, maxTyposLen, callback, positions, word);
			return;
		case 1:
			mktyposInternal<1>(ctx, word, maxTyposLen, callback, positions, word);
			return;
		case 2:
			mktyposInternal<2>(ctx, word, maxTyposLen, callback, positions, word);
			return;
		default:
			throw Error(errLogic, "Unexpected level value for mktypo(): {}", level);
	}
}

void mktypos(typos_context* ctx, std::string_view word, int level, int maxTyposLen, const typos_context::CallBack& callback) {
	ctx->typo.assign(word.begin(), word.end());
	utf8_to_utf16(ctx->typo, ctx->utf16Word);
	typos_context::TyposVec positions;
	callback(ctx->typo, level, positions, ctx->utf16Word);
	switch (level) {
		case 0:
			mktyposInternal<0>(ctx, ctx->utf16Word, maxTyposLen, callback, positions, ctx->utf16Word);
			return;
		case 1:
			mktyposInternal<1>(ctx, ctx->utf16Word, maxTyposLen, callback, positions, ctx->utf16Word);
			return;
		case 2:
			mktyposInternal<2>(ctx, ctx->utf16Word, maxTyposLen, callback, positions, ctx->utf16Word);
			return;
		default:
			throw Error(errLogic, "Unexpected level value for mktypo(): {}", level);
	}
}

}  // namespace reindexer
