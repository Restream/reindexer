
#include "typos.h"
#include "tools/stringstools.h"

namespace reindexer {

static void mktyposInternal(typos_context *ctx, const wstring &word, int level, int maxTyposLen,
							std::function<void(string_view, int)> callback) {
	if (!level || word.length() < 3 || int(word.length()) > maxTyposLen) {
		return;
	}
	ctx->utf16Typo.assign(word.data() + 1, word.size() - 1);

	for (size_t i = 0;; ++i) {
		utf16_to_utf8(ctx->utf16Typo, ctx->typo);
		callback(ctx->typo, level);
		if (i >= ctx->utf16Typo.length()) {
			break;
		}
		ctx->utf16Typo[i] = word[i];
		if (level > 1) {
			mktyposInternal(ctx + 1, ctx->utf16Typo, level - 1, maxTyposLen, callback);
		}
	}
}

void mktypos(typos_context *ctx, const wstring &word, int level, int maxTyposLen, std::function<void(string_view, int)> callback) {
	utf16_to_utf8(word, ctx->typo);
	callback(ctx->typo, level);
	mktyposInternal(ctx, word, level, maxTyposLen, callback);
}

void mktypos(typos_context *ctx, string_view word, int level, int maxTyposLen, std::function<void(string_view, int)> callback) {
	ctx->typo.assign(word.begin(), word.end());
	utf8_to_utf16(ctx->typo, ctx->utf16Word);
	callback(ctx->typo, level);
	mktyposInternal(ctx, ctx->utf16Word, level, maxTyposLen, callback);
}

}  // namespace reindexer
