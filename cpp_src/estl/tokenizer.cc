#include "tokenizer.h"
#include <stdlib.h>

namespace reindexer {

static inline bool isalpha(char c) { return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'); }
static inline bool isdigit(char c) { return (c >= '0' && c <= '9'); }
static inline char tolower(char c) { return (c >= 'A' && c <= 'Z') ? c + 'a' - 'A' : c; }

tokenizer::tokenizer(const string &query) : cur(query.c_str()), beg(query.c_str()) {}

bool tokenizer::end() const { return !*cur; }

void tokenizer::skip_space() {
	for (;;) {
		while (*cur == ' ' || *cur == '\t' || *cur == '\n') cur++;
		if (*cur == '-' && *(cur + 1) == '-') {
			cur += 2;
			while (*cur && *cur != '\n') cur++;
		} else
			return;
	}
}

token tokenizer::next_token(bool to_lower) {
	skip_space();

	if (!*cur) return token(TokenEnd);

	token res(TokenSymbol);

	if (isalpha(*cur) || *cur == '_' || *cur == '#') {
		res.type = TokenName;
		do {
			res.text_.push_back(to_lower ? tolower(*cur++) : *cur++);
		} while (isalpha(*cur) || isdigit(*cur) || *cur == '_' || *cur == '#');
	} else if (isdigit(*cur) || *cur == '-' || *cur == '+') {
		res.type = TokenNumber;
		do {
			res.text_.push_back(*cur++);
		} while (isdigit(*cur));
	} else if (*cur == '>' || *cur == '<' || *cur == '=') {
		res.type = TokenOp;
		do {
			res.text_.push_back(*cur++);
		} while ((*cur == '=' || *cur == '>' || *cur == '<') && res.text_.size() < 2);
	} else if (*cur == '"' || *cur == '\'' || *cur == '`') {
		res.type = TokenString;
		char quote_chr = *cur++;
		while (*cur) {
			if (*cur == quote_chr) {
				++cur;
				break;
			}
			if (*cur == '\\' && !*++cur) break;
			res.text_.push_back(*cur++);
		};
	} else
		res.text_.push_back(*cur++);

	// null terminate it
	res.text_.reserve(res.text_.size() + 1);
	*(res.text_.begin() + res.text_.size()) = 0;
	return res;
}

string tokenizer::where() const {
	int line = 1;
	int col = 0;
	for (const char *pos = beg; pos != cur; pos++) {
		if (*pos == '\n') {
			line++;
			col = 0;
		} else
			col++;
	}
	return "line: " + std::to_string(line) + " column: " + std::to_string(col);
}

token tokenizer::peek_token(bool to_lower) {
	auto save_cur = cur;
	auto res = next_token(to_lower);
	cur = save_cur;
	return res;
}

}  // namespace reindexer
