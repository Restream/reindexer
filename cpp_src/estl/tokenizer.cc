#include "tokenizer.h"
#include <stdlib.h>
#include "tools/stringstools.h"

namespace reindexer {

tokenizer::tokenizer(const string_view &query) : q(query), cur(query.begin()) {}

bool tokenizer::end() const { return cur == q.end(); }

void tokenizer::skip_space() {
	for (;;) {
		while (cur != q.end() && (*cur == ' ' || *cur == '\t' || *cur == '\n')) cur++;
		if (cur != q.end() && *cur == '-' && cur + 1 != q.end() && *(cur + 1) == '-') {
			cur += 2;
			while (cur != q.end() && *cur != '\n') cur++;
		} else
			return;
	}
}

token tokenizer::next_token(bool to_lower) {
	skip_space();

	if (cur == q.end()) return token(TokenEnd);

	token res(TokenSymbol);

	if (isalpha(*cur) || *cur == '_' || *cur == '#') {
		res.type = TokenName;
		do {
			res.text_.push_back(to_lower ? tolower(*cur++) : *cur++);
		} while (cur != q.end() && (isalpha(*cur) || isdigit(*cur) || *cur == '_' || *cur == '#'));
	} else if (isdigit(*cur) || *cur == '-' || *cur == '+') {
		res.type = TokenNumber;
		do {
			res.text_.push_back(*cur++);
		} while (cur != q.end() && (isdigit(*cur) || *cur == '.'));
	} else if (cur != q.end() && (*cur == '>' || *cur == '<' || *cur == '=')) {
		res.type = TokenOp;
		do {
			res.text_.push_back(*cur++);
		} while (cur != q.end() && (*cur == '=' || *cur == '>' || *cur == '<') && res.text_.size() < 2);
	} else if (*cur == '"' || *cur == '\'' || *cur == '`') {
		res.type = TokenString;
		char quote_chr = *cur++;
		while (cur != q.end()) {
			if (*cur == quote_chr) {
				++cur;
				break;
			}
			if (*cur == '\\' && ++cur == q.end()) break;
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
	for (auto pos = q.begin(); pos != cur; pos++) {
		if (*pos == '\n') {
			line++;
			col = 0;
		} else
			col++;
	}
	return "line: " + std::to_string(line) + " column: " + std::to_string(col) + " " + std::to_string(q.size());
}

token tokenizer::peek_token(bool to_lower) {
	auto save_cur = cur;
	auto res = next_token(to_lower);
	cur = save_cur;
	return res;
}

}  // namespace reindexer
