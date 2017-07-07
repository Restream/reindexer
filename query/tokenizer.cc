#include "query/tokenizer.h"
#include <ctype.h>
#include <stdlib.h>

namespace reindexer {

tokenizer::tokenizer(const string &_query) : query(_query), cur(query.begin()) {}

bool tokenizer::end() const { return cur == query.end(); }

void tokenizer::skip_space() {
	while (cur != query.end() && (*cur == ' ' || *cur == '\t')) cur++;
}

token tokenizer::next_token() {
	skip_space();

	if (cur == query.end()) return token(TokenEnd, "");

	token res;
	res.type = TokenSymbol;

	if (isalpha(*cur)) {
		res.type = TokenName;
		do {
			res.text += std::tolower(*cur++);
		} while (cur != query.end() && (isalpha(*cur) || isdigit(*cur) || *cur == '_'));
	} else if (isdigit(*cur) || *cur == '-' || *cur == '+') {
		res.type = TokenNumber;
		do {
			res.text += *cur++;
		} while (cur != query.end() && isdigit(*cur));
	} else if (*cur == '>' || *cur == '<' || *cur == '=') {
		res.type = TokenOp;
		do {
			res.text += *cur++;
		} while (cur != query.end() && (*cur == '=' || *cur == '>' || *cur == '<') && res.text.length() < 2);
	} else if (*cur == '"' || *cur == '\'' || *cur == '`') {
		res.type = TokenString;
		char quote_chr = *cur++;
		while (cur != query.end()) {
			if (*cur == quote_chr) {
				++cur;
				break;
			}
			if (*cur == '\\' && ++cur == query.end()) break;
			res.text += *cur++;
		};
	} else
		res.text = *cur++;

	//	printf ("tok=%s\n",res.text.c_str());
	return res;
}

token tokenizer::peek_token() {
	string::iterator save_cur = cur;
	token res = next_token();
	cur = save_cur;
	return res;
}

}  // namespace reindexer