

#pragma once

#include <string>

namespace reindexer {

using std::string;

enum token_type { TokenEnd, TokenName, TokenNumber, TokenString, TokenOp, TokenSymbol };

class token {
public:
	token() : type(TokenSymbol) {}
	token(token_type _type, const string &_text) : type(_type), text(_text) {}
	token_type type;
	string text;
};
class tokenizer {
public:
	tokenizer(const string &query);
	token next_token(bool to_lower = true);
	token peek_token(bool to_lower = true);
	void skip_space();
	bool end() const;

protected:
	const char *cur;
};

}  // namespace reindexer