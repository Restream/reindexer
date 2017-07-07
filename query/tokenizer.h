#pragma once

#include <string>

using namespace std;

namespace reindexer {

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
	tokenizer(const string &_query);
	token next_token();
	token peek_token();
	void skip_space();
	bool end() const;

protected:
	string query;
	string::iterator cur;
};

}  // namespace reindexer