

#pragma once

#include <string>
#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"
#include "estl/string_view.h"

namespace reindexer {

using std::string;

enum token_type { TokenEnd, TokenName, TokenNumber, TokenString, TokenOp, TokenSymbol, TokenSign };

class token {
public:
	token() : type(TokenSymbol) {}
	token(token_type type) : type(type) {}
	token(const token &) = delete;
	token &operator=(const token &) = delete;
	token(token &&other) : type(other.type), text_(std::move(other.text_)) {
		text_.reserve(other.text_.size() + 1);
		*(text_.begin() + text_.size()) = 0;
	}
	token &operator=(token &&other) {
		if (&other != this) {
			type = other.type;
			text_ = std::move(other.text_);
			text_.reserve(other.text_.size() + 1);
			*(text_.begin() + text_.size()) = 0;
		}
		return *this;
	}

	string_view text() const { return string_view(text_.data(), text_.size()); }

	token_type type;
	h_vector<char, 20> text_;
};

class tokenizer {
public:
	tokenizer(string_view query);
	token next_token(bool to_lower = true, bool treatSignAsToken = false);
	token peek_token(bool to_lower = true, bool treatSignAsToken = false);
	void skip_space();
	bool end() const;
	size_t getPos() const;
	void setPos(size_t pos);
	string where() const;
	size_t length() const;
	const char *begin() const;

protected:
	string_view q_;
	const char *cur_;
	size_t pos_ = 0;
};

Variant token2kv(const token &currTok, tokenizer &parser, bool allowComposite);

}  // namespace reindexer
