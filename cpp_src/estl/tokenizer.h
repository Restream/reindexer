

#pragma once

#include <string>
#include "core/keyvalue/variant.h"
#include "estl/h_vector.h"

namespace reindexer {

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

	std::string_view text() const { return std::string_view(text_.data(), text_.size()); }

	token_type type;
	h_vector<char, 20> text_;
};

class tokenizer {
public:
	tokenizer(std::string_view query);
	token next_token(bool to_lower = true, bool treatSignAsToken = false, bool inOrderBy = false);
	token peek_token(bool to_lower = true, bool treatSignAsToken = false, bool inOrderBy = false);
	void skip_space();
	bool end() const;
	size_t getPos() const;
	void setPos(size_t pos);
	std::string where() const;
	size_t length() const;
	const char *begin() const;

protected:
	std::string_view q_;
	std::string_view::const_iterator cur_;
	size_t pos_ = 0;
};

Variant token2kv(const token &currTok, tokenizer &parser, bool allowComposite);

}  // namespace reindexer
