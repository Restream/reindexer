#include "tokenizer.h"
#include <stdlib.h>
#include "tools/stringstools.h"

namespace reindexer {

tokenizer::tokenizer(string_view query) : q_(query), cur_(query.begin()) {}

bool tokenizer::end() const { return cur_ == q_.end(); }

void tokenizer::skip_space() {
	for (;;) {
		while (cur_ != q_.end() && (*cur_ == ' ' || *cur_ == '\t' || *cur_ == '\n')) {
			cur_++;
			pos_++;
		}
		if (cur_ != q_.end() && *cur_ == '-' && cur_ + 1 != q_.end() && *(cur_ + 1) == '-') {
			cur_ += 2;
			pos_ += 2;
			while (cur_ != q_.end() && *cur_ != '\n') {
				cur_++;
				pos_++;
			}
		} else
			return;
	}
}

token tokenizer::next_token(bool to_lower, bool treatSignAsToken) {
	skip_space();

	if (cur_ == q_.end()) return token(TokenEnd);

	token res(TokenSymbol);

	if (isalpha(*cur_) || *cur_ == '_' || *cur_ == '#') {
		res.type = TokenName;
		do {
			if (*cur_ == '*' && *(cur_ - 1) != '[') break;
			res.text_.push_back(to_lower ? tolower(*cur_++) : *cur_++);
			++pos_;
		} while (cur_ != q_.end() && (isalpha(*cur_) || isdigit(*cur_) || *cur_ == '_' || *cur_ == '#' || *cur_ == '.' || *cur_ == '*' ||
									  *cur_ == '[' || *cur_ == ']'));
	} else if (isdigit(*cur_) || (!treatSignAsToken && (*cur_ == '-' || *cur_ == '+'))) {
		res.type = TokenNumber;
		do {
			res.text_.push_back(*cur_++);
			++pos_;
		} while (cur_ != q_.end() && (isdigit(*cur_) || *cur_ == '.'));
	} else if (treatSignAsToken && (*cur_ == '-' || *cur_ == '+')) {
		res.type = TokenSign;
		res.text_.push_back(*cur_++);
		++pos_;
	} else if (cur_ != q_.end() && (*cur_ == '>' || *cur_ == '<' || *cur_ == '=')) {
		res.type = TokenOp;
		do {
			res.text_.push_back(*cur_++);
			++pos_;
		} while (cur_ != q_.end() && (*cur_ == '=' || *cur_ == '>' || *cur_ == '<') && res.text_.size() < 2);
	} else if (*cur_ == '"' || *cur_ == '\'' || *cur_ == '`') {
		res.type = TokenString;
		char quote_chr = *cur_++;
		++pos_;
		while (cur_ != q_.end()) {
			if (*cur_ == quote_chr) {
				++cur_;
				++pos_;
				break;
			}
			if (*cur_ == '\\') {
				++pos_;
				if (++cur_ == q_.end()) break;
			}
			res.text_.push_back(*cur_++);
			++pos_;
		};
	} else {
		res.text_.push_back(*cur_++);
		++pos_;
	}

	// null terminate it
	res.text_.reserve(res.text_.size() + 1);
	*(res.text_.begin() + res.text_.size()) = 0;
	skip_space();
	return res;
}

string tokenizer::where() const {
	int line = 1;
	int col = 0;
	for (auto pos = q_.begin(); pos != cur_; pos++) {
		if (*pos == '\n') {
			line++;
			col = 0;
		} else
			col++;
	}
	return "line: " + std::to_string(line) + " column: " + std::to_string(col) + " " + std::to_string(q_.size());
}

token tokenizer::peek_token(bool to_lower, bool treatSignAsToken) {
	auto save_cur = cur_;
	auto save_pos = pos_;
	auto res = next_token(to_lower, treatSignAsToken);
	cur_ = save_cur;
	pos_ = save_pos;
	return res;
}

void tokenizer::setPos(size_t pos) {
	int delta = pos - pos_;
	pos_ += delta;
	cur_ += delta;
}
size_t tokenizer::getPos() const { return pos_; }

size_t tokenizer::length() const { return q_.length(); }
const char *tokenizer::begin() const { return q_.begin(); }

}  // namespace reindexer
