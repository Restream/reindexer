#include "tokenizer.h"
#include "double-conversion/double-conversion.h"
#include "tools/stringstools.h"

namespace reindexer {

void tokenizer::skip_space() noexcept {
	for (;;) {
		while (cur_ != q_.end() && std::isspace(*cur_)) {
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
		} else {
			return;
		}
	}
}

token tokenizer::next_token(flags flgs) {
	skip_space();

	if (cur_ == q_.end()) {
		return token(TokenEnd);
	}

	token res(TokenSymbol);

	if (isalpha(*cur_) || *cur_ == '_' || *cur_ == '#' || *cur_ == '@') {
		res.type = TokenName;
		int openBrackets{0};
		do {
			if (*cur_ == '*' && *(cur_ - 1) != '[') {
				break;
			}
			res.text_.push_back(flgs.has_to_lower() ? tolower(*cur_++) : *cur_++);
			++pos_;
		} while (cur_ != q_.end() && (isalpha(*cur_) || isdigit(*cur_) || *cur_ == '_' || *cur_ == '#' || *cur_ == '@' || *cur_ == '.' ||
									  *cur_ == '*' || (*cur_ == '[' && (++openBrackets, true)) || (*cur_ == ']' && (--openBrackets >= 0))));
	} else if (*cur_ == '"') {
		res.type = TokenName;
		const size_t startPos = ++pos_;
		if (flgs.has_in_order_by()) {
			res.text_.push_back('"');
		}
		while (++cur_ != q_.end() && *cur_ != '"') {
			if (pos_ == startPos) {
				if (*cur_ != '#' && *cur_ != '_' && !isalpha(*cur_) && !isdigit(*cur_) && *cur_ != '@') {
					throw Error{errParseSQL, "Identifier should starts with alpha, digit, '_', '#' or '@', but found '{}'; {}", *cur_,
								where()};
				}
			} else if (*cur_ != '+' && *cur_ != '.' && *cur_ != '_' && *cur_ != '#' && *cur_ != '[' && *cur_ != ']' && *cur_ != '*' &&
					   !isalpha(*cur_) && !isdigit(*cur_) && *cur_ != '@') {
				throw Error{errParseSQL, "Identifier should not contain '{}'; {}", *cur_, where()};
			}
			res.text_.push_back(flgs.has_to_lower() ? tolower(*cur_) : *cur_);
			++pos_;
		}
		if (flgs.has_in_order_by()) {
			res.text_.push_back('"');
		}
		if (cur_ == q_.end()) {
			throw Error{errParseSQL, "Not found close '\"'; {}", where()};
		}
		++cur_;
		++pos_;
	} else if (isdigit(*cur_) || (!flgs.has_treat_sign_as_token() && (*cur_ == '-' || *cur_ == '+'))) {
		res.type = TokenNumber;
		do {
			res.text_.push_back(*cur_++);
			++pos_;
		} while (cur_ != q_.end() && (isdigit(*cur_) || *cur_ == '.' || *cur_ == 'e' || (*(cur_ - 1) == 'e' && issign(*cur_))));
	} else if (flgs.has_treat_sign_as_token() && (*cur_ == '-' || *cur_ == '+')) {
		res.type = TokenSign;
		res.text_.push_back(*cur_++);
		++pos_;
	} else if (cur_ != q_.end() && (*cur_ == '>' || *cur_ == '<' || *cur_ == '=')) {
		res.type = TokenOp;
		do {
			res.text_.push_back(*cur_++);
			++pos_;
		} while (cur_ != q_.end() && (*cur_ == '=' || *cur_ == '>' || *cur_ == '<') && res.text_.size() < 2);
	} else if (*cur_ == '\'' || *cur_ == '`') {
		res.type = TokenString;
		char quote_chr = *cur_++;
		++pos_;
		while (cur_ != q_.end()) {
			if (*cur_ == quote_chr) {
				++cur_;
				++pos_;
				break;
			}
			auto c = *cur_;
			if (c == '\\') {
				++pos_;
				if (++cur_ == q_.end()) {
					break;
				}
				c = *cur_;
				switch (c) {
					case 'n':
						c = '\n';
						break;
					case 'r':
						c = '\r';
						break;
					case 't':
						c = '\t';
						break;
					case 'b':
						c = '\b';
						break;
					case 'f':
						c = '\f';
						break;
					default:
						break;
				}
			}
			res.text_.push_back(c);
			++pos_;
			++cur_;
		}
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

size_t tokenizer::getPrevPos() const noexcept {
	assertrx_throw(pos_ > 0);

	// undo skip space
	auto pos = pos_ - 1;
	auto cur = cur_ - 1;
	for (;;) {
		while (cur != q_.begin() && std::isspace(*cur)) {
			--pos;
			--cur;
		}
		if (cur != q_.begin() && *cur == '-' && cur - 1 != q_.begin() && *(cur + 1) == '-') {
			cur -= 2;
			pos -= 2;
			while (cur != q_.begin() && *cur != '\n') {
				--cur;
				--pos;
			}
		} else {
			return pos;
		}
	}
}

std::string tokenizer::where() const {
	int line = 1;
	int col = 0;
	for (auto pos = q_.begin(); pos != cur_; pos++) {
		if (*pos == '\n') {
			line++;
			col = 0;
		} else {
			col++;
		}
	}
	return std::string()
		.append("line: ")
		.append(std::to_string(line))
		.append(" column: ")
		.append(std::to_string(col))
		.append(" ")
		.append(std::to_string(q_.size()));
}

Variant getVariantFromToken(const token& tok) {
	const std::string_view str = tok.text();
	if (tok.type != TokenNumber || str.empty()) {
		return Variant(make_key_string(str.data(), str.length()));
	}

	if (!isdigit(str[0]) && (str.size() == 1 || !issign(str[0]))) {
		return Variant(make_key_string(str.data(), str.length()));
	}

	bool isFloat = false;
	// INT64_MAX(9'223'372'036'854'775'807) contains 19 digits + 1 for possible sign
	const size_t maxSignsInInt = 19 + (isdigit(str[0]) ? 0 : 1);
	bool nullDecimalPart = true;

	size_t decPointPos = str.size();
	size_t ePos = str.size();
	for (unsigned i = 1; i < str.size(); i++) {
		if (str[i] == '.') {
			if (isFloat || ePos < str.size()) {
				// second or incorrect point - not a number
				return Variant(make_key_string(str.data(), str.length()));
			}

			decPointPos = i;

			isFloat = true;
			continue;
		}

		if (str[i] == 'e') {
			if (ePos < str.size()) {
				// second e not a number
				return Variant(make_key_string(str.data(), str.length()));
			}

			ePos = i;
			continue;
		}

		if (i == ePos + 1 && issign(str[i])) {
			continue;
		}

		if (!isdigit(str[i])) {
			return Variant(make_key_string(str.data(), str.length()));
		}

		if (isFloat) {
			nullDecimalPart = nullDecimalPart && str[i] == '0';
		}
	}

	if (ePos + 1 == str.size() || (ePos + 2 == str.size() && !isdigit(str[ePos + 1]))) {
		return Variant(make_key_string(str.data(), str.length()));
	}

	if (ePos == 1 && !isdigit((str[0]))) {
		return Variant(make_key_string(str.data(), str.length()));
	}

	isFloat = !nullDecimalPart || (isFloat && decPointPos > maxSignsInInt) || ePos < str.size();

	if (!isFloat) {
		auto intPart = str.substr(0, decPointPos);
		return intPart.size() <= maxSignsInInt ? Variant(stoll(intPart)) : Variant(make_key_string(str.data(), str.length()));
	}

	using double_conversion::StringToDoubleConverter;
	static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
	int countOfCharsParsedAsDouble = 0;
	return Variant(converter.StringToDouble(str.data(), str.size(), &countOfCharsParsedAsDouble));
}

}  // namespace reindexer
