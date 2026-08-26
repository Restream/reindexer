#include "tokenizer.h"
#include "core/formatters/tokenizer_range.h"
#include "double-conversion/double-conversion.h"
#include "estl/defines.h"
#include "tools/errors.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace {

struct [[nodiscard]] NumberLiteralInfo {
	bool valid = false;
	bool isFloat = false;
	size_t decPointPos = 0;
	size_t maxSignsInInt = 0;
};

bool IsExponentChar(char c) noexcept { return c == 'e' || c == 'E'; }

NumberLiteralInfo AnalyzeNumberLiteral(std::string_view str, bool allowLeadingSign) noexcept {
	NumberLiteralInfo info;
	if (str.empty()) {
		return info;
	}

	if (!isdigit(str[0]) && (str.size() == 1 || !issign(str[0]))) {
		return info;
	}

	if (!allowLeadingSign && !isdigit(str[0])) {
		return info;
	}

	bool isFloat = false;
	// INT64_MAX(9'223'372'036'854'775'807) contains 19 digits + 1 for possible sign
	const size_t maxSignsInInt = 19 + (isdigit(str[0]) ? 0 : 1);
	bool nullDecimalPart = true;
	bool hasMantissaDigit = isdigit(str[0]);

	size_t decPointPos = str.size();
	size_t ePos = str.size();
	for (unsigned i = 1; i < str.size(); i++) {
		if (str[i] == '.') {
			if (isFloat || ePos < str.size()) {
				return info;
			}

			decPointPos = i;

			isFloat = true;
			continue;
		}

		if (IsExponentChar(str[i])) {
			if (ePos < str.size()) {
				return info;
			}

			ePos = i;
			continue;
		}

		if (i == ePos + 1 && issign(str[i])) {
			continue;
		}

		if (!isdigit(str[i])) {
			return info;
		}

		if (ePos == str.size()) {
			hasMantissaDigit = true;
		}

		if (isFloat) {
			nullDecimalPart = nullDecimalPart && str[i] == '0';
		}
	}

	if (ePos + 1 == str.size() || (ePos + 2 == str.size() && !isdigit(str[ePos + 1]))) {
		return info;
	}

	if (ePos == 1 && !isdigit(str[0])) {
		return info;
	}

	if (!hasMantissaDigit) {
		return info;
	}

	info.valid = true;
	info.isFloat = !nullDecimalPart || (isFloat && decPointPos > maxSignsInInt) || ePos < str.size();
	info.decPointPos = decPointPos;
	info.maxSignsInInt = maxSignsInInt;
	return info;
}

RX_ALWAYS_INLINE void ConsumeNameChars(std::string_view::const_iterator& cur, size_t& pos, std::string_view::const_iterator end,
									   Token::StorageT& text, Tokenizer::Flags flgs, bool applyToLower) {
	int openBrackets{0};
	do {
		if (*cur == '*' && !text.empty() && text.back() != '[') {
			break;
		}
		text.push_back(applyToLower && flgs.HasToLower() ? tolower(*cur++) : *cur++);
		++pos;
	} while (cur != end && (isalpha(*cur) || isdigit(*cur) || *cur == '_' || *cur == '#' || *cur == '@' || *cur == '.' || *cur == '*' ||
							(*cur == '[' && (++openBrackets, true)) || (*cur == ']' && (--openBrackets >= 0))));
}

RX_ALWAYS_INLINE void ConsumeNumberSuffixChars(std::string_view::const_iterator& cur, size_t& pos, std::string_view::const_iterator end,
											   Token::StorageT& text) {
	while (cur != end &&
		   (isdigit(*cur) || *cur == '.' || IsExponentChar(*cur) || (!text.empty() && IsExponentChar(text.back()) && issign(*cur)))) {
		text.push_back(*cur++);
		++pos;
	}
}

RX_ALWAYS_INLINE bool IsNameContinuationChar(char c) noexcept {
	return isalpha(c) || isdigit(c) || c == '_' || c == '#' || c == '@' || c == '.' || c == '*' || c == '[' || c == ']';
}

void ClassifyDigitStartToken(h_vector<char, 20>& text, TokenType& type, std::string_view::const_iterator& cur, size_t& pos,
							 std::string_view::const_iterator end, Tokenizer::Flags flgs) {
	const auto startCur = cur;
	const size_t startPos = pos;
	ConsumeNumberSuffixChars(cur, pos, end, text);
	const NumberLiteralInfo info = AnalyzeNumberLiteral(std::string_view{text.data(), text.size()}, false);
	if (info.valid && (cur == end || !IsNameContinuationChar(*cur))) {
		type = TokenNumber;
		return;
	}

	cur = startCur;
	pos = startPos;
	text.clear();
	// Name consume may stop earlier than number suffix (e.g. "123*456" stops at '*').
	ConsumeNameChars(cur, pos, end, text, flgs, false);
	if (AnalyzeNumberLiteral(std::string_view{text.data(), text.size()}, false).valid) {
		type = TokenNumber;
	} else {
		type = TokenName;
		if (flgs.HasToLower()) {
			for (char& c : text) {
				c = tolower(c);
			}
		}
	}
}

}  // namespace

void Tokenizer::SkipSpace() noexcept {
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

Token Tokenizer::NextToken(Flags flgs) {
	SkipSpace();

	if (cur_ == q_.end()) {
		return Token(TokenEnd, pos_);
	}

	Token res(TokenSymbol, pos_);

	if (isalpha(*cur_) || *cur_ == '_' || *cur_ == '#' || *cur_ == '@') {
		res.type_ = TokenName;
		ConsumeNameChars(cur_, pos_, q_.end(), res.text_, flgs, true);
	} else if (*cur_ == '"') {
		res.type_ = TokenName;
		const size_t startPos = ++pos_;
		if (flgs.HasInOrderBy()) {
			res.text_.push_back('"');
		}
		while (++cur_ != q_.end() && *cur_ != '"') {
			if (pos_ == startPos) {
				if (*cur_ != '#' && *cur_ != '_' && !isalpha(*cur_) && !isdigit(*cur_) && *cur_ != '@') {
					const auto range = Where();
					throw SqlParserError{range, "Identifier should starts with alpha, digit, '_', '#' or '@', but found '{}'; {}", *cur_,
										 range};
				}
			} else if (*cur_ != '+' && *cur_ != '.' && *cur_ != '_' && *cur_ != '#' && *cur_ != '[' && *cur_ != ']' && *cur_ != '*' &&
					   !isalpha(*cur_) && !isdigit(*cur_) && *cur_ != '@') {
				const auto range = Where();
				throw SqlParserError{range, "Identifier should not contain '{}'; {}", *cur_, range};
			}
			res.text_.push_back(flgs.HasToLower() ? tolower(*cur_) : *cur_);
			++pos_;
		}
		if (flgs.HasInOrderBy()) {
			res.text_.push_back('"');
		}
		if (cur_ == q_.end()) {
			const auto range = Where();
			throw SqlParserError{range, "Not found close '\"'; {}", range};
		}
		++cur_;
		++pos_;
	} else if (*cur_ == '-' || *cur_ == '+') {
		if (flgs.HasTreatSignAsToken() || (cur_ + 1 != q_.end() && issign(*(cur_ + 1))) || cur_ + 1 == q_.end() ||
			(!isdigit(*(cur_ + 1)) && *(cur_ + 1) != '.')) {
			res.type_ = TokenSign;
			res.text_.push_back(*cur_++);
			++pos_;
		} else {
			const auto savedCur = cur_;
			const size_t savedPos = pos_;
			res.text_.push_back(*cur_++);
			++pos_;
			ConsumeNumberSuffixChars(cur_, pos_, q_.end(), res.text_);
			if (AnalyzeNumberLiteral(std::string_view{res.text_.data(), res.text_.size()}, true).valid) {
				res.type_ = TokenNumber;
			} else {
				cur_ = savedCur + 1;
				pos_ = savedPos + 1;
				res.text_.clear();
				res.text_.push_back(*savedCur);
				res.type_ = TokenSign;
			}
		}
	} else if (isdigit(*cur_)) {
		ClassifyDigitStartToken(res.text_, res.type_, cur_, pos_, q_.end(), flgs);
	} else if (cur_ != q_.end() && (*cur_ == '>' || *cur_ == '<' || *cur_ == '=')) {
		res.type_ = TokenOp;
		do {
			res.text_.push_back(*cur_++);
			++pos_;
		} while (cur_ != q_.end() && (*cur_ == '=' || *cur_ == '>' || *cur_ == '<') && res.text_.size() < 2);
	} else if (*cur_ == '\'' || *cur_ == '`') {
		res.type_ = TokenString;
		char quoteChr = *cur_++;
		res.pos_ = ++pos_;
		while (cur_ != q_.end()) {
			if (*cur_ == quoteChr) {
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

	SkipSpace();
	return res;
}

size_t Tokenizer::GetPrevPos() const {
	if (pos_ == 0) [[unlikely]] {
		throw SqlParserError(TokenizerRange{}, "Tokenizer pos is 0");
	}

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

TokenizerRange Tokenizer::where(size_t startPos, size_t lastPos) const noexcept {
	TokenizerRange result;
	symbolMultilinePos(q_, startPos, 0, result.lineStart, result.columnStart);

	result.lineEnd = result.lineStart;
	result.columnEnd = result.columnStart;
	symbolMultilinePos(q_, lastPos, startPos, result.lineEnd, result.columnEnd);
	return result;
}

TokenizerRange Tokenizer::Where() {
	size_t curPos = cur_ - q_.begin();
	size_t lastPos = q_.length();
	if (pos_ != lastPos) {
		if (const Token nextToken = PeekToken(); !nextToken.Text().empty()) {
			lastPos = pos_ + nextToken.Text().length();
		}
	}
	return where(curPos, lastPos);
}

TokenizerRange Tokenizer::Where(const Token& token) const noexcept {
	size_t startPos = token.pos_;
	size_t lastPos = startPos + token.Text().length();
	if (lastPos >= q_.length()) {
		lastPos = q_.length();
	}
	return where(startPos, lastPos);
}

Variant GetVariantFromToken(const Token& tok) {
	const std::string_view str = tok.Text();
	if (tok.Type() != TokenNumber || str.empty()) {
		return Variant(make_key_string(str.data(), str.length()));
	}

	const NumberLiteralInfo info = AnalyzeNumberLiteral(str, true);
	if (!info.valid) {
		return Variant(make_key_string(str.data(), str.length()));
	}

	if (!info.isFloat) {
		const auto intPart = str.substr(0, info.decPointPos);
		return intPart.size() <= info.maxSignsInInt ? Variant(stoll(intPart)) : Variant(make_key_string(str.data(), str.length()));
	}

	using double_conversion::StringToDoubleConverter;
	static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
	int countOfCharsParsedAsDouble = 0;
	return Variant(converter.StringToDouble(str.data(), str.size(), &countOfCharsParsedAsDouble));
}

}  // namespace reindexer
