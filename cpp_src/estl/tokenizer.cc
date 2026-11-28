#include "tokenizer.h"
#include <cctype>
#include <initializer_list>
#include "core/formatters/tokenizer_range.h"
#include "double-conversion/double-conversion.h"
#include "estl/defines.h"
#include "tools/errors.h"
#include "tools/serilize/wrserializer.h"
#include "tools/stringstools.h"

namespace reindexer {
namespace {

RX_ALWAYS_INLINE bool IsAlphaChar(char c) noexcept { return std::isalpha(static_cast<unsigned char>(c)); }
RX_ALWAYS_INLINE bool IsDigitChar(char c) noexcept { return std::isdigit(static_cast<unsigned char>(c)); }
RX_ALWAYS_INLINE bool IsSpaceChar(char c) noexcept { return std::isspace(static_cast<unsigned char>(c)); }
RX_ALWAYS_INLINE char ToLowerChar(char c) noexcept { return static_cast<char>(std::tolower(static_cast<unsigned char>(c))); }

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

	if (!IsDigitChar(str[0]) && (str.size() == 1 || !issign(str[0]))) {
		return info;
	}

	if (!allowLeadingSign && !IsDigitChar(str[0])) {
		return info;
	}

	bool isFloat = false;
	// INT64_MAX(9'223'372'036'854'775'807) contains 19 digits + 1 for possible sign
	const size_t maxSignsInInt = 19 + (IsDigitChar(str[0]) ? 0 : 1);
	bool nullDecimalPart = true;
	bool hasMantissaDigit = IsDigitChar(str[0]);

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

		if (!IsDigitChar(str[i])) {
			return info;
		}

		if (ePos == str.size()) {
			hasMantissaDigit = true;
		}

		if (isFloat) {
			nullDecimalPart = nullDecimalPart && str[i] == '0';
		}
	}

	if (ePos + 1 == str.size() || (ePos + 2 == str.size() && !IsDigitChar(str[ePos + 1]))) {
		return info;
	}

	if (ePos == 1 && !IsDigitChar(str[0])) {
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

RX_ALWAYS_INLINE bool IsNameStartChar(char c) noexcept { return IsAlphaChar(c) || c == '_' || c == '#' || c == '@'; }

RX_ALWAYS_INLINE bool IsNameChar(char c, char prev, int& openBrackets) noexcept {
	switch (c) {
		case '[':
			++openBrackets;
			return true;
		case ']':
			return --openBrackets >= 0;
		case '*':
			return prev == '[';
		default:
			return IsAlphaChar(c) || IsDigitChar(c) || c == '_' || c == '#' || c == '@' || c == '.';
	}
}

bool IsUnquotedName(std::initializer_list<std::string_view> parts) noexcept {
	int openBrackets{0};
	char prev{'\0'};
	bool empty{true};
	for (const auto part : parts) {
		for (const char c : part) {
			if (empty ? !IsNameStartChar(c) : !IsNameChar(c, prev, openBrackets)) {
				return false;
			}
			empty = false;
			prev = c;
		}
	}
	return !empty;
}

RX_ALWAYS_INLINE void ConsumeNameChars(std::string_view::const_iterator& cur, size_t& pos, std::string_view::const_iterator end,
									   Token::StorageT& text, Tokenizer::Flags flgs, bool applyToLower) {
	int openBrackets{0};
	char prev;
	do {
		prev = *cur;
		text.push_back(applyToLower && flgs.HasToLower() ? ToLowerChar(*cur++) : *cur++);
		++pos;
	} while (cur != end && IsNameChar(*cur, prev, openBrackets));
}

RX_ALWAYS_INLINE void ConsumeNumberSuffixChars(std::string_view::const_iterator& cur, size_t& pos, std::string_view::const_iterator end,
											   Token::StorageT& text) {
	while (cur != end &&
		   (IsDigitChar(*cur) || *cur == '.' || IsExponentChar(*cur) || (!text.empty() && IsExponentChar(text.back()) && issign(*cur)))) {
		text.push_back(*cur++);
		++pos;
	}
}

RX_ALWAYS_INLINE bool IsNameContinuationChar(char c) noexcept {
	return IsAlphaChar(c) || IsDigitChar(c) || c == '_' || c == '#' || c == '@' || c == '.' || c == '*' || c == '[' || c == ']';
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
				c = ToLowerChar(c);
			}
		}
	}
}

bool DecodeEscapedChar(char c, char& decoded) noexcept {
	switch (c) {
		case '0':
			decoded = '\0';
			return true;
		case 'a':
			decoded = '\a';
			return true;
		case 'b':
			decoded = '\b';
			return true;
		case 't':
			decoded = '\t';
			return true;
		case 'n':
			decoded = '\n';
			return true;
		case 'v':
			decoded = '\v';
			return true;
		case 'f':
			decoded = '\f';
			return true;
		case 'r':
			decoded = '\r';
			return true;
		case 'e':
			decoded = '\x1b';
			return true;
		case '\'':
		case '"':
		case '\\':
			decoded = c;
			return true;
		default:
			return false;
	}
}

std::string_view EscapeSequence(char c, char quote) noexcept {
	using namespace std::string_view_literals;
	switch (c) {
		case '\0':
			return "\\0"sv;
		case '\a':
			return "\\a"sv;
		case '\b':
			return "\\b"sv;
		case '\t':
			return "\\t"sv;
		case '\n':
			return "\\n"sv;
		case '\v':
			return "\\v"sv;
		case '\f':
			return "\\f"sv;
		case '\r':
			return "\\r"sv;
		case '\x1b':
			return "\\e"sv;
		case '\\':
			return "\\\\"sv;
		case '\'':
			return quote == '\'' ? "\\'"sv : std::string_view{};
		case '"':
			return quote == '"' ? "\\\""sv : std::string_view{};
		default:
			return {};
	}
}

template <typename Append>
void AppendQuoted(std::string_view str, char quote, Append&& append) {
	append(std::string_view{&quote, 1});
	size_t plainBegin = 0;
	for (size_t i = 0; i < str.size(); ++i) {
		if (const auto escaped = EscapeSequence(str[i], quote); !escaped.empty()) {
			append(str.substr(plainBegin, i - plainBegin));
			append(escaped);
			plainBegin = i + 1;
		}
	}
	append(str.substr(plainBegin));
	append(std::string_view{&quote, 1});
}

bool ConsumeQuoted(std::string_view::const_iterator& cur, size_t& pos, std::string_view::const_iterator end, Token::StorageT& text,
				   bool toLower) {
	const char quote = *cur++;
	++pos;
	while (cur != end) {
		char c = *cur;
		if (c == quote) {
			if (cur + 1 == end || *(cur + 1) != quote) {
				++cur;
				++pos;
				return true;
			}
			cur += 2;
			pos += 2;
		} else if (c == '\\' && cur + 1 != end) {
			if (char decoded; DecodeEscapedChar(*(cur + 1), decoded)) {
				c = decoded;
			} else {
				text.push_back(c);
				c = *(cur + 1);
			}
			cur += 2;
			pos += 2;
		} else {
			++cur;
			++pos;
		}
		text.push_back(toLower ? ToLowerChar(c) : c);
	}
	return false;
}

}  // namespace

bool Token::IsKeyword(std::string_view kw) const noexcept { return type_ == TokenName && !quoted_ && iequals(Text(), kw); }

void Tokenizer::SkipSpace(Flags flgs) noexcept {
	for (;;) {
		while (cur_ != q_.end() && IsSpaceChar(*cur_)) {
			cur_++;
			pos_++;
		}
		if (!flgs.HasNoLineComments() && cur_ != q_.end() && *cur_ == '-' && cur_ + 1 != q_.end() && *(cur_ + 1) == '-') {
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
	SkipSpace(flgs);

	if (cur_ == q_.end()) {
		return Token(TokenEnd, pos_);
	}

	Token res(TokenSymbol, pos_);

	if (IsNameStartChar(*cur_)) {
		res.type_ = TokenName;
		ConsumeNameChars(cur_, pos_, q_.end(), res.text_, flgs, true);
	} else if (*cur_ == '"') {
		res.type_ = TokenName;
		res.quoted_ = true;
		if (!ConsumeQuoted(cur_, pos_, q_.end(), res.text_, flgs.HasToLower())) {
			const auto range = Where();
			throw SqlParserError{range, "Not found close '\"'; {}", range};
		}
		if (flgs.HasInOrderBy()) {
			Token::StorageT quotedText;
			AppendQuoted(res.Text(), '"',
						 [&quotedText](std::string_view part) { quotedText.insert(quotedText.end(), part.begin(), part.end()); });
			res.text_ = std::move(quotedText);
		}
	} else if (*cur_ == '-' || *cur_ == '+') {
		if (flgs.HasTreatSignAsToken() || (cur_ + 1 != q_.end() && issign(*(cur_ + 1))) || cur_ + 1 == q_.end() ||
			(!IsDigitChar(*(cur_ + 1)) && *(cur_ + 1) != '.')) {
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
	} else if (IsDigitChar(*cur_)) {
		ClassifyDigitStartToken(res.text_, res.type_, cur_, pos_, q_.end(), flgs);
	} else if (cur_ != q_.end() && (*cur_ == '>' || *cur_ == '<' || *cur_ == '=')) {
		res.type_ = TokenOp;
		do {
			res.text_.push_back(*cur_++);
			++pos_;
		} while (cur_ != q_.end() && (*cur_ == '=' || *cur_ == '>' || *cur_ == '<') && res.text_.size() < 2);
	} else if (*cur_ == '\'' || *cur_ == '`') {
		res.type_ = TokenString;
		res.pos_ = pos_ + 1;
		const char quote = *cur_;
		if (!ConsumeQuoted(cur_, pos_, q_.end(), res.text_, false)) {
			const auto range = Where();
			throw SqlParserError{range, "Not found close '{}'; {}", quote, range};
		}
	} else {
		res.text_.push_back(*cur_++);
		++pos_;
	}

	res.sourceEnd_ = pos_;
	SkipSpace(flgs);
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
		while (cur != q_.begin() && IsSpaceChar(*cur)) {
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

bool FieldNameNeedsQuotes(std::string_view name) noexcept {
	constexpr static std::string_view kReservedNames[]{"true", "false", "null", "not", "select", "equal_position"};
	for (const auto reserved : kReservedNames) {
		if (iequals(name, reserved)) {
			return true;
		}
	}
	return !IsUnquotedName({name});
}

void PrintQuotedFieldName(WrSerializer& ser, std::string_view name) {
	AppendQuoted(name, '"', [&ser](std::string_view part) { ser << part; });
}

void PrintQuotedString(WrSerializer& ser, std::string_view str) {
	AppendQuoted(str, '\'', [&ser](std::string_view part) { ser << part; });
}

void PrintFieldName(WrSerializer& ser, std::string_view name) {
	if (FieldNameNeedsQuotes(name)) {
		PrintQuotedFieldName(ser, name);
	} else {
		ser << name;
	}
}

void PrintFieldName(WrSerializer& ser, std::string_view ns, std::string_view field) {
	if (IsUnquotedName({ns, ".", field})) {
		ser << ns << '.' << field;
	} else {
		WrSerializer fullName;
		fullName << ns << '.' << field;
		PrintQuotedFieldName(ser, fullName.Slice());
	}
}

}  // namespace reindexer
