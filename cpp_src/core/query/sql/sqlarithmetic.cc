#include "sqlarithmetic.h"
#include <algorithm>
#include <cctype>
#include "core/formatters/tokenizer_range.h"

namespace reindexer {

using namespace std::string_view_literals;

namespace {

class [[nodiscard]] TokenizerPosGuard {
public:
	explicit TokenizerPosGuard(Tokenizer& parser) noexcept : parser_{parser}, pos_{parser.GetPos()} {}
	~TokenizerPosGuard() noexcept { parser_.SetPos(pos_); }
	TokenizerPosGuard(const TokenizerPosGuard&) = delete;
	TokenizerPosGuard& operator=(const TokenizerPosGuard&) = delete;

private:
	Tokenizer& parser_;
	size_t pos_;
};

bool isComparisonOperator(const Token& tok) noexcept {
	return tok.Type() == TokenOp || isSymbol(tok, "!"sv) || tok.IsKeyword("in"sv) || tok.IsKeyword("range"sv) ||
		   tok.IsKeyword("allset"sv) || tok.IsKeyword("like"sv) || tok.IsKeyword("is"sv);
}

bool isArithmeticOperandTerminator(const Token& tok) noexcept {
	static constexpr std::string_view kKeywords[]{"and"sv,	  "or"sv,	"not"sv,   "equal_position"sv, "order"sv, "limit"sv,
												  "offset"sv, "join"sv, "inner"sv, "left"sv,		   "merge"sv};
	if (tok.Type() == TokenEnd || isComparisonOperator(tok) || isSymbol(tok, ")"sv) || isSymbol(tok, ";"sv) || isSymbol(tok, ","sv) ||
		isSymbol(tok, "}"sv)) {
		return true;
	}
	for (auto kw : kKeywords) {
		if (tok.IsKeyword(kw)) {
			return true;
		}
	}
	return false;
}

// Expects that '(' is already consumed. Returns false if the end of the query was reached first
bool skipToClosingBracket(Tokenizer& parser) {
	for (int depth = 1;;) {
		const auto tok = parser.NextToken(kArithmeticTokFlags);
		if (tok.Type() == TokenEnd) {
			return false;
		}
		if (isSymbol(tok, "("sv)) {
			++depth;
		} else if (isSymbol(tok, ")"sv) && --depth == 0) {
			return true;
		}
	}
}

bool isReservedFieldName(const Token& tok) noexcept {
	return tok.IsKeyword("select"sv) || tok.IsKeyword("not"sv) || tok.IsKeyword("equal_position"sv);
}

class [[nodiscard]] KeywordFieldTracker {
public:
	bool Track(const Token& tok) noexcept {
		const bool keywordField = !callArgument_ && isReservedFieldName(tok);
		callArgument_ = afterName_ && isSymbol(tok, "("sv);
		afterName_ = tok.Type() == TokenName && !tok.Quoted();
		return keywordField;
	}
	bool AfterName() const noexcept { return afterName_; }

private:
	bool afterName_{false};
	bool callArgument_{false};
};

// Moves the parser to the first token, which can't belong to the arithmetic operand. Returns the operand's end position.
size_t skipArithmeticOperand(Tokenizer& parser, std::string& text) {
	const size_t start = parser.GetPos();
	size_t end = start;
	bool expectPrimary = true;
	KeywordFieldTracker keywordFields;
	size_t tokensCount = 0;
	std::optional<std::pair<std::string, TokenizerRange>> unquotedKeyword;
	for (int depth = 0, bracketDepth = 0;;) {
		const size_t tokStart = parser.GetPos();
		const auto tok = parser.PeekToken(kArithmeticTokFlags);
		if (tok.Type() == TokenEnd) {
			break;
		}
		// A comma inside `[1, 2]` belongs to the operand. At depth 0 it separates SQL values.
		if (depth == 0 && bracketDepth == 0) {
			if (!expectPrimary && tok.Type() == TokenName) {
				break;
			}
			if (expectPrimary ? tok.Type() != TokenName && isArithmeticOperandTerminator(tok) : isArithmeticOperandTerminator(tok)) {
				break;
			}
			expectPrimary = isArithmeticOperator(tok);
		}
		if (isSymbol(tok, "("sv)) {
			if (!keywordFields.AfterName() && parser.PeekSecondToken(kArithmeticTokFlags).IsKeyword("select"sv)) [[unlikely]] {
				const auto range = parser.Where(tok);
				throw SqlParserError(range, "Subqueries are not supported by arithmetic expressions in WHERE, {}", range);
			}
			++depth;
		} else if (isSymbol(tok, ")"sv)) {
			--depth;
		} else if (isSymbol(tok, "["sv)) {
			++bracketDepth;
		} else if (isSymbol(tok, "]"sv) && bracketDepth > 0) {
			--bracketDepth;
		}
		if (keywordFields.Track(tok) && !unquotedKeyword) {
			unquotedKeyword.emplace(tok.Text(), parser.Where(tok));
		}
		++tokensCount;
		if (end > start) {
			const std::string_view gap{parser.Begin() + end, tokStart - end};
			text.append(gap.find("--"sv) == std::string_view::npos ? gap : " "sv);
		}
		text.append(parser.Begin() + tokStart, tok.SourceEnd() - tokStart);
		parser.SkipToken(kArithmeticTokFlags);
		end = tok.SourceEnd();
	}
	if (unquotedKeyword && tokensCount > 1) [[unlikely]] {
		const auto& [keyword, range] = *unquotedKeyword;
		throw SqlParserError(range, "Keyword '{}' must be double-quoted when used as a field name, {}", keyword, range);
	}
	return end;
}

// A whole operand that the WHERE expression grammar does not accept: string, bool, null
bool isBareSqlLiteral(const Token& tok) noexcept {
	return tok.Type() == TokenString || tok.IsKeyword("true"sv) || tok.IsKeyword("false"sv) || tok.IsKeyword("null"sv);
}

enum class [[nodiscard]] BracketContent { NotBracket, SingleLiteral, ValueList, Operand, Unclosed, InvalidToken };

BracketContent bracketContent(Tokenizer& parser) noexcept {
	const TokenizerPosGuard guard{parser};
	try {
		if (!isSymbol(parser.PeekToken(kArithmeticTokFlags), "("sv)) {
			return BracketContent::NotBracket;
		}
		parser.SkipToken(kArithmeticTokFlags);
		if (isBareSqlLiteral(parser.PeekToken(kArithmeticTokFlags)) && isSymbol(parser.PeekSecondToken(kArithmeticTokFlags), ")"sv)) {
			return BracketContent::SingleLiteral;
		}
		for (int depth = 1; depth > 0;) {
			const auto tok = parser.NextToken(kArithmeticTokFlags);
			if (tok.Type() == TokenEnd) {
				return BracketContent::Unclosed;
			}
			if (isSymbol(tok, "("sv)) {
				++depth;
			} else if (isSymbol(tok, ")"sv)) {
				--depth;
			} else if (depth == 1 && isSymbol(tok, ","sv)) {
				return BracketContent::ValueList;
			}
		}
		return BracketContent::Operand;
	} catch (const std::exception&) {
		return BracketContent::InvalidToken;
	}
}

}  // namespace

bool isFieldNameToken(const Token& tok) noexcept {
	return tok.Type() == TokenName && !tok.IsKeyword("true"sv) && !tok.IsKeyword("false"sv) && !tok.IsKeyword("null"sv);
}

bool isArithmeticOperator(const Token& tok) noexcept { return tok.Type() == TokenSign || isSymbol(tok, "*"sv) || isSymbol(tok, "/"sv); }

bool operandHasArithmetic(Tokenizer& parser) noexcept {
	const TokenizerPosGuard guard{parser};
	try {
		auto tok = parser.NextToken(kArithmeticTokFlags);
		if (tok.Type() == TokenSign) {
			tok = parser.NextToken(kArithmeticTokFlags);
			if (isSymbol(tok, "("sv) || isFieldNameToken(tok)) {
				return true;
			}
			if (tok.Type() != TokenNumber) {
				return false;
			}
		} else if (isSymbol(tok, "("sv)) {
			if (!skipToClosingBracket(parser)) {
				return false;
			}
		} else if (tok.Type() == TokenName) {
			if (!tok.Quoted() && isSymbol(parser.PeekToken(kArithmeticTokFlags), "("sv)) {
				parser.SkipToken(kArithmeticTokFlags);
				if (!skipToClosingBracket(parser)) {
					return false;
				}
			}
		} else if (tok.Type() != TokenNumber) {
			return false;
		}
		return isArithmeticOperator(parser.PeekToken(kArithmeticTokFlags));
	} catch (const std::exception&) {
		return false;
	}
}

bool isJoinKeyword(const Token& tok, std::string_view kw, const Token& next) noexcept {
	return tok.IsKeyword(kw) && !isArithmeticOperator(next) && !isComparisonOperator(next);
}

bool isJoinKeyword(Tokenizer& parser, const Token& tok, std::string_view kw) {
	return tok.IsKeyword(kw) && isJoinKeyword(tok, kw, parser.PeekToken(kArithmeticTokFlags));
}

bool ArithmeticBracketDetector::IsArithmetic(Tokenizer& parser) noexcept {
	const TokenizerPosGuard guard{parser};
	const size_t pos = parser.GetPos();
	const auto found = std::ranges::lower_bound(brackets_, pos, {}, &Bracket::contentPos);
	if (found != brackets_.end() && found->contentPos == pos) {
		return found->arithmetic;
	}
	try {
		scanGroup(parser);
		return brackets_.front().arithmetic;
	} catch (const std::exception&) {
		return false;
	}
}

void ArithmeticBracketDetector::scanGroup(Tokenizer& parser) {
	brackets_.clear();
	brackets_.push_back(Bracket{.contentPos = parser.GetPos(), .arithmetic = false});
	h_vector<unsigned, 16> open;
	open.push_back(0);
	while (!open.empty()) {
		const auto tok = parser.NextToken(kArithmeticTokFlags);
		if (tok.Type() == TokenEnd) {
			return;
		}
		if (isSymbol(tok, "("sv)) {
			open.push_back(brackets_.size());
			brackets_.push_back(Bracket{.contentPos = parser.GetPos(), .arithmetic = false});
		} else if (isSymbol(tok, ")"sv)) {
			const auto next = parser.PeekToken(kArithmeticTokFlags);
			brackets_[open.back()].arithmetic = isArithmeticOperator(next) || isComparisonOperator(next);
			open.pop_back();
		}
	}
}

bool isArithmeticValueBracket(Tokenizer& parser) noexcept {
	const TokenizerPosGuard guard{parser};
	try {
		parser.SkipToken(kArithmeticTokFlags);
		bool arithmetic = false;
		bool afterPrimary = false;
		for (int depth = 1;;) {
			const auto tok = parser.NextToken(kArithmeticTokFlags);
			if (tok.Type() == TokenEnd) {
				return false;
			}
			if (isSymbol(tok, "("sv)) {
				arithmetic = arithmetic || depth == 1;
				afterPrimary = false;
				++depth;
				continue;
			}
			if (isSymbol(tok, ")"sv)) {
				if (--depth == 0) {
					break;
				}
				afterPrimary = true;
				continue;
			}
			if (depth > 1) {
				continue;
			}
			if (isSymbol(tok, "{"sv)) {
				for (int braces = 1; braces > 0;) {
					const auto inner = parser.NextToken(kArithmeticTokFlags);
					if (inner.Type() == TokenEnd) {
						return false;
					}
					if (isSymbol(inner, "{"sv)) {
						++braces;
					} else if (isSymbol(inner, "}"sv)) {
						--braces;
					}
				}
				afterPrimary = true;
				continue;
			}
			if (isSymbol(tok, ","sv)) {
				return false;
			}
			if (isArithmeticOperator(tok)) {
				arithmetic = arithmetic || afterPrimary;
				afterPrimary = false;
				continue;
			}
			arithmetic = arithmetic || isFieldNameToken(tok);
			afterPrimary = true;
		}
		return arithmetic || isArithmeticOperator(parser.PeekToken(kArithmeticTokFlags));
	} catch (const std::exception&) {
		return false;
	}
}

bool isValueListBracket(Tokenizer& parser) noexcept {
	const auto content = bracketContent(parser);
	return content == BracketContent::SingleLiteral || content == BracketContent::ValueList;
}

ArithmeticOperand parseArithmeticOperand(Tokenizer& parser) {
	parser.SkipSpace();
	const size_t start = parser.GetPos();
	{
		auto tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		const auto next = parser.PeekToken(kArithmeticTokFlags);
		if ((tok.Type() == TokenNumber || isBareSqlLiteral(tok)) && !isArithmeticOperator(next)) {
			ArithmeticOperand operand;
			operand.sourceBegin = start;
			operand.sourceEnd = tok.SourceEnd();
			try {
				operand.value = comparison::Literal{Token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True)};
			} catch (const SqlParserError&) {
				throw;
			} catch (const Error& err) {
				const auto range = operand.Range(parser);
				throw SqlParserError(range, "{}, {}", err.what(), range);
			}
			return operand;
		}
		if (isFieldNameToken(tok) && (next.Type() == TokenName || isArithmeticOperandTerminator(next))) {
			ArithmeticOperand operand;
			operand.value = comparison::Field{std::string{tok.Text()}};
			operand.sourceBegin = start;
			operand.sourceEnd = tok.SourceEnd();
			return operand;
		}
		parser.SetPos(start);
	}
	const auto first = parser.PeekToken(kArithmeticTokFlags);
	if ((first.Type() == TokenSymbol && first.Text() != "("sv && first.Text() != "{"sv) || first.Type() == TokenOp) {
		auto tok = parser.NextToken(kArithmeticTokFlags);
		const auto range = parser.Where(tok);
		throw SqlParserError(range, "Expected parameter, but found '{}' in query, {}", tok.Text(), range);
	}
	if (isSymbol(first, "{"sv)) {
		auto tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		ArithmeticOperand operand;
		auto literal = Token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_True);
		auto end = parser.GetPos();
		while (end > start && std::isspace(static_cast<unsigned char>(parser.Begin()[end - 1]))) {
			--end;
		}
		operand.sourceBegin = start;
		operand.sourceEnd = end;
		operand.value = comparison::Literal{std::move(literal)};
		return operand;
	}
	std::string text;
	const size_t end = skipArithmeticOperand(parser, text);
	if (end <= start) [[unlikely]] {
		const auto tok = parser.PeekToken();
		const auto tokRange = parser.Where(tok);
		throw SqlParserError(tokRange, "Expected field, value or arithmetic expression, but found '{}' in query, {}", tok.Text(), tokRange);
	}
	ArithmeticOperand operand;
	operand.sourceBegin = start;
	operand.sourceEnd = end;
	try {
		operand.value = comparison::Classify(expressions::ArithmeticExpression{text});
	} catch (const SqlParserError&) {
		throw;
	} catch (const Error& err) {
		const auto range = operand.Range(parser);
		throw SqlParserError(range, "{}, {}", err.what(), range);
	}
	return operand;
}

}  // namespace reindexer
