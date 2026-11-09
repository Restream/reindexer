#include "sqlarithmetic.h"
#include <cctype>
#include "core/formatters/tokenizer_range.h"
#include "tools/overflow.h"

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

// Moves the parser to the first token, which can't belong to the arithmetic operand. Returns the operand's end position.
// Keywords end the operand only after a complete primary (`a+b ORDER BY ...`). Where a primary is expected (at the beginning
// and after + - * /) any name is a field: `order+1`, `price+limit`. Another name after a finished primary starts the next
// clause (`AND equal_position(...) AND arr2`), so it is not part of this operand.
size_t skipArithmeticOperand(Tokenizer& parser, std::string& text) {
	const size_t start = parser.GetPos();
	size_t end = start;
	bool expectPrimary = true;
	for (int depth = 0;;) {
		const size_t tokStart = parser.GetPos();
		const auto tok = parser.PeekToken(kArithmeticTokFlags);
		if (tok.Type() == TokenEnd) {
			break;
		}
		if (depth == 0) {
			if (!expectPrimary && tok.Type() == TokenName) {
				break;
			}
			if (expectPrimary ? tok.Type() != TokenName && isArithmeticOperandTerminator(tok) : isArithmeticOperandTerminator(tok)) {
				break;
			}
			expectPrimary = isArithmeticOperator(tok);
		}
		if (isSymbol(tok, "("sv)) {
			++depth;
		} else if (isSymbol(tok, ")"sv)) {
			--depth;
		}
		if (end > start) {
			const std::string_view gap{parser.Begin() + end, tokStart - end};
			text.append(gap.find("--"sv) == std::string_view::npos ? gap : " "sv);
		}
		text.append(parser.Begin() + tokStart, tok.SourceEnd() - tokStart);
		parser.SkipToken(kArithmeticTokFlags);
		end = tok.SourceEnd();
	}
	return end;
}

// A whole operand that the WHERE expression grammar does not accept: string, bool, null
bool isBareSqlLiteral(const Token& tok) noexcept {
	return tok.Type() == TokenString || tok.IsKeyword("true"sv) || tok.IsKeyword("false"sv) || tok.IsKeyword("null"sv);
}

const ExprNumber* numberNode(const ExprNode* node) noexcept {
	return node && node->Type() == ExprNodeType::Number ? static_cast<const ExprNumber*>(node) : nullptr;
}

void setLiteralOperand(ArithmeticOperand& operand, Variant value) {
	operand.kind = ArithmeticOperand::Kind::Literal;
	operand.literal = std::move(value);
}

// now()/flat_array_len() are built from the AST. Re-parsing Dump() uses a different grammar and drops quotes.
std::optional<functions::FunctionVariant> whereFunctionFromAst(const ExprFunction& fn) {
	if (!fn.exprArgs.empty()) {
		return std::nullopt;
	}
	switch (fn.kind) {
		case ExprFunction::Kind::Now:
			return functions::Now{fn.timeUnit};
		case ExprFunction::Kind::FlatArrayLen:
			if (fn.stringArgs.size() != 1) {
				return std::nullopt;
			}
			return functions::FlatArrayLen{fn.stringArgs.front().value};
		case ExprFunction::Kind::Serial:
		case ExprFunction::Kind::ArrayRemove:
		case ExprFunction::Kind::ArrayRemoveOnce:
			return std::nullopt;
	}
	return std::nullopt;
}

void setConstantLiteral(ArithmeticOperand& operand, const expressions::ArithmeticExpression& expression) {
	const ExprNode* root = expression.Ast().Root();
	if (const ExprNumber* number = numberNode(root)) {
		setLiteralOperand(operand, number->value);
		return;
	}
	if (!root || root->Type() != ExprNodeType::UnaryMinus) {
		return;
	}
	const ExprNumber* number = numberNode(static_cast<const ExprUnaryMinus*>(root)->child.get());
	if (!number) {
		return;
	}
	const auto& value = number->value;
	if (value.Type().Is<KeyValueType::Int64>()) {
		int64_t negated = 0;
		if (SubOverflow(int64_t{0}, value.As<int64_t>(), negated)) {
			throw Error(errLogic, kIntegerOverflowError);
		}
		setLiteralOperand(operand, Variant{negated});
	} else if (value.Type().Is<KeyValueType::Double>()) {
		setLiteralOperand(operand, Variant{-value.As<double>()});
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

bool isArithmeticBracket(Tokenizer& parser) noexcept {
	const TokenizerPosGuard guard{parser};
	try {
		if (!skipToClosingBracket(parser)) {
			return false;
		}
		const auto next = parser.PeekToken(kArithmeticTokFlags);
		return isArithmeticOperator(next) || isComparisonOperator(next);
	} catch (const std::exception&) {
		return false;
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

ArithmeticOperand parseArithmeticOperand(Tokenizer& parser) {
	parser.SkipSpace();
	const size_t start = parser.GetPos();
	{
		auto tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		const auto next = parser.PeekToken(kArithmeticTokFlags);
		if ((tok.Type() == TokenNumber || isBareSqlLiteral(tok)) && !isArithmeticOperator(next)) {
			ArithmeticOperand operand;
			operand.kind = ArithmeticOperand::Kind::Literal;
			operand.sourceBegin = start;
			operand.sourceEnd = tok.SourceEnd();
			operand.source = std::string{tok.Text()};
			try {
				operand.literal = Token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True);
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
			operand.kind = ArithmeticOperand::Kind::Field;
			operand.field = std::string{tok.Text()};
			operand.source = operand.field;
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
	if (first.Text() == "{"sv) {
		auto tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		ArithmeticOperand operand;
		operand.kind = ArithmeticOperand::Kind::Literal;
		operand.literal = Token2kv(tok, parser, CompositeAllowed_True, FieldAllowed_False, NullAllowed_True);
		auto end = parser.GetPos();
		while (end > start && std::isspace(static_cast<unsigned char>(parser.Begin()[end - 1]))) {
			--end;
		}
		operand.sourceBegin = start;
		operand.sourceEnd = end;
		operand.source = std::string(parser.Begin() + start, end - start);
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
	operand.source = text;
	try {
		operand.expression = expressions::ArithmeticExpression{text};
		const auto& ast = operand.expression.Ast();
		if (ast.Root()->Type() == ExprNodeType::Field) {
			operand.kind = ArithmeticOperand::Kind::Field;
			operand.field = static_cast<const ExprField*>(ast.Root())->name;
		} else if (ast.Root()->Type() == ExprNodeType::Function) {
			const auto* fn = static_cast<const ExprFunction*>(ast.Root());
			// Parentheses alone must not turn now()/flat_array_len() into an arithmetic expression.
			if (auto function = whereFunctionFromAst(*fn)) {
				operand.kind = ArithmeticOperand::Kind::Function;
				operand.function = std::move(*function);
			}
		} else {
			setConstantLiteral(operand, operand.expression);
		}
	} catch (const SqlParserError&) {
		throw;
	} catch (const Error& err) {
		const auto range = operand.Range(parser);
		throw SqlParserError(range, "{}, {}", err.what(), range);
	}
	return operand;
}

}  // namespace reindexer
