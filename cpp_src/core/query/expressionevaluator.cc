#include "expressionevaluator.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/selectfunc/selectfunc.h"
#include "double-conversion/double-conversion.h"
#include "estl/tokenizer.h"

namespace reindexer {

using namespace std::string_view_literals;

constexpr char kWrongFieldTypeError[] = "Only integral type non-array fields are supported in arithmetical expressions: %s";
constexpr char kScalarsInConcatenationError[] = "Unable to use scalar values in the arrays concatenation expressions: %s";

void ExpressionEvaluator::captureArrayContent(tokenizer& parser) {
	arrayValues_.MarkArray();
	token tok = parser.next_token(tokenizer::flags::no_flags);
	if (tok.text() == "]") {
		return;
	}
	for (;; tok = parser.next_token(tokenizer::flags::no_flags)) {
		if rx_unlikely (tok.text() == "]"sv) {
			throw Error(errParseSQL, "Expected field value, but found ']' in query, %s", parser.where());
		}
		arrayValues_.emplace_back(token2kv(tok, parser, false));
		tok = parser.next_token(tokenizer::flags::no_flags);
		if (tok.text() == "]"sv) {
			break;
		}
		if rx_unlikely (tok.text() != ","sv) {
			throw Error(errParseSQL, "Expected ']' or ',', but found '%s' in query, %s", tok.text(), parser.where());
		}
	}
}

void ExpressionEvaluator::throwUnexpectedTokenError(tokenizer& parser, const token& outTok) {
	if (state_ == StateArrayConcat || parser.peek_token(tokenizer::flags::treat_sign_as_token).text() == "|"sv) {
		throw Error(errParams, kScalarsInConcatenationError, outTok.text());
	}
	throw Error(errParams, kWrongFieldTypeError, outTok.text());
}

ExpressionEvaluator::PrimaryToken ExpressionEvaluator::getPrimaryToken(tokenizer& parser, const PayloadValue& v, token& outTok,
																	   const NsContext& ctx) {
	outTok = parser.next_token();
	if (outTok.text() == "("sv) {
		const double val = performSumAndSubtracting(parser, v, ctx);
		if rx_unlikely (parser.next_token().text() != ")"sv) {
			throw Error(errParams, "')' expected in arithmetical expression");
		}
		return {.value = val, .type = PrimaryToken::Type::Scalar};
	} else if (outTok.text() == "["sv) {
		captureArrayContent(parser);
		return {.value = std::nullopt, .type = PrimaryToken::Type::Array};
	}
	switch (outTok.type) {
		case TokenNumber: {
			try {
				using double_conversion::StringToDoubleConverter;
				static const StringToDoubleConverter converter{StringToDoubleConverter::NO_FLAGS, NAN, NAN, nullptr, nullptr};
				int countOfCharsParsedAsDouble;
				return {.value = converter.StringToDouble(outTok.text_.data(), outTok.text_.size(), &countOfCharsParsedAsDouble),
						.type = PrimaryToken::Type::Scalar};
			} catch (...) {
				throw Error(errParams, "Unable to convert '%s' to double value", outTok.text());
			}
		}
		case TokenName:
			return handleTokenName(parser, v, outTok, ctx);
		case TokenString:
			throwUnexpectedTokenError(parser, outTok);
		case TokenEnd:
		case TokenOp:
		case TokenSymbol:
		case TokenSign:
			break;
	}
	throw Error(errParams, "Unexpected token in expression: '%s'", outTok.text());
}

ExpressionEvaluator::PrimaryToken ExpressionEvaluator::handleTokenName(tokenizer& parser, const PayloadValue& v, token& outTok,
																	   const NsContext& ctx) {
	int field = 0;
	VariantArray fieldValues;
	ConstPayload pv(type_, v);
	if (type_.FieldByName(outTok.text(), field)) {
		if (type_.Field(field).IsArray()) {
			pv.Get(field, fieldValues);
			arrayValues_.MarkArray();
			for (Variant& v : fieldValues) {
				arrayValues_.emplace_back(std::move(v));
			}
			return (state_ == StateArrayConcat || fieldValues.size() != 1)
					   ? PrimaryToken{.value = std::nullopt, .type = PrimaryToken::Type::Array}
					   : type_.Field(field).Type().EvaluateOneOf(
							 [this](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) -> PrimaryToken {
								 return {.value = arrayValues_.back().As<double>(), .type = PrimaryToken::Type::Array};
							 },
							 [&, this](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid>) -> PrimaryToken {
								 if rx_unlikely (state_ != StateArrayConcat &&
												 parser.peek_token(tokenizer::flags::treat_sign_as_token).text() != "|"sv) {
									 throw Error(errParams, kWrongFieldTypeError, outTok.text());
								 }
								 return {.value = std::nullopt, .type = PrimaryToken::Type::Array};
							 },
							 [](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) noexcept
							 -> PrimaryToken {
								 assertrx_throw(false);
								 abort();
							 });
		}
		return type_.Field(field).Type().EvaluateOneOf(
			[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) -> PrimaryToken {
				pv.Get(field, fieldValues);
				if rx_unlikely (fieldValues.empty()) {
					throw Error(errParams, "Calculating value of an empty field is impossible: %s", outTok.text());
				}
				return {.value = fieldValues.front().As<double>(), .type = PrimaryToken::Type::Scalar};
			},
			[&, this](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid>) -> PrimaryToken {
				throwUnexpectedTokenError(parser, outTok);
			},
			[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> PrimaryToken {
				assertrx_throw(false);
				abort();
			});
	} else if rx_unlikely (outTok.text() == "true"sv || outTok.text() == "false"sv) {
		throwUnexpectedTokenError(parser, outTok);
	}

	pv.GetByJsonPath(outTok.text(), tagsMatcher_, fieldValues, KeyValueType::Undefined{});

	if (fieldValues.IsNullValue()) {
		return {.value = std::nullopt, .type = PrimaryToken::Type::Null};
	}

	const bool isArrayField = fieldValues.IsArrayValue();
	if (isArrayField) {
		for (Variant& v : fieldValues) {
			arrayValues_.emplace_back(std::move(v));
		}
		if ((state_ == StateArrayConcat) || (fieldValues.size() != 1)) {
			return {.value = std::nullopt, .type = PrimaryToken::Type::Array};
		}
	}
	if (fieldValues.size() == 1) {
		const Variant* vptr = isArrayField ? &arrayValues_.back() : &fieldValues.front();
		return vptr->Type().EvaluateOneOf(
			[vptr, isArrayField](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) -> PrimaryToken {
				return {.value = vptr->As<double>(), .type = isArrayField ? PrimaryToken::Type::Array : PrimaryToken::Type::Scalar};
			},
			[&, this](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid>) -> PrimaryToken {
				if (isArrayField) {
					return {.value = std::nullopt, .type = PrimaryToken::Type::Array};
				}
				throwUnexpectedTokenError(parser, outTok);
			},
			[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> PrimaryToken {
				assertrx_throw(0);
				abort();
			});
	} else if (parser.peek_token(tokenizer::flags::treat_sign_as_token).text() == "(") {
		SelectFuncStruct funcData = SelectFuncParser().ParseFunction(parser, true, outTok);
		funcData.field = std::string(forField_);
		return {.value = functionExecutor_.Execute(funcData, ctx).As<double>(), .type = PrimaryToken::Type::Scalar};
	}
	return {.value = std::nullopt, .type = PrimaryToken::Type::Null};
}

double ExpressionEvaluator::performArrayConcatenation(tokenizer& parser, const PayloadValue& v, token& tok, const NsContext& ctx) {
	token valueToken;
	auto left = getPrimaryToken(parser, v, valueToken, ctx);
	tok = parser.peek_token();
	switch (left.type) {
		case PrimaryToken::Type::Scalar:
			if rx_unlikely (tok.text() == "|"sv) {
				throw Error(errParams, kScalarsInConcatenationError, valueToken.text());
			}
			break;
		case PrimaryToken::Type::Array:
		case PrimaryToken::Type::Null:
			if rx_unlikely (!left.value.has_value() && tok.text() != "|"sv) {
				throw Error(errParams, "Unable to use array and null values outside of the arrays concatenation");
			}
			break;
	}

	while (tok.text() == "|"sv) {
		parser.next_token();
		tok = parser.next_token();
		if rx_unlikely (tok.text() != "|") {
			throw Error(errParams, "Expected '|', not %s", tok.text());
		}
		if rx_unlikely (state_ != StateArrayConcat && state_ != None) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '%s'", tok.text());
		}
		state_ = StateArrayConcat;
		const auto right = getPrimaryToken(parser, v, valueToken, ctx);
		if rx_unlikely (right.type == PrimaryToken::Type::Scalar) {
			throw Error(errParams, kScalarsInConcatenationError, valueToken.text());
		}
		assertrx_throw(!right.value.has_value());
		tok = parser.peek_token();
	}
	return left.value.has_value() ? left.value.value() : 0.0;
}

double ExpressionEvaluator::performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& tok, const NsContext& ctx) {
	double left = performArrayConcatenation(parser, v, tok, ctx);
	tok = parser.peek_token(tokenizer::flags::treat_sign_as_token);
	while (tok.text() == "*"sv || tok.text() == "/"sv) {
		if rx_unlikely (state_ == StateArrayConcat) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '%s'", tok.text());
		}
		state_ = StateMultiplyAndDivide;
		if (tok.text() == "*"sv) {
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			left *= performMultiplicationAndDivision(parser, v, tok, ctx);
		} else {
			// tok.text() == "/"sv
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			const double val = performMultiplicationAndDivision(parser, v, tok, ctx);
			if (val == 0) throw Error(errLogic, "Division by zero!");
			left /= val;
		}
	}
	return left;
}

double ExpressionEvaluator::performSumAndSubtracting(tokenizer& parser, const PayloadValue& v, const NsContext& ctx) {
	token tok;
	double left = performMultiplicationAndDivision(parser, v, tok, ctx);
	tok = parser.peek_token(tokenizer::flags::treat_sign_as_token);
	while (tok.text() == "+"sv || tok.text() == "-"sv) {
		if rx_unlikely (state_ == StateArrayConcat) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '%s'", tok.text());
		}
		state_ = StateSumAndSubtract;
		if (tok.text() == "+"sv) {
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			left += performMultiplicationAndDivision(parser, v, tok, ctx);
		} else {
			// tok.text() == "-"sv
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			left -= performMultiplicationAndDivision(parser, v, tok, ctx);
		}
	}
	return left;
}

VariantArray ExpressionEvaluator::Evaluate(std::string_view expr, const PayloadValue& v, std::string_view forField, const NsContext& ctx) {
	arrayValues_.clear<false>();
	tokenizer parser(expr);
	forField_ = forField;
	state_ = None;
	const double expressionValue = performSumAndSubtracting(parser, v, ctx);
	return (state_ == StateArrayConcat) ? std::move(arrayValues_).MarkArray() : VariantArray{Variant(expressionValue)};
}

}  // namespace reindexer
