#include "expression_evaluator.h"
#include "core/payload/payloadiface.h"
#include "estl/tokenizer.h"
#include "function_executor.h"
#include "function_parser.h"

namespace reindexer {

using namespace std::string_view_literals;

namespace {
constexpr char kWrongFieldTypeError[] = "Only integral type non-array fields are supported in arithmetical expressions: {}";
constexpr char kScalarsInConcatenationError[] = "Unable to use scalar values in the arrays concatenation expressions: {}";

enum class [[nodiscard]] Command : int {
	ArrayRemove = 1,
	ArrayRemoveOnce,
};
}  // namespace

void ExpressionEvaluator::captureArrayContent(Tokenizer& parser) {
	rx_unused = arrayValues_.MarkArray();
	Token tok = parser.NextToken(Tokenizer::Flags::NoFlags);
	if (tok.Text() == "]"sv) {
		return;
	}
	for (;; tok = parser.NextToken(Tokenizer::Flags::NoFlags)) {
		if rx_unlikely (tok.Text() == "]"sv) {
			throw Error(errParseSQL, "Expected field value, but found ']' in query, {}", parser.Where(tok));
		}
		arrayValues_.emplace_back(Token2kv(tok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True));
		tok = parser.NextToken(Tokenizer::Flags::NoFlags);
		if (tok.Text() == "]"sv) {
			break;
		}
		if rx_unlikely (tok.Text() != ","sv) {
			throw Error(errParseSQL, "Expected ']' or ',', but found '{}' in query, {}", tok.Text(), parser.Where(tok));
		}
	}
}

void ExpressionEvaluator::throwUnexpectedTokenError(Tokenizer& parser, const Token& outTok) {
	if (state_ == StateArrayConcat || parser.PeekToken(Tokenizer::Flags::TreatSignAsToken).Text() == "|"sv) {
		throw Error(errParams, kScalarsInConcatenationError, outTok.Text());
	}
	throw Error(errParams, kWrongFieldTypeError, outTok.Text());
}

ExpressionEvaluator::PrimaryToken ExpressionEvaluator::getPrimaryToken(Tokenizer& parser, const PayloadValue& v, StringAllowed strAllowed,
																	   NonIntegralAllowed nonIntAllowed, Token& outTok,
																	   const NsContext& ctx) {
	outTok = parser.NextToken();
	if (outTok.Text() == "("sv) {
		const double val = performSumAndSubtracting(parser, v, ctx);
		if rx_unlikely (parser.NextToken().Text() != ")"sv) {
			throw Error(errParams, "')' expected in arithmetical expression");
		}
		return {.value = Variant{val}, .type = PrimaryToken::Type::Scalar};
	} else if (outTok.Text() == "["sv) {
		captureArrayContent(parser);
		return {.value = Variant{}, .type = PrimaryToken::Type::Array};
	}
	switch (outTok.Type()) {
		case TokenNumber: {
			try {
				return {.value = GetVariantFromToken(outTok), .type = PrimaryToken::Type::Scalar};
			} catch (...) {
				throw Error(errParams, "Unable to convert '{}' to numeric value", outTok.Text());
			}
		}
		case TokenName:
			return handleTokenName(parser, v, nonIntAllowed, outTok, ctx);
		case TokenString:
			if (strAllowed == StringAllowed::Yes) {
				rx_unused = arrayValues_.MarkArray();
				arrayValues_.emplace_back(Token2kv(outTok, parser, CompositeAllowed_False, FieldAllowed_False, NullAllowed_True));
				return {.value = Variant{}, .type = PrimaryToken::Type::Array};
			} else {
				throwUnexpectedTokenError(parser, outTok);
			}
		case TokenEnd:
		case TokenOp:
		case TokenSymbol:
		case TokenSign:
			break;
	}
	throw Error(errParams, "Unexpected token in expression: '{}'", outTok.Text());
}

ExpressionEvaluator::PrimaryToken ExpressionEvaluator::handleTokenName(Tokenizer& parser, const PayloadValue& v,
																	   NonIntegralAllowed nonIntAllowed, Token& outTok,
																	   const NsContext& ctx) {
	int field = 0;
	VariantArray fieldValues;
	ConstPayload pv(type_, v);
	if (type_.FieldByName(outTok.Text(), field)) {
		if (type_.Field(field).IsArray()) {
			pv.Get(field, fieldValues);
			rx_unused = arrayValues_.MarkArray();
			for (Variant& val : fieldValues) {
				arrayValues_.emplace_back(std::move(val));
			}
			return (state_ == StateArrayConcat || fieldValues.size() != 1)
					   ? PrimaryToken{.value = Variant{}, .type = PrimaryToken::Type::Array}
					   : type_.Field(field).Type().EvaluateOneOf(
							 [this](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) -> PrimaryToken {
								 return {.value = arrayValues_.back(), .type = PrimaryToken::Type::Array};
							 },
							 [&, this](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid>) -> PrimaryToken {
								 if rx_unlikely (nonIntAllowed == NonIntegralAllowed::No && state_ != StateArrayConcat &&
												 parser.PeekToken(Tokenizer::Flags::TreatSignAsToken).Text() != "|"sv) {
									 throw Error(errParams, kWrongFieldTypeError, outTok.Text());
								 }
								 return {.value = Variant{}, .type = PrimaryToken::Type::Array};
							 },
							 [](KeyValueType::Float) noexcept -> PrimaryToken {
								 // Indexed field type can not be float
								 assertrx_throw(false);
								 abort();
							 },
							 [](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null,
									  KeyValueType::FloatVector>) noexcept -> PrimaryToken {
								 assertrx_throw(false);
								 abort();
							 });
		}
		return type_.Field(field).Type().EvaluateOneOf(
			[&](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) -> PrimaryToken {
				pv.Get(field, fieldValues);
				if rx_unlikely (fieldValues.empty()) {
					throw Error(errParams, "Calculating value of an empty field is impossible: '{}'", outTok.Text());
				}
				return {.value = fieldValues.front(), .type = PrimaryToken::Type::Scalar};
			},
			[&, this](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid, KeyValueType::FloatVector>) -> PrimaryToken {
				throwUnexpectedTokenError(parser, outTok);
			},
			[](KeyValueType::Float) noexcept -> PrimaryToken {
				// Indexed field type can not be float
				assertrx_throw(false);
				abort();
			},
			[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null>) -> PrimaryToken {
				assertrx_throw(false);
				abort();
			});
	} else if rx_unlikely (outTok.Text() == "array_remove"sv || outTok.Text() == "array_remove_once"sv) {
		return {.value = Variant{int(outTok.Text() == "array_remove"sv ? Command::ArrayRemove : Command::ArrayRemoveOnce)},
				.type = PrimaryToken::Type::Command};
	} else if rx_unlikely (outTok.Text() == "true"sv || outTok.Text() == "false"sv) {
		if rx_unlikely (nonIntAllowed == NonIntegralAllowed::No) {
			throwUnexpectedTokenError(parser, outTok);
		}
		arrayValues_.emplace_back(outTok.Text() == "true"sv);
		return {.value = Variant{}, .type = PrimaryToken::Type::Null};
	}

	pv.GetByJsonPath(outTok.Text(), tagsMatcher_, fieldValues, KeyValueType::Undefined{});

	if (fieldValues.IsNullValue()) {
		return {.value = Variant{}, .type = PrimaryToken::Type::Null};
	}

	const bool isArrayField = fieldValues.IsArrayValue();
	if (isArrayField) {
		for (Variant& val : fieldValues) {
			arrayValues_.emplace_back(std::move(val));
		}
		if ((state_ == StateArrayConcat) || (fieldValues.size() != 1)) {
			return {.value = Variant{}, .type = PrimaryToken::Type::Array};
		}
	}
	if (fieldValues.size() == 1) {
		const Variant* vptr = isArrayField ? &arrayValues_.back() : &fieldValues.front();
		return vptr->Type().EvaluateOneOf(
			[vptr, isArrayField](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double, KeyValueType::Float>) -> PrimaryToken {
				return {.value = *vptr, .type = isArrayField ? PrimaryToken::Type::Array : PrimaryToken::Type::Scalar};
			},
			[&, this](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid>) -> PrimaryToken {
				if (isArrayField) {
					return {.value = Variant{}, .type = PrimaryToken::Type::Array};
				}
				throwUnexpectedTokenError(parser, outTok);
			},
			[](OneOf<KeyValueType::Tuple, KeyValueType::Undefined, KeyValueType::Composite, KeyValueType::Null, KeyValueType::FloatVector>)
				-> PrimaryToken {
				assertrx_throw(0);
				abort();
			});
	} else if (parser.PeekToken(Tokenizer::Flags::TreatSignAsToken).Text() == "("sv) {
		auto parsedFunction = QueryFunctionParser::ParseFunction(parser, outTok);
		parsedFunction.field = std::string(forField_);
		return {.value = functionExecutor_.Execute(QueryFunction{std::move(parsedFunction)}, ctx), .type = PrimaryToken::Type::Scalar};
	}
	return {.value = Variant{}, .type = PrimaryToken::Type::Null};
}

void ExpressionEvaluator::handleCommand(Tokenizer& parser, const PayloadValue& v, const Variant& flag, const NsContext& ctx) {
	if (!flag.Type().Is<KeyValueType::Int>()) {
		throw Error(errParams, "Could not recognize command");
	}
	auto cmd = Command(flag.As<int>());
	if ((cmd != Command::ArrayRemove) && (cmd != Command::ArrayRemoveOnce)) {
		throw Error(errParams, "Unexpected command detected");
	}

	Token tok = parser.NextToken();
	if rx_unlikely (tok.Text() != "("sv) {
		throw Error(errParams, "Expected '(' after command name, not '{}'", tok.Text());
	}

	// parse field name and read values from field
	VariantArray resultArr;

	Token valueToken;
	auto trr = getPrimaryToken(parser, v, StringAllowed::No, NonIntegralAllowed::Yes, valueToken, ctx);
	if rx_unlikely (trr.type != PrimaryToken::Type::Null) {
		if rx_unlikely (trr.type != PrimaryToken::Type::Array) {
			throw Error(errParams, "Only an array field is expected as first parameter of command 'array_remove_once/array_remove'");
		}

		// move member state to separate variable
		resultArr = std::move(arrayValues_);
		arrayValues_.Clear();
	}

	tok = parser.NextToken();
	if rx_unlikely (tok.Text() != ","sv) {
		throw Error(errParams, "Expected ',' after field parameter, not '{}'", tok.Text());
	}

	// parse list of delete items
	auto val = getPrimaryToken(parser, v, StringAllowed::Yes, NonIntegralAllowed::Yes, valueToken, ctx);
	if rx_unlikely (val.type != PrimaryToken::Type::Null) {
		if ((val.type != PrimaryToken::Type::Array) && (val.type != PrimaryToken::Type::Scalar)) {
			throw Error(errParams, "Expecting array or scalar as command parameter: '{}'", valueToken.Text());
		} else if ((val.type == PrimaryToken::Type::Scalar) && !val.value.IsNullValue()) {
			arrayValues_.emplace_back(val.value);
		}
	}

	tok = parser.NextToken();
	if rx_unlikely (tok.Text() != ")"sv) {
		throw Error(errParams, "Expected ')' after command name and params, not '{}'", tok.Text());
	}

	// do command
	VariantArray& values = resultArr;
	for (const auto& item : arrayValues_) {
		if (cmd == Command::ArrayRemoveOnce) {
			// remove elements from array once
			auto it = std::find_if(values.begin(), values.end(), [&item](const auto& elem) {
				return item.RelaxCompare<WithString::Yes, NotComparable::Return>(elem) == ComparationResult::Eq;
			});
			if (it != values.end()) {
				rx_unused = values.erase(it);
			}
		} else {
			// remove elements from array
			rx_unused = values.erase(std::remove_if(values.begin(), values.end(),
													[&item](const auto& elem) {
														return item.RelaxCompare<WithString::Yes, NotComparable::Return>(elem) ==
															   ComparationResult::Eq;
													}),
									 values.end());
		}
	}
	// move results array to class member
	arrayValues_ = std::move(resultArr);
}

double ExpressionEvaluator::performArrayConcatenation(Tokenizer& parser, const PayloadValue& v, Token& tok, const NsContext& ctx) {
	Token valueToken;
	auto left = getPrimaryToken(parser, v, StringAllowed::No, NonIntegralAllowed::No, valueToken, ctx);
	tok = parser.PeekToken();
	switch (left.type) {
		case PrimaryToken::Type::Scalar:
			if rx_unlikely (tok.Text() == "|"sv) {
				throw Error(errParams, kScalarsInConcatenationError, valueToken.Text());
			}
			break;
		case PrimaryToken::Type::Array:
		case PrimaryToken::Type::Null:
			if rx_unlikely (left.value.IsNullValue() && tok.Text() != "|"sv) {
				throw Error(errParams, "Unable to use array and null values outside of the arrays concatenation");
			}
			break;
		case PrimaryToken::Type::Command:
			handleCommand(parser, v, left.value, ctx);

			// update state
			left.value = Variant{};
			state_ = StateArrayConcat;
			tok = parser.PeekToken();
			break;
	}

	while (tok.Text() == "|"sv) {
		parser.SkipToken();
		tok = parser.NextToken();
		if rx_unlikely (tok.Text() != "|"sv) {
			throw Error(errParams, "Expected '|', not '{}'", tok.Text());
		}
		if rx_unlikely (state_ != StateArrayConcat && state_ != None) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '{}'", tok.Text());
		}
		state_ = StateArrayConcat;
		auto right = getPrimaryToken(parser, v, StringAllowed::No, NonIntegralAllowed::No, valueToken, ctx);
		if rx_unlikely (right.type == PrimaryToken::Type::Scalar) {
			throw Error(errParams, kScalarsInConcatenationError, valueToken.Text());
		}
		if rx_unlikely (right.type == PrimaryToken::Type::Command) {
			handleCommand(parser, v, right.value, ctx);
			right.value = Variant{};
		}
		assertrx_throw(right.value.IsNullValue());
		tok = parser.PeekToken();
	}
	return left.value.IsNullValue() ? 0.0 : left.value.As<double>();
}

double ExpressionEvaluator::performMultiplicationAndDivision(Tokenizer& parser, const PayloadValue& v, Token& tok, const NsContext& ctx) {
	double left = performArrayConcatenation(parser, v, tok, ctx);
	tok = parser.PeekToken(Tokenizer::Flags::TreatSignAsToken);
	while (tok.Text() == "*"sv || tok.Text() == "/"sv) {
		if rx_unlikely (state_ == StateArrayConcat) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '{}'", tok.Text());
		}
		state_ = StateMultiplyAndDivide;
		if (tok.Text() == "*"sv) {
			parser.SkipToken(Tokenizer::Flags::TreatSignAsToken);
			left *= performMultiplicationAndDivision(parser, v, tok, ctx);
		} else {
			// tok.text() == "/"sv
			parser.SkipToken(Tokenizer::Flags::TreatSignAsToken);
			const double val = performMultiplicationAndDivision(parser, v, tok, ctx);
			if (val == 0) {
				throw Error(errLogic, "Division by zero!");
			}
			left /= val;
		}
	}
	return left;
}

double ExpressionEvaluator::performSumAndSubtracting(Tokenizer& parser, const PayloadValue& v, const NsContext& ctx) {
	Token tok;
	double left = performMultiplicationAndDivision(parser, v, tok, ctx);
	tok = parser.PeekToken(Tokenizer::Flags::TreatSignAsToken);
	while (tok.Text() == "+"sv || tok.Text() == "-"sv) {
		if rx_unlikely (state_ == StateArrayConcat) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '{}'", tok.Text());
		}
		state_ = StateSumAndSubtract;
		if (tok.Text() == "+"sv) {
			parser.SkipToken(Tokenizer::Flags::TreatSignAsToken);
			left += performMultiplicationAndDivision(parser, v, tok, ctx);
		} else {
			// tok.text() == "-"sv
			parser.SkipToken(Tokenizer::Flags::TreatSignAsToken);
			left -= performMultiplicationAndDivision(parser, v, tok, ctx);
		}
	}
	return left;
}

VariantArray ExpressionEvaluator::Evaluate(std::string_view expr, const PayloadValue& v, std::string_view forField, const NsContext& ctx) {
	arrayValues_.Clear();
	Tokenizer parser(expr);
	forField_ = forField;
	state_ = None;
	const double expressionValue = performSumAndSubtracting(parser, v, ctx);
	return (state_ == StateArrayConcat) ? std::move(arrayValues_).MarkArray() : VariantArray{Variant(expressionValue)};
}

}  // namespace reindexer
