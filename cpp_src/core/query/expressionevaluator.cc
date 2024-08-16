#include "expressionevaluator.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/selectfunc/selectfuncparser.h"
#include "double-conversion/double-conversion.h"
#include "estl/tokenizer.h"

namespace reindexer {

using namespace std::string_view_literals;

namespace {
constexpr char kWrongFieldTypeError[] = "Only integral type non-array fields are supported in arithmetical expressions: %s";
constexpr char kScalarsInConcatenationError[] = "Unable to use scalar values in the arrays concatenation expressions: %s";

enum class Command : int {
	ArrayRemove = 1,
	ArrayRemoveOnce,
};
}  // namespace

void ExpressionEvaluator::captureArrayContent(tokenizer& parser) {
	arrayValues_.MarkArray();
	token tok = parser.next_token(tokenizer::flags::no_flags);
	if (tok.text() == "]"sv) {
		return;
	}
	for (;; tok = parser.next_token(tokenizer::flags::no_flags)) {
		if rx_unlikely (tok.text() == "]"sv) {
			throw Error(errParseSQL, "Expected field value, but found ']' in query, %s", parser.where());
		}
		arrayValues_.emplace_back(token2kv(tok, parser, CompositeAllowed::No, FieldAllowed::No));
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

ExpressionEvaluator::PrimaryToken ExpressionEvaluator::getPrimaryToken(tokenizer& parser, const PayloadValue& v, StringAllowed strAllowed,
																	   NonIntegralAllowed nonIntAllowed, token& outTok) {
	outTok = parser.next_token();
	if (outTok.text() == "("sv) {
		const double val = performSumAndSubtracting(parser, v);
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
			return handleTokenName(parser, v, nonIntAllowed, outTok);
		case TokenString:
			if (strAllowed == StringAllowed::Yes) {
				arrayValues_.MarkArray();
				arrayValues_.emplace_back(token2kv(outTok, parser, CompositeAllowed::No, FieldAllowed::No));
				return {.value = std::nullopt, .type = PrimaryToken::Type::Array};
			} else {
				throwUnexpectedTokenError(parser, outTok);
			}
		case TokenEnd:
		case TokenOp:
		case TokenSymbol:
		case TokenSign:
			break;
	}
	throw Error(errParams, "Unexpected token in expression: '%s'", outTok.text());
}

ExpressionEvaluator::PrimaryToken ExpressionEvaluator::handleTokenName(tokenizer& parser, const PayloadValue& v,
																	   NonIntegralAllowed nonIntAllowed, token& outTok) {
	int field = 0;
	VariantArray fieldValues;
	ConstPayload pv(type_, v);
	if (type_.FieldByName(outTok.text(), field)) {
		if (type_.Field(field).IsArray()) {
			pv.Get(field, fieldValues);
			arrayValues_.MarkArray();
			for (Variant& val : fieldValues) {
				arrayValues_.emplace_back(std::move(val));
			}
			return (state_ == StateArrayConcat || fieldValues.size() != 1)
					   ? PrimaryToken{.value = std::nullopt, .type = PrimaryToken::Type::Array}
					   : type_.Field(field).Type().EvaluateOneOf(
							 [this](OneOf<KeyValueType::Int, KeyValueType::Int64, KeyValueType::Double>) -> PrimaryToken {
								 return {.value = arrayValues_.back().As<double>(), .type = PrimaryToken::Type::Array};
							 },
							 [&, this](OneOf<KeyValueType::Bool, KeyValueType::String, KeyValueType::Uuid>) -> PrimaryToken {
								 if rx_unlikely (nonIntAllowed == NonIntegralAllowed::No && state_ != StateArrayConcat &&
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
					throw Error(errParams, "Calculating value of an empty field is impossible: '%s'", outTok.text());
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
	} else if rx_unlikely (outTok.text() == "array_remove"sv || outTok.text() == "array_remove_once"sv) {
		return {.value = double((outTok.text() == "array_remove"sv) ? Command::ArrayRemove : Command::ArrayRemoveOnce),
				.type = PrimaryToken::Type::Command};
	} else if rx_unlikely (outTok.text() == "true"sv || outTok.text() == "false"sv) {
		if rx_unlikely (nonIntAllowed == NonIntegralAllowed::No) {
			throwUnexpectedTokenError(parser, outTok);
		}
		arrayValues_.emplace_back(outTok.text() == "true"sv);
		return {.value = std::nullopt, .type = PrimaryToken::Type::Null};
	}

	pv.GetByJsonPath(outTok.text(), tagsMatcher_, fieldValues, KeyValueType::Undefined{});

	if (fieldValues.IsNullValue()) {
		return {.value = std::nullopt, .type = PrimaryToken::Type::Null};
	}

	const bool isArrayField = fieldValues.IsArrayValue();
	if (isArrayField) {
		for (Variant& val : fieldValues) {
			arrayValues_.emplace_back(std::move(val));
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
	} else if (parser.peek_token(tokenizer::flags::treat_sign_as_token).text() == "("sv) {
		SelectFuncStruct funcData = SelectFuncParser().ParseFunction(parser, true, outTok);
		funcData.field = std::string(forField_);
		return {.value = functionExecutor_.Execute(funcData).As<double>(), .type = PrimaryToken::Type::Scalar};
	}
	return {.value = std::nullopt, .type = PrimaryToken::Type::Null};
}

void ExpressionEvaluator::handleCommand(tokenizer& parser, const PayloadValue& v, const std::optional<double>& flag) {
	if (!flag.has_value()) {
		throw Error(errParams, "Could not recognize command");
	}
	auto cmd = Command(int(flag.value()));
	if ((cmd != Command::ArrayRemove) && (cmd != Command::ArrayRemoveOnce)) {
		throw Error(errParams, "Unexpected command detected");
	}

	token tok = parser.next_token();
	if rx_unlikely (tok.text() != "("sv) {
		throw Error(errParams, "Expected '(' after command name, not '%s'", tok.text());
	}

	// parse field name and read values from field
	VariantArray resultArr;

	token valueToken;
	auto trr = getPrimaryToken(parser, v, StringAllowed::No, NonIntegralAllowed::Yes, valueToken);
	if rx_unlikely (trr.type != PrimaryToken::Type::Null) {
		if rx_unlikely (trr.type != PrimaryToken::Type::Array) {
			throw Error(errParams, "Only an array field is expected as first parameter of command 'array_remove_once/array_remove'");
		}

		// move member state to separate variable
		resultArr = std::move(arrayValues_);
		arrayValues_.Clear();
	}

	tok = parser.next_token();
	if rx_unlikely (tok.text() != ","sv) {
		throw Error(errParams, "Expected ',' after field parameter, not '%s'", tok.text());
	}

	// parse list of delete items
	auto val = getPrimaryToken(parser, v, StringAllowed::Yes, NonIntegralAllowed::Yes, valueToken);
	if rx_unlikely (val.type != PrimaryToken::Type::Null) {
		if ((val.type != PrimaryToken::Type::Array) && (val.type != PrimaryToken::Type::Scalar)) {
			throw Error(errParams, "Expecting array or scalar as command parameter: '%s'", valueToken.text());
		} else if ((val.type == PrimaryToken::Type::Scalar) && val.value.has_value()) {
			arrayValues_.emplace_back(Variant(val.value.value()));
		}
	}

	tok = parser.next_token();
	if rx_unlikely (tok.text() != ")"sv) {
		throw Error(errParams, "Expected ')' after command name and params, not '%s'", tok.text());
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
				values.erase(it);
			}
		} else {
			// remove elements from array
			values.erase(std::remove_if(values.begin(), values.end(),
										[&item](const auto& elem) {
											return item.RelaxCompare<WithString::Yes, NotComparable::Return>(elem) == ComparationResult::Eq;
										}),
						 values.end());
		}
	}
	// move results array to class member
	arrayValues_ = std::move(resultArr);
}

double ExpressionEvaluator::performArrayConcatenation(tokenizer& parser, const PayloadValue& v, token& tok) {
	token valueToken;
	auto left = getPrimaryToken(parser, v, StringAllowed::No, NonIntegralAllowed::No, valueToken);
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
		case PrimaryToken::Type::Command:
			handleCommand(parser, v, left.value);

			// update state
			left.value.reset();
			state_ = StateArrayConcat;
			tok = parser.peek_token();
			break;
	}

	while (tok.text() == "|"sv) {
		parser.next_token();
		tok = parser.next_token();
		if rx_unlikely (tok.text() != "|"sv) {
			throw Error(errParams, "Expected '|', not '%s'", tok.text());
		}
		if rx_unlikely (state_ != StateArrayConcat && state_ != None) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '%s'", tok.text());
		}
		state_ = StateArrayConcat;
		auto right = getPrimaryToken(parser, v, StringAllowed::No, NonIntegralAllowed::No, valueToken);
		if rx_unlikely (right.type == PrimaryToken::Type::Scalar) {
			throw Error(errParams, kScalarsInConcatenationError, valueToken.text());
		}
		if rx_unlikely (right.type == PrimaryToken::Type::Command) {
			handleCommand(parser, v, right.value);
			right.value.reset();
		}
		assertrx_throw(!right.value.has_value());
		tok = parser.peek_token();
	}
	return left.value.has_value() ? left.value.value() : 0.0;
}

double ExpressionEvaluator::performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& tok) {
	double left = performArrayConcatenation(parser, v, tok);
	tok = parser.peek_token(tokenizer::flags::treat_sign_as_token);
	while (tok.text() == "*"sv || tok.text() == "/"sv) {
		if rx_unlikely (state_ == StateArrayConcat) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '%s'", tok.text());
		}
		state_ = StateMultiplyAndDivide;
		if (tok.text() == "*"sv) {
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			left *= performMultiplicationAndDivision(parser, v, tok);
		} else {
			// tok.text() == "/"sv
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			const double val = performMultiplicationAndDivision(parser, v, tok);
			if (val == 0) {
				throw Error(errLogic, "Division by zero!");
			}
			left /= val;
		}
	}
	return left;
}

double ExpressionEvaluator::performSumAndSubtracting(tokenizer& parser, const PayloadValue& v) {
	token tok;
	double left = performMultiplicationAndDivision(parser, v, tok);
	tok = parser.peek_token(tokenizer::flags::treat_sign_as_token);
	while (tok.text() == "+"sv || tok.text() == "-"sv) {
		if rx_unlikely (state_ == StateArrayConcat) {
			throw Error(errParams, "Unable to mix arrays concatenation and arithmetic operations. Got token: '%s'", tok.text());
		}
		state_ = StateSumAndSubtract;
		if (tok.text() == "+"sv) {
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			left += performMultiplicationAndDivision(parser, v, tok);
		} else {
			// tok.text() == "-"sv
			parser.next_token(tokenizer::flags::treat_sign_as_token);
			left -= performMultiplicationAndDivision(parser, v, tok);
		}
	}
	return left;
}

VariantArray ExpressionEvaluator::Evaluate(std::string_view expr, const PayloadValue& v, std::string_view forField) {
	arrayValues_.Clear();
	tokenizer parser(expr);
	forField_ = forField;
	state_ = None;
	const double expressionValue = performSumAndSubtracting(parser, v);
	return (state_ == StateArrayConcat) ? std::move(arrayValues_).MarkArray() : VariantArray{Variant(expressionValue)};
}

}  // namespace reindexer
