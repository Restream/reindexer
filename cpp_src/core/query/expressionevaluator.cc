#include "expressionevaluator.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/selectfunc/selectfunc.h"
#include "estl/tokenizer.h"

namespace reindexer {

const char* kWrongFieldTypeError = "Only integral type non-array fields are supported in arithmetical expressions: %s";

ExpressionEvaluator::ExpressionEvaluator(const PayloadType& type, TagsMatcher& tagsMatcher, FunctionExecutor& func)
	: type_(type), tagsMatcher_(tagsMatcher), functionExecutor_(func) {}

void ExpressionEvaluator::captureArrayContent(tokenizer& parser) {
	token tok = parser.next_token(false);
	for (;;) {
		tok = parser.next_token(false);
		if (tok.text() == "]"_sv) {
			if (arrayValues_.empty()) break;
			throw Error(errParseSQL, "Expected field value, but found ']' in query, %s", parser.where());
		}
		arrayValues_.emplace_back(token2kv(tok, parser, false));
		tok = parser.next_token();
		if (tok.text() == "]"_sv) break;
		if (tok.text() != ","_sv) throw Error(errParseSQL, "Expected ']' or ',', but found '%s' in query, %s", tok.text(), parser.where());
	};
}

double ExpressionEvaluator::getPrimaryToken(tokenizer& parser, const PayloadValue& v) {
	token tok = parser.peek_token(true, true);
	if (tok.text() == "("_sv) {
		parser.next_token();
		double val = performSumAndSubtracting(parser, v);
		if (parser.next_token().text() != ")"_sv) throw Error(errLogic, "')' expected in arithmetical expression");
		return val;
	} else if (tok.text() == "["_sv) {
		captureArrayContent(parser);
	} else if (tok.type == TokenNumber) {
		char* p = nullptr;
		parser.next_token();
		return strtod(tok.text().data(), &p);
	} else if (tok.type == TokenName) {
		int field = 0;
		VariantArray fieldValues;
		ConstPayload pv(type_, v);
		if (type_.FieldByName(tok.text(), field)) {
			KeyValueType type = type_.Field(field).Type();
			if (type_.Field(field).IsArray()) {
				pv.Get(field, fieldValues);
				for (const Variant& v : fieldValues) {
					arrayValues_.emplace_back(v);
				}
				parser.next_token();
				return 0.0;
			} else if (state_ == StateArrayConcat) {
				VariantArray vals;
				pv.GetByJsonPath(tok.text(), tagsMatcher_, vals, KeyValueUndefined);
				for (const Variant& v : vals) {
					arrayValues_.emplace_back(v);
				}
				parser.next_token();
				return 0.0;
			} else if ((type == KeyValueInt) || (type == KeyValueInt64) || (type == KeyValueDouble)) {
				pv.Get(field, fieldValues);
				if (fieldValues.empty()) throw Error(errLogic, "Calculating value of an empty field is impossible: %s", tok.text());
				parser.next_token();
				return fieldValues.front().As<double>();
			} else {
				throw Error(errLogic, kWrongFieldTypeError, tok.text());
			}
		} else {
			pv.GetByJsonPath(tok.text(), tagsMatcher_, fieldValues, KeyValueUndefined);
			if (fieldValues.size() > 0) {
				KeyValueType type = fieldValues.front().Type();
				if ((fieldValues.size() > 1) || (state_ == StateArrayConcat)) {
					for (const Variant& v : fieldValues) {
						arrayValues_.emplace_back(v);
					}
					parser.next_token();
					return 0.0;
				} else if ((type == KeyValueInt) || (type == KeyValueInt64) || (type == KeyValueDouble)) {
					parser.next_token();
					return fieldValues.front().As<double>();
				} else {
					throw Error(errLogic, kWrongFieldTypeError, tok.text());
				}
			} else {
				SelectFuncStruct funcData = SelectFuncParser().ParseFunction(parser, true);
				funcData.field = forField_;
				return functionExecutor_.Execute(funcData).As<double>();
			}
		}
	} else {
		throw Error(errLogic, "Only integral type non-array fields are supported in arithmetical expressions");
	}
	return 0.0;
}

double ExpressionEvaluator::performArrayConcatenation(tokenizer& parser, const PayloadValue& v, token& tok) {
	double left = getPrimaryToken(parser, v);
	tok = parser.peek_token();
	while (tok.text() == "|"_sv) {
		parser.next_token();
		tok = parser.next_token();
		if (tok.text() != "|") throw Error(errLogic, "Expected '|', not %s", tok.text());
		state_ = StateArrayConcat;
		getPrimaryToken(parser, v);
		tok = parser.peek_token();
	}
	return left;
}

double ExpressionEvaluator::performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& tok) {
	double left = performArrayConcatenation(parser, v, tok);
	tok = parser.peek_token(true, true);
	while (tok.text() == "*"_sv || tok.text() == "/"_sv) {
		state_ = StateMultiplyAndDivide;
		if (tok.text() == "*"_sv) {
			parser.next_token();
			left *= performMultiplicationAndDivision(parser, v, tok);
		} else if (tok.text() == "/"_sv) {
			parser.next_token();
			double val = performMultiplicationAndDivision(parser, v, tok);
			if (val == 0) throw Error(errLogic, "Division by zero!");
			left /= val;
		}
	}
	return left;
}

double ExpressionEvaluator::performSumAndSubtracting(tokenizer& parser, const PayloadValue& v) {
	token tok;
	double left = performMultiplicationAndDivision(parser, v, tok);
	tok = parser.peek_token(true, true);
	while (tok.text() == "+"_sv || tok.text() == "-"_sv) {
		state_ = StateSumAndSubtract;
		if (tok.text() == "+"_sv) {
			parser.next_token(true, true);
			left += performMultiplicationAndDivision(parser, v, tok);
		} else if (tok.text() == "-"_sv) {
			parser.next_token(true, true);
			left -= performMultiplicationAndDivision(parser, v, tok);
		}
	}
	return left;
}

VariantArray ExpressionEvaluator::Evaluate(tokenizer& parser, const PayloadValue& v, string_view forField) {
	forField_ = string(forField);
	double expressionValue = performSumAndSubtracting(parser, v);
	if (arrayValues_.empty()) {
		return {Variant(expressionValue)};
	} else {
		arrayValues_.MarkArray();
		return arrayValues_;
	}
}

VariantArray ExpressionEvaluator::Evaluate(const string_view& expr, const PayloadValue& v, string_view forField) {
	arrayValues_.clear();
	tokenizer parser(expr);
	return Evaluate(parser, v, forField);
}

}  // namespace reindexer
