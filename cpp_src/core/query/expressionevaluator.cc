#include "expressionevaluator.h"
#include "core/cjson/tagsmatcher.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/selectfunc/selectfunc.h"
#include "estl/tokenizer.h"

namespace reindexer {

const char* kWrongFieldTypeError = "Only integral type non-array fields are supported in arithmetical expressions: %s";

ExpressionEvaluator::ExpressionEvaluator(const PayloadType& type, TagsMatcher& tagsMatcher, FunctionExecutor& func, const string& forField)
	: type_(type), tagsMatcher_(tagsMatcher), functionExecutor_(func), forField_(forField) {}

double ExpressionEvaluator::getPrimaryToken(tokenizer& parser, const PayloadValue& v) {
	token tok = parser.peek_token(true, true);
	if (tok.text() == "("_sv) {
		parser.next_token();
		double val = performSumAndSubtracting(parser, v);
		if (parser.next_token().text() != ")"_sv) throw Error(errLogic, "')' expected in arithmetical expression");
		return val;
	} else if (tok.type == TokenNumber) {
		char* p = nullptr;
		parser.next_token();
		return strtod(tok.text().data(), &p);
	} else if (tok.type == TokenName) {
		int field = 0;
		VariantArray fieldValue;
		ConstPayload pv(type_, v);
		if (type_.FieldByName(tok.text(), field)) {
			KeyValueType type = type_.Field(field).Type();
			if (type_.Field(field).IsArray() || ((type != KeyValueInt) && (type != KeyValueInt64) && (type != KeyValueDouble)))
				throw Error(errLogic, kWrongFieldTypeError, tok.text());
			pv.Get(field, fieldValue);
			if (fieldValue.size() == 0) throw Error(errLogic, "Calculating value of an empty field is impossible: %s", tok.text());
			parser.next_token();
			return fieldValue.front().As<double>();
		} else {
			pv.GetByJsonPath(tok.text(), tagsMatcher_, fieldValue, KeyValueUndefined);
			if (fieldValue.size() > 0) {
				KeyValueType type = fieldValue.front().Type();
				if ((fieldValue.size() > 1) || ((type != KeyValueInt) && (type != KeyValueInt64) && (type != KeyValueDouble)))
					throw Error(errLogic, kWrongFieldTypeError, tok.text());
				parser.next_token();
				return fieldValue.front().As<double>();
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

double ExpressionEvaluator::performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v, token& tok) {
	double left = getPrimaryToken(parser, v);
	tok = parser.peek_token(true, true);
	while (tok.text() == "*"_sv || tok.text() == "/"_sv) {
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

Variant ExpressionEvaluator::Evaluate(tokenizer& parser, const PayloadValue& v) { return Variant(performSumAndSubtracting(parser, v)); }
Variant ExpressionEvaluator::Evaluate(const string_view& expr, const PayloadValue& v) {
	tokenizer parser(expr);
	return Evaluate(parser, v);
}

}  // namespace reindexer
