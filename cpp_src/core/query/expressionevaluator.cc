#include "expressionevaluator.h"
#include "core/payload/payloadiface.h"
#include "core/selectfunc/functionexecutor.h"
#include "core/selectfunc/selectfunc.h"
#include "estl/tokenizer.h"

namespace reindexer {

ExpressionEvaluator::ExpressionEvaluator(const PayloadType& type, FunctionExecutor& func, const string& forField)
	: type_(type), functionExecutor_(func), forField_(forField) {}

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
		if (type_.FieldByName(tok.text(), field)) {
			KeyValueType type = type_.Field(field).Type();
			if (type_.Field(field).IsArray() || ((type != KeyValueInt) && (type != KeyValueInt64) && (type != KeyValueDouble)))
				throw Error(errLogic, "Only integral type non-array fields are supported in arithmetical expressions: %s", tok.text());
			VariantArray fieldValue;
			ConstPayload pv(type_, v);
			pv.Get(field, fieldValue);
			if (fieldValue.size() == 0) throw Error(errLogic, "Calculating value of an empty field is impossible: %s", tok.text());
			parser.next_token();
			return fieldValue.front().As<double>();
		} else {
			SelectFuncStruct funcData = SelectFuncParser().ParseFunction(parser, true);
			funcData.field = forField_;
			return functionExecutor_.Execute(funcData).As<double>();
		}
	} else {
		throw Error(errLogic, "Only integral type non-array fields are supported in arithmetical expressions");
	}
	return 0.0;
}

double ExpressionEvaluator::performMultiplicationAndDivision(tokenizer& parser, const PayloadValue& v) {
	double left = getPrimaryToken(parser, v);
	token tok = parser.peek_token(true, true);
	if (tok.text() == "*"_sv) {
		parser.next_token();
		left *= performMultiplicationAndDivision(parser, v);
	} else if (tok.text() == "/"_sv) {
		parser.next_token();
		double val = performMultiplicationAndDivision(parser, v);
		if (val == 0) throw Error(errLogic, "Division by zero!");
		left /= val;
	}
	return left;
}

double ExpressionEvaluator::performSumAndSubtracting(tokenizer& parser, const PayloadValue& v) {
	double left = performMultiplicationAndDivision(parser, v);
	token tok = parser.peek_token(true, true);
	if (tok.text() == "+"_sv) {
		parser.next_token(true, true);
		left += performMultiplicationAndDivision(parser, v);
	} else if (tok.text() == "-"_sv) {
		parser.next_token(true, true);
		left -= performMultiplicationAndDivision(parser, v);
	}
	return left;
}

Variant ExpressionEvaluator::Evaluate(tokenizer& parser, const PayloadValue& v) { return Variant(performSumAndSubtracting(parser, v)); }
Variant ExpressionEvaluator::Evaluate(const string_view& expr, const PayloadValue& v) {
	tokenizer parser(expr);
	return Evaluate(parser, v);
}

}  // namespace reindexer
