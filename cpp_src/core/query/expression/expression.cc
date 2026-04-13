#include "expression.h"
#include "core/function/function.h"
#include "estl/fast_hash_map.h"
#include "estl/fast_hash_set.h"

namespace reindexer::expressions {

ExpressionValue Expression::Deserialize(Serializer& ser) {
	const auto type{ser.GetVarUInt()};
	switch (type) {
		case ExpressionTypeField: {
			return std::string(ser.GetVString());
		}
		case ExpressionTypeValues: {
			VariantArray va;
			va.resize(ser.GetVarUInt());
			for (size_t i = 0; i < va.size(); ++i) {
				va[i] = ser.GetVariant();
			}
			return va;
		}
		case ExpressionTypeExpression: {
			return functions::Function::Deserialize(ser);
		}
		case ExpressionTypeSubQuery: {
			Serializer subQuery{ser.GetVString()};
			return Query::Deserialize(subQuery);
		}
		default:
			throw Error{errParams, "Error deserializing expression: type ({}) is not supported"};
	}
}

void Field::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(Type());
	ser.PutVString(fieldName_);
}

const std::string& Field::Get() const { return fieldName_; }
std::string Field::Dump() const { return fieldName_; }

void Values::Serialize(WrSerializer& ser) const {
	const auto& values{Get()};
	ser.PutVarUint(Type());
	ser.PutVarUint(values.size());
	for (const auto& v : values) {
		ser.PutVariant(v);
	}
}

const VariantArray& Values::Get() const { return values_; }
std::string Values::Dump() const { return Get().Dump(); }

void Function::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(Type());
	std::visit([&ser](const auto& f) { f.Serialize(ser); }, Get());
}

const functions::FunctionVariant& Function::Get() const { return function_; }

std::string Function::Dump() const {
	return std::visit([](const auto& f) { return f.ToString(); }, Get());
}

void SubQuery::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(Type());
	Get().Serialize(ser);
}

const Query& SubQuery::Get() const { return subQuery_; }
std::string SubQuery::Dump() const { return Get().GetSQL(); }

ExpressionType GetValueType(const ExpressionValue& value) {
	if (auto v = std::get_if<std::string>(&value); v) {
		return ExpressionTypeField;
	} else if (auto v = std::get_if<VariantArray>(&value); v) {
		return ExpressionTypeValues;
	} else if (auto v = std::get_if<functions::FunctionVariant>(&value); v) {
		return ExpressionTypeExpression;
	} else if (auto v = std::get_if<Query>(&value); v) {
		return ExpressionTypeSubQuery;
	}
	throw Error{errParseBin, "Unsupported type of expression: {}", value.index()};
}

ExpressionType MakeExpressionType(std::string_view type) {
	if (type == "field") {
		return ExpressionTypeField;
	} else if (type == "values") {
		return ExpressionTypeValues;
	} else if (type == "expression") {
		return ExpressionTypeExpression;
	} else if (type == "subquery") {
		return ExpressionTypeSubQuery;
	}
	throw Error{errParams, "Unknown expression type: '{}'", type};
}

std::string_view ExpressionTypeToString(ExpressionType type) {
	switch (type) {
		case ExpressionTypeField:
			return "field";
		case ExpressionTypeValues:
			return "values";
		case ExpressionTypeExpression:
			return "expression";
		case ExpressionTypeSubQuery:
			return "subquery";
		default:
			throw Error{errParams, "Type ({}) is not supported"};
	}
}

void ValidateExpressions(ExpressionType leftExpression, ExpressionType rightExpression, ValidationType type) {
	static const fast_hash_map<ExpressionType, fast_hash_set<ExpressionType>> combinationsNoSubQueries = {
		{ExpressionTypeField, {ExpressionTypeValues, ExpressionTypeExpression, ExpressionTypeField}},
		{ExpressionTypeExpression, {ExpressionTypeValues}},
	};
	static const fast_hash_map<ExpressionType, fast_hash_set<ExpressionType>> combinationsAll = {
		{ExpressionTypeField, {ExpressionTypeValues, ExpressionTypeExpression, ExpressionTypeField, ExpressionTypeSubQuery}},
		{ExpressionTypeExpression, {ExpressionTypeValues, ExpressionTypeSubQuery}},
		{ExpressionTypeSubQuery, {ExpressionTypeValues, ExpressionTypeExpression}},
	};

	const auto& allowedCombinations{type == ValidationType::Full ? combinationsAll : combinationsNoSubQueries};
	auto allowedTypesToString = [](const auto& allowedTypes, auto toString) {
		std::string result;
		for (auto it = allowedTypes.begin(); it != allowedTypes.end(); ++it) {
			if (it != allowedTypes.begin()) {
				result += "\\";
			}
			result += toString(*it);
		}
		return result;
	};
	auto itLeftExpr = allowedCombinations.find(leftExpression);
	if (itLeftExpr == allowedCombinations.end()) {
		throw Error(errLogic, "Unsupported type of left expression '{}': {} is expected", ExpressionTypeToString(leftExpression),
					allowedTypesToString(allowedCombinations, [](const auto& it) { return ExpressionTypeToString(it.first); }));
	}
	if (itLeftExpr->second.count(rightExpression) == 0) {
		throw Error(errLogic, "Unsupported type of right expression '{}': {} is expected", ExpressionTypeToString(rightExpression),
					allowedTypesToString(itLeftExpr->second, [](ExpressionType type) { return ExpressionTypeToString(type); }));
	}
}

}  // namespace reindexer::expressions
