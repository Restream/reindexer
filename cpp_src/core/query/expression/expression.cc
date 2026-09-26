#include "expression.h"
#include "core/function/function.h"
#include "core/query/query_impl.h"
#include "tools/serilize/serializer.h"

#include <algorithm>
#include <array>
#include <span>
#include <string>

namespace reindexer::expressions {

ExpressionValue Deserialize(Serializer& ser, QueryFormat queryFormat) {
	const auto type{ser.GetVarUInt()};
	switch (type) {
		case ExpressionTypeField: {
			return std::string(ser.GetVString());
		}
		case ExpressionTypeValues: {
			VariantArray va;
			const auto count = ser.GetVarUIntCount();
			va.reserve(static_cast<size_t>(count));
			for (auto left = count; left > 0; --left) {
				va.emplace_back(ser.GetVariant().EnsureHold());
			}
			return va;
		}
		case ExpressionTypeExpression: {
			return functions::Function::Deserialize(ser);
		}
		case ExpressionTypeArithmetic:
			return ArithmeticExpression{ser.GetVString()};
		case ExpressionTypeSubQuery: {
			Serializer subQuery{ser.GetVString()};
			return QueryImpl::Deserialize<Query>(subQuery, queryFormat);
		}
		default:
			throw Error{errParseBin, "Error deserializing expression: type ({}) is not supported", type};
	}
}

void Field::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(ExpressionTypeField);
	ser.PutVString(fieldName_);
}

void Values::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(ExpressionTypeValues);
	ser.PutVarUint(values_.size());
	for (const auto& v : values_) {
		ser.PutVariant(v);
	}
}

void Function::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(ExpressionTypeExpression);
	std::visit([&ser](const auto& f) { f.Serialize(ser); }, function_);
}

void SubQuery::Serialize(WrSerializer& ser) const {
	ser.PutVarUint(ExpressionTypeSubQuery);
	{
		const auto sizePosSaver = ser.StartVString();
		Impl(subQuery_).Serialize(ser, Normal, queryFormat_);
	}
}

ExpressionType GetValueType(const ExpressionValue& value) {
	if (auto v = std::get_if<std::string>(&value); v) {
		return ExpressionTypeField;
	} else if (auto v = std::get_if<VariantArray>(&value); v) {
		return ExpressionTypeValues;
	} else if (auto v = std::get_if<functions::FunctionVariant>(&value); v) {
		return ExpressionTypeExpression;
	} else if (auto v = std::get_if<Query>(&value); v) {
		return ExpressionTypeSubQuery;
	} else if (auto v = std::get_if<ArithmeticExpression>(&value); v) {
		return ExpressionTypeArithmetic;
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
		case ExpressionTypeArithmetic:
			return "expression";
		case ExpressionTypeSubQuery:
			return "subquery";
		default:
			throw Error{errParams, "Type ({}) is not supported", int(type)};
	}
}

void ValidateExpressions(ExpressionType leftExpression, ExpressionType rightExpression, ValidationType type) {
	const bool full = type == ValidationType::Full;
	std::span<const ExpressionType> allowed;
	switch (leftExpression) {
		case ExpressionTypeField: {
			// Order is part of the DSL error text.
			static constexpr std::array kNoSub{ExpressionTypeField, ExpressionTypeValues, ExpressionTypeExpression};
			static constexpr std::array kFull{ExpressionTypeField, ExpressionTypeValues, ExpressionTypeExpression, ExpressionTypeSubQuery,
											  ExpressionTypeArithmetic};
			allowed = full ? std::span<const ExpressionType>{kFull} : std::span<const ExpressionType>{kNoSub};
			break;
		}
		case ExpressionTypeExpression: {
			static constexpr std::array kNoSub{ExpressionTypeValues, ExpressionTypeField, ExpressionTypeExpression};
			static constexpr std::array kFull{ExpressionTypeValues, ExpressionTypeSubQuery};
			allowed = full ? std::span<const ExpressionType>{kFull} : std::span<const ExpressionType>{kNoSub};
			break;
		}
		case ExpressionTypeSubQuery: {
			static constexpr std::array kFull{ExpressionTypeExpression, ExpressionTypeValues};
			if (!full) {
				break;
			}
			allowed = kFull;
			break;
		}
		case ExpressionTypeArithmetic: {
			static constexpr std::array kFull{ExpressionTypeValues, ExpressionTypeField, ExpressionTypeArithmetic};
			if (!full) {
				break;
			}
			allowed = kFull;
			break;
		}
		case ExpressionTypeValues:
		default:
			break;
	}

	auto typesToString = [](std::span<const ExpressionType> types) {
		std::string result;
		for (const ExpressionType type : types) {
			if (!result.empty()) {
				result += "\\";
			}
			result += ExpressionTypeToString(type);
		}
		return result;
	};

	if (allowed.empty()) {
		// Arithmetic is spelled "expression" and is not a separate left-side option in the error text.
		static constexpr std::array kFullLeft{ExpressionTypeField, ExpressionTypeExpression, ExpressionTypeSubQuery};
		static constexpr std::array kNoSubLeft{ExpressionTypeField, ExpressionTypeExpression};
		throw Error(errLogic, "Unsupported type of left expression '{}': {} is expected", ExpressionTypeToString(leftExpression),
					typesToString(full ? std::span<const ExpressionType>{kFullLeft} : std::span<const ExpressionType>{kNoSubLeft}));
	}
	if (std::ranges::find(allowed, rightExpression) == allowed.end()) {
		throw Error(errLogic, "Unsupported type of right expression '{}': {} is expected", ExpressionTypeToString(rightExpression),
					typesToString(allowed));
	}
}

}  // namespace reindexer::expressions
