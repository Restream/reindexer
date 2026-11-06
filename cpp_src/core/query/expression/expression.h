#pragma once

#include "core/query/query.h"

#include <variant>

namespace reindexer::expressions {

using ExpressionValue = std::variant<std::string, VariantArray, functions::FunctionVariant, Query, ArithmeticExpression>;

ExpressionValue Deserialize(Serializer&, QueryFormat);

class [[nodiscard]] Field {
public:
	explicit Field(const std::string& fieldName) noexcept : fieldName_(fieldName) {}

	void Serialize(WrSerializer& ser) const;

private:
	const std::string& fieldName_;
};

class [[nodiscard]] Values {
public:
	explicit Values(const VariantArray& values) noexcept : values_(values) {}

	void Serialize(WrSerializer& ser) const;

private:
	const VariantArray& values_;
};

class [[nodiscard]] Function {
public:
	explicit Function(const functions::FunctionVariant& function) noexcept : function_(function) {}

	void Serialize(WrSerializer& ser) const;

private:
	const functions::FunctionVariant& function_;
};

class [[nodiscard]] SubQuery {
public:
	SubQuery(const Query& subQuery, QueryFormat queryFormat) noexcept : subQuery_(subQuery), queryFormat_(queryFormat) {}

	void Serialize(WrSerializer& ser) const;

private:
	const Query& subQuery_;
	const QueryFormat queryFormat_;
};

ExpressionType GetValueType(const ExpressionValue& value);
ExpressionType MakeExpressionType(std::string_view type);
std::string_view ExpressionTypeToString(ExpressionType type);

enum class [[nodiscard]] ValidationType { Full, WithoutSubqueries };
void ValidateExpressions(ExpressionType leftExpression, ExpressionType rightExpression, ValidationType type);

}  // namespace reindexer::expressions
