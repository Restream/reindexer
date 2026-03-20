#pragma once

#include "core/query/query.h"

namespace reindexer::expressions {

using ExpressionValue = std::variant<std::string, VariantArray, functions::FunctionVariant, Query>;

class [[nodiscard]] Expression {
public:
	explicit Expression(ExpressionType type) noexcept : type_(type) {}
	virtual ~Expression() = default;

	virtual void Serialize(WrSerializer&) const {}
	static ExpressionValue Deserialize(Serializer&);

	ExpressionType Type() const noexcept { return type_; }

private:
	ExpressionType type_;
};

class [[nodiscard]] Field : public Expression {
public:
	Field(const std::string& fieldName) : Expression(ExpressionTypeField), fieldName_(fieldName) {}
	~Field() override = default;

	void Serialize(WrSerializer& ser) const override;

	const std::string& Get() const;
	std::string Dump() const;

private:
	const std::string& fieldName_;
};

class [[nodiscard]] Values : public Expression {
public:
	Values(const VariantArray& values) : Expression(ExpressionTypeValues), values_(values) {}
	~Values() override = default;

	void Serialize(WrSerializer& ser) const override;
	const VariantArray& Get() const;
	std::string Dump() const;

private:
	const VariantArray& values_;
};

class [[nodiscard]] Function : public Expression {
public:
	Function(const functions::FunctionVariant& function) : Expression(ExpressionTypeExpression), function_(function) {}
	~Function() override = default;

	void Serialize(WrSerializer& ser) const override;
	const functions::FunctionVariant& Get() const;
	std::string Dump() const;

private:
	const functions::FunctionVariant& function_;
};

class [[nodiscard]] SubQuery : public Expression {
public:
	SubQuery(const Query& subQuery) : Expression(ExpressionTypeSubQuery), subQuery_(subQuery) {}
	~SubQuery() override = default;

	void Serialize(WrSerializer& ser) const override;
	const Query& Get() const;
	std::string Dump() const;

private:
	const Query& subQuery_;
};

ExpressionType GetValueType(const ExpressionValue& value);
ExpressionType MakeExpressionType(std::string_view type);
std::string_view ExpressionTypeToString(ExpressionType type);

enum class [[nodiscard]] ValidationType { Full, WithoutSubqueries };
void ValidateExpressions(ExpressionType leftExpression, ExpressionType rightExpression, ValidationType type);

}  // namespace reindexer::expressions
