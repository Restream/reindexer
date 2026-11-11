#include <fmt/format.h>
#include <gmock/gmock.h>

#include <algorithm>
#include <limits>
#include <numeric>
#include <vector>

#include "tools/stringstools.h"

#include "arithmetic_expression_precedence_cases.h"
#include "cluster/config.h"
#include "cluster/sharding/sharding.h"
#include "core/function/expression_ast.h"
#include "core/function/function.h"
#include "core/system_ns_names.h"
#include "core/type_consts_helpers.h"
#include "expression_api.h"
#include "yaml-cpp/yaml.h"

namespace reindexer_tests {

using reindexer::CondTypeToStrShort;
using reindexer::Error;
using reindexer::IndexOpts;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::expressions::ArithmeticExpression;

static std::string LeftSubqueryConditionError(CondType cond, std::string_view rightOperand) {
	return fmt::format("Condition {} is not supported with subquery on the left side and {} on the right side", CondTypeToStrShort(cond),
					   rightOperand);
}

TEST_F(ExpressionApi, RejectsUpdateConstructs) {
	EXPECT_ANY_THROW(std::ignore = ArithmeticExpression("1)"));
	EXPECT_ANY_THROW(std::ignore = ArithmeticExpression("1 unexpected"));
	constexpr std::string_view kUnsupported[]{"1||2", "serial()", "'x'", "array_remove(a,1)", "1+false"};
	for (const auto expr : kUnsupported) {
		EXPECT_ANY_THROW(std::ignore = ArithmeticExpression(expr)) << expr;
	}
	for (const auto expr : {"[1,2]", "flat_array_len([1,2])", "now([1,2])"}) {
		try {
			std::ignore = ArithmeticExpression(expr);
			FAIL() << "expected parse error for " << expr;
		} catch (const Error& err) {
			EXPECT_EQ(err.code(), errParams) << expr << ": " << err.what();
			EXPECT_THAT(err.what(), ::testing::HasSubstr("Unsupported construct in WHERE arithmetic expression: '['"));
		}
	}
}

TEST_F(ExpressionApi, ParsesPythonStyleArrayLiterals) {
	constexpr std::string_view kExprs[]{
		"array_remove(a, [-1])", "array_remove(a, [False])", "array_remove(a, [True, false])", "[False]||a", "[-2918]||a",
		"a||[-1.5, +2, True]"};
	for (const auto expr : kExprs) {
		EXPECT_NO_THROW(std::ignore = reindexer::ExpressionAst::Parse(expr, false)) << expr;
	}
	{
		const auto ast = reindexer::ExpressionAst::Parse("array_remove(a, [-1])", false);
		ASSERT_TRUE(ast.Root());
		EXPECT_EQ(ast.Root()->Type(), reindexer::ExprNodeType::Function);
		EXPECT_TRUE(static_cast<const reindexer::ExprFunction*>(ast.Root())->ReturnsArray());
		EXPECT_EQ(static_cast<const reindexer::ExprFunction*>(ast.Root())->kind, reindexer::ExprFunction::Kind::ArrayRemove);
	}
	{
		const auto ast = reindexer::ExpressionAst::Parse("array_remove_once(a, [-1])", false);
		ASSERT_TRUE(ast.Root());
		EXPECT_EQ(ast.Root()->Type(), reindexer::ExprNodeType::Function);
		EXPECT_TRUE(static_cast<const reindexer::ExprFunction*>(ast.Root())->ReturnsArray());
		EXPECT_EQ(static_cast<const reindexer::ExprFunction*>(ast.Root())->kind, reindexer::ExprFunction::Kind::ArrayRemoveOnce);
	}
}

TEST_F(ExpressionApi, InvalidConditions) {
	EXPECT_THROW(std::ignore = Query(default_namespace)
								   .Where(ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondRange,
										  ArithmeticExpression(std::string(kFieldNameAge) + "*3")),
				 Error);
	for (const auto cond : {CondLike, CondAny, CondEmpty, CondDWithin, CondKnn}) {
		EXPECT_THROW(
			std::ignore = Query(default_namespace).Where(ArithmeticExpression(std::string(kFieldNameAge) + "*2"), cond, VariantArray{}),
			Error)
			<< CondTypeToStrShort(cond);
	}
}

TEST_F(ExpressionApi, OrderedConditionsAcceptAnyValueCount) {
	const auto expr = std::string(kFieldNameAge) + "*2";
	// Ordered comparisons match any of the values, so an empty list and several values are both legal.
	for (const auto cond : {CondGt, CondGe, CondLt, CondLe}) {
		EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), cond, VariantArray{}))
			<< CondTypeToStrShort(cond);
		EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), cond, VariantArray::Create(1, 2, 3)))
			<< CondTypeToStrShort(cond);
		EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), cond, VariantArray{Variant{}}))
			<< CondTypeToStrShort(cond);
		EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), cond, VariantArray::Create(1)))
			<< CondTypeToStrShort(cond);
	}
	// Without literal values on the right side there is nothing to count; the condition does not matter
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondGt, std::string(kFieldNameYear)));
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondGt, ArithmeticExpression(kFieldNameYear)));
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(kFieldNameYear, CondGt, ArithmeticExpression(expr)));

	// = and IN keep the plain-field rule: an empty list and several values are both legal
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondEq, VariantArray{}));
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondEq, VariantArray::Create(1, 2)));
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondSet, VariantArray{}));
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondSet, VariantArray::Create(1, 2, 3)));

	try {
		std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondRange, VariantArray::Create(1));
		FAIL() << "RANGE with one value";
	} catch (const Error& err) {
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_THAT(err.what(), ::testing::HasSubstr("RANGE"));
		EXPECT_THAT(err.what(), ::testing::HasSubstr("requires exactly two literal values"));
	}
	try {
		std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondRange, VariantArray{Variant{}, Variant{1}});
		FAIL() << "RANGE with null";
	} catch (const Error& err) {
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_THAT(err.what(), ::testing::HasSubstr("RANGE"));
		EXPECT_THAT(err.what(), ::testing::HasSubstr("can't have null argument"));
	}
	EXPECT_NO_THROW(std::ignore = Query(default_namespace).Where(ArithmeticExpression(expr), CondRange, VariantArray::Create(1, 2)));
}

TEST_F(ExpressionApi, ExpressionRequiresValue) {
	const auto filters = {
		R"("left_expression":{"type":"field"},"right_expression":{"type":"values","value":[1]})",
		R"("left_expression":{"type":"expression"},"right_expression":{"type":"values","value":[1]})",
		R"("left_expression":{"type":"field","value":"age"},"right_expression":{"type":"expression"})",
	};
	for (const auto filter : filters) {
		const auto dsl = fmt::format(R"({{"namespace":"{}","filters":[{{"cond":"eq",{}}}]}})", default_namespace, filter);
		try {
			std::ignore = Query::FromJSON(dsl);
			FAIL() << dsl;
		} catch (const Error& err) {
			EXPECT_EQ(err.code(), errParseDSL) << err.what();
			EXPECT_THAT(err.what(), ::testing::HasSubstr("'value' was not found")) << err.what();
		}
	}
}

TEST_F(ExpressionApi, SubqueryExpressionRequiresValue) {
	const auto subquery = fmt::format(R"({{"namespace":"{}"}})", default_namespace);
	const auto missingLeft =
		fmt::format(R"({{"namespace":"{}","filters":[{{"cond":"eq","left_expression":{{"type":"field"}},"subquery":{}}}]}})",
					default_namespace, subquery);
	const auto missingRight =
		fmt::format(R"({{"namespace":"{}","filters":[{{"cond":"eq","right_expression":{{"type":"values"}},"subquery":{}}}]}})",
					default_namespace, subquery);
	for (const auto& dsl : {missingLeft, missingRight}) {
		try {
			std::ignore = Query::FromJSON(dsl);
			FAIL() << dsl;
		} catch (const Error& err) {
			EXPECT_EQ(err.code(), errParseDSL) << err.what();
			EXPECT_THAT(err.what(), ::testing::HasSubstr("'value' was not found")) << err.what();
		}
	}
}

TEST_F(ExpressionApi, RuntimeErrors) {
	InsertSampleItem(/*packagesCount=*/2);

	{
		QueryResults qr;
		const auto err = rt.reindexer->Select(Query(default_namespace)
												  .Strict(StrictModeNone)
												  .Where(ArithmeticExpression("missing_field+1"), CondEq, VariantArray{Variant{0}}),
											  qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
	{
		const auto err = SelectError(Query(default_namespace)
										 .Strict(StrictModeNone)
										 .Where(ArithmeticExpression("missing_field+1"), CondEq, ArithmeticExpression("1/0")));
		EXPECT_EQ(err.code(), errLogic) << err.what();
	}

	{
		QueryResults qr;
		auto err = rt.reindexer->Select(
			Query(default_namespace).Strict(StrictModeNone).Where(ArithmeticExpression("1/0"), CondEq, VariantArray{Variant{0}}), qr);
		EXPECT_EQ(err.code(), errLogic) << err.what();
	}

	QueryResults qr;
	auto err = rt.reindexer->Select(
		Query(default_namespace).Strict(StrictModeNames).Where(ArithmeticExpression("missing_field+1"), CondEq, VariantArray{Variant{0}}),
		qr);
	EXPECT_EQ(err.code(), errStrictMode) << err.what();
}

TEST_F(ExpressionApi, ArrayFieldIsNeverAcceptedAsScalar) {
	for (const size_t packagesCount : {size_t{0}, size_t{1}, size_t{2}}) {
		InsertSampleItem(packagesCount);
		const auto expressionErr = SelectError(
			Query(default_namespace).Where(ArithmeticExpression(std::string(kFieldNamePackages) + "+1"), CondGt, VariantArray{Variant{0}}));
		EXPECT_EQ(expressionErr.code(), errParams) << "packagesCount=" << packagesCount << ": " << expressionErr.what();
		EXPECT_THAT(expressionErr.what(), ::testing::HasSubstr("Only integral type")) << "packagesCount=" << packagesCount;
	}
}

TEST_F(ExpressionApi, IntegerOverflowFailsSelect) {
	InsertSampleItem();

	for (const std::string_view expression : {"9223372036854775807+1", "9223372036854775807*1000", "-9223372036854775807-2",
											  "(-9223372036854775807-1)*-1", "-(-9223372036854775807-1)"}) {
		const auto err = SelectError(Query(default_namespace).Where(ArithmeticExpression(expression), CondEq, VariantArray{Variant{0}}));
		EXPECT_EQ(err.code(), errLogic) << expression << ": " << err.what();
		EXPECT_THAT(err.what(), ::testing::HasSubstr("Integer overflow")) << expression;
	}
}

TEST_F(ExpressionApi, RowIndependentExpressionErrorsAreRaisedOnSelect) {
	InsertSampleItem();
	const auto ids = [&] { return Query(default_namespace).Select(kFieldNameId); };
	const std::tuple<std::string_view, ErrorCode, std::string_view> constantErrors[]{
		{"1/0", errLogic, "Division by zero"},
		{"9223372036854775807+1", errLogic, "Integer overflow"},
		{"now(nsec)*1000000000000", errLogic, "Integer overflow"},
	};
	for (const auto& [expression, code, message] : constantErrors) {
		const auto buildErr = BuildError([&] {
			return Query::FromSQL(
				fmt::format("SELECT * FROM {} WHERE {} > (SELECT id FROM {})", default_namespace, expression, default_namespace));
		});
		EXPECT_TRUE(buildErr.ok()) << expression << ": " << buildErr.what();
		for (const Query& q : {Query(default_namespace).Where(ArithmeticExpression(expression), CondGt, ids()),
							   Query(default_namespace).Where(ids(), CondLt, ArithmeticExpression(expression))}) {
			for (const Query& query : {q, Query::FromSQL(q.GetSQL()), Query::FromJSON(q.GetJSON())}) {
				const auto err = SelectError(query);
				EXPECT_EQ(err.code(), code) << query.GetSQL() << ": " << err.what();
				EXPECT_THAT(err.whatStr(), ::testing::HasSubstr(message)) << query.GetSQL();
			}
		}
	}
}

TEST_F(ExpressionApi, DivisionByZeroDoesNotDependOnNowPosition) {
	InsertSampleItem();
	for (const std::string_view expression : {"1/0+now()", "now()+1/0"}) {
		const std::string sql =
			fmt::format("SELECT * FROM {} WHERE (SELECT id FROM {}) > {}", default_namespace, default_namespace, expression);
		const Query expected =
			Query(default_namespace).Where(Query(default_namespace).Select(kFieldNameId), CondGt, ArithmeticExpression(expression));

		Query parsed;
		const auto parseErr = BuildError([&] { return parsed = Query::FromSQL(sql); });
		ASSERT_TRUE(parseErr.ok()) << sql << ": " << parseErr.what();
		EXPECT_EQ(parsed, expected) << sql;
		EXPECT_EQ(parsed.GetSQL(), sql);

		for (const Query& query : {expected, parsed, Query::FromJSON(expected.GetJSON())}) {
			const auto err = SelectError(query);
			EXPECT_EQ(err.code(), errLogic) << query.GetSQL() << ": " << err.what();
			EXPECT_THAT(err.whatStr(), ::testing::HasSubstr("Division by zero")) << query.GetSQL();
		}
		QueryResults qr;
		const auto sqlErr = rt.reindexer->ExecSQL(sql, qr);
		EXPECT_EQ(sqlErr.code(), errLogic) << sql << ": " << sqlErr.what();
		EXPECT_THAT(sqlErr.whatStr(), ::testing::HasSubstr("Division by zero")) << sql;
	}
}

TEST_F(ExpressionApi, ProtectedConfigFields) {
	auto restrictedRx = rt.reindexer->WithContextParams(reindexer::milliseconds{0}, reindexer::NeedMaskingDSN_True, {}, {});

	for (const std::string_view expression : {"async_replication.nodes.dsn+1", "flat_array_len(sharding.shards.dsns)"}) {
		QueryResults qr;
		const auto err = restrictedRx.Select(
			Query(reindexer::kConfigNamespace).Where(ArithmeticExpression(expression), CondGt, VariantArray{Variant{0}}), qr);
		EXPECT_EQ(err.code(), errForbidden) << expression << ": " << err.what();
	}
}

TEST_F(ExpressionApi, Precedence) {
	InsertSampleItem();

	for (const auto& test : kArithmeticPrecedenceCases) {
		const auto expr = ExpandArithmeticPrecedenceExpr(test.expr, "8");
		EXPECT_EQ(SelectCount(Query(default_namespace).Where(ArithmeticExpression(expr), CondEq, VariantArray{Variant{test.expected}})), 1)
			<< expr;
	}
}

TEST_F(ExpressionApi, Dump) {
	const auto getSql = [](const Query& q) { return q.GetSQL(); };

	EXPECT_EQ(getSql(Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, VariantArray{Variant{20}})),
			  "SELECT * FROM test_namespace WHERE age*2 = 20");
	EXPECT_EQ(getSql(Query(default_namespace).Where(kFieldNameAge, CondEq, ArithmeticExpression("year-2000"))),
			  "SELECT * FROM test_namespace WHERE age = year-2000");
	EXPECT_EQ(getSql(Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, ArithmeticExpression("year-2000"))),
			  "SELECT * FROM test_namespace WHERE age*2 = year-2000");
	EXPECT_EQ(getSql(Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, kFieldNameYear)),
			  "SELECT * FROM test_namespace WHERE age*2 = year");
	EXPECT_EQ(getSql(Query(default_namespace).Where(ArithmeticExpression("age+1"), CondEq, VariantArray{Variant{std::string{"O'Reilly"}}})),
			  R"(SELECT * FROM test_namespace WHERE age+1 = 'O\'Reilly')");
	EXPECT_EQ(
		Query(default_namespace).Where(ArithmeticExpression("age+1"), CondEq, VariantArray{Variant{std::string{"O'Reilly"}}}).GetSQL(true),
		"SELECT * FROM test_namespace WHERE age+1 = ?");
	EXPECT_EQ(getSql(Query(default_namespace).Where(ArithmeticExpression("age+1"), CondEq, "index+field")),
			  R"(SELECT * FROM test_namespace WHERE age+1 = "index+field")");
	EXPECT_EQ(getSql(Query(default_namespace).Where(ArithmeticExpression("age+1"), CondEq, "field with space")),
			  R"(SELECT * FROM test_namespace WHERE age+1 = "field with space")");
}

TEST_F(ExpressionApi, DslRoundTrip) {
	const Query queries[] = {
		Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, VariantArray{Variant{20}}),
		Query(default_namespace).Where(kFieldNameAge, CondEq, ArithmeticExpression("year-2000")),
		Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, ArithmeticExpression("year-2000")),
		Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, kFieldNameYear),
		Query(default_namespace).Where(kFieldNameAge, CondEq, ArithmeticExpression("flat_array_len(packages)")),
		Query(default_namespace).Where(ArithmeticExpression("now()"), CondGt, VariantArray{Variant{0}}),
		Query(default_namespace).Where(ArithmeticExpression("flat_array_len(packages)"), CondEq, VariantArray{Variant{2}}),
		Query(default_namespace).Where(ArithmeticExpression("now()"), CondGt, kFieldNameAge),
		Query(default_namespace).Where(ArithmeticExpression("flat_array_len(packages)"), CondEq, kFieldNameAge),
		Query(default_namespace).Where(ArithmeticExpression("now()"), CondEq, ArithmeticExpression("now()")),
		Query(default_namespace)
			.Where(ArithmeticExpression("flat_array_len(packages)"), CondEq, ArithmeticExpression("flat_array_len(packages)")),
		Query(default_namespace).Where(ArithmeticExpression("flat_array_len(packages)"), CondLt, ArithmeticExpression("now()")),
		Query(default_namespace).Where(ArithmeticExpression("now()"), CondGe, ArithmeticExpression("flat_array_len(packages)")),
	};
	for (const auto& q : queries) {
		Query parsed;
		ASSERT_NO_THROW(parsed = Query::FromJSON(q.GetJSON())) << q.GetJSON();
		EXPECT_EQ(parsed, q) << q.GetJSON();
		EXPECT_EQ(parsed.GetJSON(), q.GetJSON());
		EXPECT_EQ(parsed.GetSQL(), q.GetSQL()) << q.GetJSON();
	}
}

TEST_F(ExpressionApi, FunctionDslFallsBackToArithmetic) {
	const auto now = reindexer::functions::Now{};
	const std::pair<Query, Query> queries[] = {
		{Query(default_namespace).Where(kFieldNameAge, CondGt, now),
		 Query(default_namespace).Where(kFieldNameAge, CondGt, ArithmeticExpression(now.ToString()))},
		{Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNamePackages), CondEq, VariantArray{Variant{2}}),
		 Query(default_namespace)
			 .Where(ArithmeticExpression("flat_array_len(" + std::string(kFieldNamePackages) + ")"), CondEq, VariantArray{Variant{2}})},
	};
	for (const auto& [functionQuery, arithmeticQuery] : queries) {
		Query parsed;
		ASSERT_NO_THROW(parsed = Query::FromJSON(functionQuery.GetJSON())) << functionQuery.GetJSON();
		EXPECT_EQ(parsed, arithmeticQuery) << functionQuery.GetJSON();
	}
}

TEST_F(ExpressionApi, ExpressionTagFallsBackToArithmetic) {
	const auto arithDsl = fmt::format(
		R"({{"namespace":"{}","filters":[{{"cond":"EQ","left_expression":{{"type":"expression","value":"age*2"}},"right_expression":{{"type":"values","value":20}}}}]}})",
		default_namespace);
	Query parsed;
	ASSERT_NO_THROW(parsed = Query::FromJSON(arithDsl)) << arithDsl;
	EXPECT_EQ(parsed, Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, VariantArray{Variant{20}}));

	const auto nowPlusDsl = fmt::format(
		R"({{"namespace":"{}","filters":[{{"cond":"GT","left_expression":{{"type":"expression","value":"now()+1"}},"right_expression":{{"type":"values","value":0}}}}]}})",
		default_namespace);
	ASSERT_NO_THROW(parsed = Query::FromJSON(nowPlusDsl)) << nowPlusDsl;
	EXPECT_EQ(parsed, Query(default_namespace).Where(ArithmeticExpression("now()+1"), CondGt, VariantArray{Variant{0}}));
	EXPECT_THAT(parsed.GetJSON(), ::testing::HasSubstr(R"("type":"expression","value":"now()+1")"));
}

TEST_F(ExpressionApi, DslEmptyFieldWithFunctionIsSymmetric) {
	InsertSampleItem();
	constexpr std::string_view kMissingField = "nonexist";

	const auto selectDsl = [&](std::string_view mode, std::string_view leftType, std::string_view leftValue, std::string_view rightType,
							   std::string_view rightValue) {
		const auto dsl = fmt::format(
			R"json({{"namespace":"{}","strict_mode":"{}","filters":[{{"cond":"EQ","left_expression":{{"type":"{}","value":"{}"}},"right_expression":{{"type":"{}","value":"{}"}}}}]}})json",
			default_namespace, mode, leftType, leftValue, rightType, rightValue);
		Query parsed;
		EXPECT_NO_THROW(parsed = Query::FromJSON(dsl)) << dsl;
		EXPECT_EQ(Impl(parsed).GetStrictMode(), reindexer::strictModeFromString(mode)) << dsl;
		QueryResults qr;
		const auto err = rt.reindexer->Select(parsed, qr);
		return std::tuple{err, qr.Count(), parsed.GetJSON()};
	};
	const auto expectSame = [&](std::string_view mode, auto left, auto right) {
		EXPECT_EQ(std::get<0>(left).code(), std::get<0>(right).code())
			<< "strict_mode=" << mode << " left=" << std::get<0>(left).what() << " right=" << std::get<0>(right).what()
			<< " jsonL=" << std::get<2>(left) << " jsonR=" << std::get<2>(right);
		EXPECT_STREQ(std::get<0>(left).what(), std::get<0>(right).what()) << "strict_mode=" << mode;
		if (std::get<0>(left).ok() && std::get<0>(right).ok()) {
			EXPECT_EQ(std::get<1>(left), std::get<1>(right)) << "strict_mode=" << mode;
		}
	};

	{
		const auto left = selectDsl("none", "field", kMissingField, "expression", "now()");
		const auto right = selectDsl("none", "expression", "now()", "field", kMissingField);
		expectSame("none", left, right);
		EXPECT_TRUE(std::get<0>(left).ok()) << std::get<0>(left).what();
		EXPECT_EQ(std::get<1>(left), 0);
	}
	for (const auto mode : {"names", "indexes"}) {
		const auto left = selectDsl(mode, "field", kMissingField, "expression", "now()");
		const auto right = selectDsl(mode, "expression", "now()", "field", kMissingField);
		expectSame(mode, left, right);
		EXPECT_EQ(std::get<0>(left).code(), errStrictMode) << std::get<0>(left).what() << " json=" << std::get<2>(left);
		EXPECT_THAT(std::get<0>(left).what(), ::testing::HasSubstr(kMissingField));
	}

	{
		EXPECT_EQ(SelectCount(
					  Query(default_namespace).Strict(StrictModeNone).Where(ArithmeticExpression("nonexist+100"), CondEq, VariantArray{0})),
				  0);

		const auto names = SelectError(
			Query(default_namespace).Strict(StrictModeNames).Where(ArithmeticExpression("nonexist+100"), CondEq, VariantArray{0}));
		EXPECT_EQ(names.code(), errStrictMode) << names.what();
		EXPECT_THAT(names.what(), ::testing::HasSubstr("existing fields only"));

		const auto indexes = SelectError(
			Query(default_namespace).Strict(StrictModeIndexes).Where(ArithmeticExpression("nonexist+100"), CondEq, VariantArray{0}));
		EXPECT_EQ(indexes.code(), errStrictMode) << indexes.what();
		EXPECT_THAT(indexes.what(), ::testing::HasSubstr("indexes only"));
	}
}

TEST_F(ExpressionApi, WhereFunctionComparisons) {
	constexpr std::string_view kNs = "where_fn_cmp_ns";
	rt.OpenNamespace(kNs);
	DefineNamespaceDataset(
		kNs,
		{IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{kFieldNameAge, "hash", "int", IndexOpts(), 0},
		 IndexDeclaration{"arr1", "hash", "int", IndexOpts().Array(), 0}, IndexDeclaration{"arr2", "hash", "int", IndexOpts().Array(), 0}});
	rt.UpsertJSON(kNs, R"json({"id":1,"age":2,"arr1":[10,20],"arr2":[30,40]})json");
	rt.UpsertJSON(kNs, R"json({"id":2,"age":1,"arr1":[10],"arr2":[10,20]})json");
	rt.UpsertJSON(kNs, R"json({"id":3,"age":3,"arr1":[1,2,3],"arr2":[1,2,3]})json");
	rt.UpsertJSON(kNs, R"json({"id":4,"age":5,"arr1":[],"arr2":[]})json");
	rt.UpsertJSON(kNs, R"json({"id":5,"age":2,"arr1":[1,2],"arr2":[1]})json");

	auto selectIds = [&](const Query& q) {
		QueryResults qr;
		const auto err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what() << " json=" << q.GetJSON();
		std::vector<int> ids;
		ids.reserve(qr.Count());
		for (auto it : qr) {
			ids.push_back(it.GetItem()[kFieldNameId].As<int>());
		}
		return ids;
	};
	auto expectIds = [&](const Query& q, std::vector<int> expected) {
		auto ids = selectIds(q);
		std::sort(ids.begin(), ids.end());
		std::sort(expected.begin(), expected.end());
		EXPECT_EQ(ids, expected) << q.GetJSON();
		Query parsed;
		ASSERT_NO_THROW(parsed = Query::FromJSON(q.GetJSON())) << q.GetJSON();
		auto parsedIds = selectIds(parsed);
		std::sort(parsedIds.begin(), parsedIds.end());
		EXPECT_EQ(parsedIds, expected) << parsed.GetJSON();
	};

	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondEq, ArithmeticExpression("flat_array_len(arr2)")),
			  {1, 3, 4});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondEq, ArithmeticExpression("flat_array_len(arr1)")),
			  {1, 2, 3, 4, 5});
	expectIds(Query(kNs).Where(kFieldNameAge, CondEq, ArithmeticExpression("flat_array_len(arr1)")), {1, 2, 3, 5});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondEq, kFieldNameAge), {1, 2, 3, 5});
	expectIds(Query(kNs).Where(kFieldNameAge, CondEq, ArithmeticExpression("flat_array_len(age)")), {2});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(age)"), CondEq, kFieldNameAge), {2});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondLt, ArithmeticExpression("flat_array_len(arr2)")), {2});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondGt, ArithmeticExpression("flat_array_len(arr2)")), {5});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondGt, VariantArray{Variant{1}}), {1, 3, 5});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondSet, VariantArray::Create(0, 2)), {1, 4, 5});
	expectIds(Query(kNs)
				  .Where(ArithmeticExpression("flat_array_len(arr1)"), CondEq, ArithmeticExpression("flat_array_len(arr2)"))
				  .Where(ArithmeticExpression("flat_array_len(arr1)"), CondGt, VariantArray{Variant{1}})
				  .Where(kFieldNameAge, CondSet, VariantArray::Create(2, 3)),
			  {1, 3});
	expectIds(Query(kNs).Where(ArithmeticExpression("now()"), CondGt, kFieldNameAge), {1, 2, 3, 4, 5});
	expectIds(Query(kNs).Where(kFieldNameAge, CondLt, ArithmeticExpression("now()")), {1, 2, 3, 4, 5});
	expectIds(Query(kNs).Where(ArithmeticExpression("now()"), CondEq, ArithmeticExpression("now()")), {1, 2, 3, 4, 5});
	expectIds(Query(kNs).Where(ArithmeticExpression("flat_array_len(arr1)"), CondLt, ArithmeticExpression("now()")), {1, 2, 3, 4, 5});
	expectIds(Query(kNs).Where(ArithmeticExpression("now()"), CondGt, "arr1"), {1, 2, 3, 5});
	expectIds(Query(kNs).Where("arr1", CondLt, ArithmeticExpression("now()")), {1, 2, 3, 5});
	expectIds(Query(kNs).Where(ArithmeticExpression("now()"), CondLt, "arr1"), {});
	expectIds(Query(kNs).Where("arr1", CondGt, ArithmeticExpression("now()")), {});
}

TEST_F(ExpressionApi, KeywordFieldNames) {
	constexpr std::string_view kNs = "where_arith_keywords_ns";
	rt.OpenNamespace(kNs);
	DefineNamespaceDataset(
		kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"true", "hash", "int", IndexOpts(), 0},
			  IndexDeclaration{"false", "hash", "int", IndexOpts(), 0}, IndexDeclaration{"null", "hash", "int", IndexOpts(), 0},
			  IndexDeclaration{"flag", "-", "bool", IndexOpts(), 0}});
	rt.UpsertJSON(kNs, R"json({"id":1,"true":10,"false":20,"null":30,"flag":true})json");

	const auto expectField = [](std::string_view expr) {
		const auto ast = reindexer::ExpressionAst::Parse(expr, true);
		ASSERT_TRUE(ast.Root()) << expr;
		EXPECT_EQ(ast.Root()->Type(), reindexer::ExprNodeType::Field) << expr;
		EXPECT_EQ(static_cast<const reindexer::ExprField*>(ast.Root())->name, ast.FieldNames().front()) << expr;
	};
	expectField("\"true\"");
	expectField("\"false\"");
	expectField("\"null\"");

	const auto expectUpdateLiteral = [](std::string_view expr, const Variant& value) {
		const auto ast = reindexer::ExpressionAst::Parse(expr, false);
		ASSERT_TRUE(ast.Root()) << expr;
		ASSERT_EQ(ast.Root()->Type(), reindexer::ExprNodeType::Number) << expr;
		EXPECT_TRUE(ast.FieldNames().empty()) << expr;
		EXPECT_EQ(static_cast<const reindexer::ExprNumber*>(ast.Root())->value, value) << expr;
	};
	expectUpdateLiteral("true", Variant{true});
	expectUpdateLiteral("false", Variant{false});
	expectUpdateLiteral("null", Variant{});

	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("\"true\"+1"), CondEq, VariantArray{Variant{11}})), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("1+\"true\""), CondEq, VariantArray{Variant{11}})), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("\"false\"*2"), CondEq, VariantArray{Variant{40}})), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("2*\"false\""), CondEq, VariantArray{Variant{40}})), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("\"null\"-1"), CondEq, VariantArray{Variant{29}})), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("31-\"null\""), CondEq, VariantArray{Variant{1}})), 1);

	for (const auto expr : {"true+1", "1+true", "false*2", "2*false", "null-1", "1-null", "'true'+1", "1+'false'", "flat_array_len(true)",
							"flat_array_len(1)", "flat_array_len(1.5)", "now(1)"}) {
		EXPECT_THROW(std::ignore = ArithmeticExpression(expr), Error) << expr;
	}
	EXPECT_NO_THROW(std::ignore = ArithmeticExpression("flat_array_len(\"true\")"));
	EXPECT_THROW(std::ignore = ArithmeticExpression("flat_array_len('true')"), Error);
	EXPECT_NO_THROW(std::ignore = ArithmeticExpression("now('nsec')"));
	EXPECT_THROW(std::ignore = ArithmeticExpression("now(\"nsec\")"), Error);
	EXPECT_THROW(std::ignore = ArithmeticExpression("\"now\"()"), Error);

	EXPECT_EQ(rt.ExecSQL(R"(SELECT * FROM where_arith_keywords_ns WHERE "true" = 10)").Count(), 1);
	EXPECT_EQ(rt.ExecSQL(R"(SELECT * FROM where_arith_keywords_ns WHERE 10 = "true")").Count(), 1);
	EXPECT_EQ(rt.ExecSQL(R"(SELECT * FROM where_arith_keywords_ns WHERE flag = true)").Count(), 1);
	EXPECT_EQ(rt.ExecSQL(R"(SELECT * FROM where_arith_keywords_ns WHERE true = flag)").Count(), 1);
	EXPECT_EQ(rt.ExecSQL(R"(SELECT * FROM where_arith_keywords_ns WHERE TRUE = flag)").Count(), 1);
	EXPECT_EQ(rt.ExecSQL(R"(SELECT * FROM where_arith_keywords_ns WHERE flag = false)").Count(), 0);
	EXPECT_EQ(rt.ExecSQL(R"(SELECT * FROM where_arith_keywords_ns WHERE false = flag)").Count(), 0);
}

TEST_F(ExpressionApi, UpdateSetDoesNotTreatKeywordsAsFields) {
	constexpr std::string_view kNs = "update_keyword_fields_ns";
	rt.OpenNamespace(kNs);
	DefineNamespaceDataset(
		kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"true", "hash", "int", IndexOpts(), 0},
			  IndexDeclaration{"where", "hash", "int", IndexOpts(), 0}, IndexDeclaration{"array_remove", "hash", "int", IndexOpts(), 0},
			  IndexDeclaration{"dst", "hash", "int", IndexOpts(), 0}, IndexDeclaration{"arr", "hash", "string", IndexOpts().Array(), 0},
			  IndexDeclaration{"flag", "-", "bool", IndexOpts(), 0}});
	rt.UpsertJSON(kNs, R"json({"id":1,"true":10,"where":20,"array_remove":30,"dst":0,"arr":[],"flag":false})json");

	auto selectAfter = [this, kNs](std::string_view sql) {
		EXPECT_EQ(rt.ExecSQL(sql).Count(), 1) << sql;
		auto qr = rt.ExecSQL(fmt::format("SELECT * FROM {} WHERE id = 1", kNs));
		EXPECT_EQ(qr.Count(), 1) << sql;
		return qr;
	};

	{
		const auto qr = selectAfter("UPDATE update_keyword_fields_ns SET flag = true WHERE id = 1");
		const auto item = qr.begin().GetItem(false);
		EXPECT_TRUE(item["flag"].As<bool>());
		EXPECT_EQ(item["true"].As<int>(), 10);
	}
	{
		const auto qr = selectAfter(R"(UPDATE update_keyword_fields_ns SET dst = "true"+1 WHERE id = 1)");
		EXPECT_EQ(qr.begin().GetItem(false)["dst"].As<int>(), 11);
	}
	{
		const auto qr = selectAfter(R"(UPDATE update_keyword_fields_ns SET dst = 1+"true" WHERE id = 1)");
		EXPECT_EQ(qr.begin().GetItem(false)["dst"].As<int>(), 11);
	}
	{
		const auto qr = selectAfter(R"(UPDATE update_keyword_fields_ns SET dst = 1+"where" WHERE id = 1)");
		EXPECT_EQ(qr.begin().GetItem(false)["dst"].As<int>(), 21);
	}
	{
		const auto qr = selectAfter(R"(UPDATE update_keyword_fields_ns SET dst = "array_remove"+1 WHERE id = 1)");
		EXPECT_EQ(qr.begin().GetItem(false)["dst"].As<int>(), 31);
	}
	{
		QueryResults qr;
		const auto err = rt.reindexer->ExecSQL("UPDATE update_keyword_fields_ns SET dst = true+1 WHERE id = 1", qr);
		EXPECT_FALSE(err.ok()) << err.what();
	}
	for (const auto sql : {R"(UPDATE update_keyword_fields_ns SET arr = ["true"] WHERE id = 1)",
						   R"(UPDATE update_keyword_fields_ns SET dst = "now"() WHERE id = 1)"}) {
		QueryResults qr;
		const auto err = rt.reindexer->ExecSQL(sql, qr);
		EXPECT_FALSE(err.ok()) << sql;
	}
	{
		const auto qr = selectAfter(R"(UPDATE update_keyword_fields_ns SET dst = "true" WHERE id = 1)");
		EXPECT_EQ(qr.begin().GetItem(false)["dst"].As<int>(), 10);
	}
}

TEST_F(ExpressionApi, Int64Precision) {
	InsertSampleItem();
	constexpr int64_t kLeft = 9007199254740993LL;  // 2^53 + 1, not exactly representable as double
	constexpr int64_t kSum = kLeft + 1;
	EXPECT_EQ(SelectCount(Query(default_namespace).Where(ArithmeticExpression("9007199254740993+1"), CondEq, VariantArray{Variant{kSum}})),
			  1);
}

TEST_F(ExpressionApi, RejectsNonNumericFields) {
	InsertSampleItem();
	for (const auto& expression : {std::string(kFieldNameName) + "+1", std::string(kFieldNameEnabled) + "+1"}) {
		const auto err = SelectError(Query(default_namespace).Where(ArithmeticExpression(expression), CondGt, VariantArray{Variant{0}}));
		EXPECT_EQ(err.code(), errParams) << expression << ": " << err.what();
		EXPECT_THAT(err.what(), ::testing::HasSubstr("Only integral type"));
	}
}

TEST_F(ExpressionApi, AcceptsIntMixedWithDouble) {
	InsertSampleItem();
	EXPECT_EQ(SelectCount(
				  Query(default_namespace)
					  .Where(ArithmeticExpression(std::string(kFieldNameAge) + "+" + kFieldNameRate), CondEq, VariantArray{Variant{11.5}})),
			  1);
}

TEST_F(ExpressionApi, MissingSparseFieldDoesNotMatch) {
	InsertSampleItem();
	{
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = 2;
		item[kFieldNameAge] = 10;
		item[kFieldNameYear] = 2010;
		item[kFieldNameName] = "name";
		item[kFieldNameRate] = 1.5;
		item[kFieldNameSparseAge] = 7;
		Upsert(default_namespace, item);
	}

	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondGt, VariantArray{Variant{0}})),
			  1);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondEq, VariantArray{Variant{8}})),
			  1);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondRange,
									 VariantArray::Create(Variant{0}, Variant{100}))),
			  1);
	EXPECT_EQ(SelectCount(
				  Query(default_namespace).Where(kFieldNameAge, CondAllSet, ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"))),
			  0);
	EXPECT_EQ(SelectCount(Query(default_namespace).Where(kFieldNameAge, CondAllSet, ArithmeticExpression(kFieldNameSparseAge))), 0);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Not()
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondGt, VariantArray{Variant{0}})),
			  1);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Not()
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondGt, VariantArray{Variant{100}})),
			  2);
	EXPECT_EQ(
		SelectCount(Query(default_namespace).Not().Where(ArithmeticExpression(kFieldNameSparseAge), CondEq, VariantArray{Variant{7}})), 1);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Not()
							  .OpenBracket()
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondEq, VariantArray{Variant{8}})
							  .CloseBracket()),
			  1);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Not()
							  .OpenBracket()
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondEq, VariantArray{Variant{100}})
							  .CloseBracket()),
			  2);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Not()
							  .OpenBracket()
							  .Not()
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondEq, VariantArray{Variant{8}})
							  .CloseBracket()),
			  1);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Not()
							  .OpenBracket()
							  .Not()
							  .Where(ArithmeticExpression(std::string(kFieldNameSparseAge) + "+1"), CondEq, VariantArray{Variant{100}})
							  .CloseBracket()),
			  0);
}

TEST_F(ExpressionApi, JsonNullNumericFieldDoesNotMatch) {
	constexpr std::string_view kNs = "json_null_arith_ns";
	rt.OpenNamespace(kNs);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	rt.UpsertJSON(kNs, R"json({"id":1,"val":null})json");
	rt.UpsertJSON(kNs, R"json({"id":2,"val":5})json");

	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("val+1"), CondEq, VariantArray{Variant{6}})), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Not().Where(ArithmeticExpression("val+1"), CondEq, VariantArray{Variant{6}})), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Not().Where(ArithmeticExpression("val+1"), CondEq, VariantArray{Variant{0}})), 2);
	EXPECT_EQ(SelectCount(Query(kNs).Not().Where("val", CondLt, ArithmeticExpression("now()"))), 1);
	EXPECT_EQ(SelectCount(Query(kNs).Not().Where(ArithmeticExpression("now()"), CondLt, "val")), 2);
}

TEST_F(ExpressionApi, SingleFieldCompositeIsRejected) {
	rt.AddIndex(default_namespace, reindexer::IndexDef{"comp_idx", {kFieldNameId}, "hash", "composite", IndexOpts()});
	InsertSampleItem();
	const auto err = SelectError(Query(default_namespace).Where(ArithmeticExpression("comp_idx+1"), CondEq, VariantArray{Variant{0}}));
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_THAT(err.what(), ::testing::HasSubstr("Only integral type non-array fields are supported"));

	const auto errTrivial = SelectError(Query(default_namespace).Where(ArithmeticExpression("comp_idx"), CondEq, VariantArray{Variant{0}}));
	EXPECT_EQ(errTrivial.code(), errParams) << errTrivial.what();
	EXPECT_THAT(errTrivial.what(), ::testing::HasSubstr("Only integral type non-array fields are supported"));
}

TEST_F(ExpressionApi, MultiFieldCompositeIsRejected) {
	rt.AddIndex(default_namespace, reindexer::IndexDef{"comp_idx", {kFieldNameId, kFieldNameAge}, "hash", "composite", IndexOpts()});
	InsertSampleItem();
	const auto err = SelectError(Query(default_namespace).Where(ArithmeticExpression("comp_idx+1"), CondEq, VariantArray{Variant{0}}));
	EXPECT_EQ(err.code(), errParams) << err.what();
	EXPECT_THAT(err.what(), ::testing::HasSubstr("Only integral type non-array fields are supported"));
}

TEST_F(ExpressionApi, FlatArrayLenArgsAndEmptyArray) {
	InsertSampleItem(/*packagesCount=*/0);

	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Where(ArithmeticExpression(std::string("flat_array_len(") + kFieldNamePackages + ')'), CondEq,
									 VariantArray{Variant{0}})),
			  1);
	EXPECT_EQ(SelectCount(Query(default_namespace)
							  .Where(ArithmeticExpression(std::string("flat_array_len(") + kFieldNameYear + ')'), CondEq,
									 VariantArray{Variant{1}})),
			  1);

	for (const auto expression : {"flat_array_len()", "flat_array_len(year,age)", "1 + flat_array_len()", "flat_array_len(year,age) + 1"}) {
		try {
			std::ignore = ArithmeticExpression(expression);
			FAIL() << "expected parse error for " << expression;
		} catch (const Error& err) {
			EXPECT_EQ(err.code(), errParams) << expression << ": " << err.what();
			EXPECT_THAT(err.what(), ::testing::HasSubstr("'flat_array_len' expects only 1 argument"));
		}
	}
}

TEST_F(ExpressionApi, NowUnits) {
	InsertSampleItem();
	for (const auto expr :
		 {"now()", "now(sec)", "now('sec')", "now(msec)", "now('msec')", "now(usec)", "now('usec')", "now(nsec)", "now('nsec')"}) {
		EXPECT_EQ(SelectCount(Query(default_namespace).Where(ArithmeticExpression(expr), CondGt, VariantArray{Variant{0}})), 1) << expr;
	}
	EXPECT_EQ(SelectCount(Query(default_namespace).Where(ArithmeticExpression("now(nsec)-now(nsec)"), CondEq, VariantArray{Variant{0}})),
			  1);

	for (const auto expression : {"now(sec, msec)", "1 + now(sec, msec)"}) {
		try {
			std::ignore = ArithmeticExpression(expression);
			FAIL() << "expected parse error for " << expression;
		} catch (const Error& err) {
			EXPECT_EQ(err.code(), errParams) << expression << ": " << err.what();
			EXPECT_THAT(err.what(), ::testing::HasSubstr("'now' expects 0 or 1 argument"));
		}
	}

	try {
		std::ignore = ArithmeticExpression("now(wrong)");
		FAIL() << "expected parse error for now(wrong)";
	} catch (const Error& err) {
		EXPECT_EQ(err.code(), errParams) << err.what();
		EXPECT_THAT(err.what(), ::testing::HasSubstr("Unknown time unit"));
	}
}

TEST_F(ExpressionApi, DivisionByZeroOnSomeItemsFailsQuery) {
	InsertSampleItem();
	{
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = 2;
		item[kFieldNameAge] = 0;
		item[kFieldNameYear] = 2010;
		item[kFieldNameName] = "name";
		item[kFieldNameRate] = 1.5;
		Upsert(default_namespace, item);
	}

	const auto err = SelectError(
		Query(default_namespace).Where(ArithmeticExpression(std::string("1/") + kFieldNameAge), CondGt, VariantArray{Variant{0}}));
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_THAT(err.what(), ::testing::HasSubstr("Division by zero"));
}

TEST_F(ExpressionApi, SqlParserParsesArithmeticWhere) {
	InsertSampleItem();

	const struct {
		std::string_view sql;
		Query expected;
		size_t count;
	} cases[]{
		{"SELECT * FROM test_namespace WHERE year*2 > 10",
		 Query(default_namespace).Where(ArithmeticExpression("year*2"), CondGt, VariantArray{Variant{10}}), 1},
		{"SELECT * FROM test_namespace WHERE age*2 = year-2000",
		 Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, ArithmeticExpression("year-2000")), 0},
		{"SELECT * FROM test_namespace WHERE age*2 = year-1990",
		 Query(default_namespace).Where(ArithmeticExpression("age*2"), CondEq, ArithmeticExpression("year-1990")), 1},
		{"SELECT * FROM test_namespace WHERE 2030 = year+age*2",
		 Query(default_namespace).Where(ArithmeticExpression("year+age*2"), CondEq, VariantArray{Variant{2030}}), 1},
		{"SELECT * FROM test_namespace WHERE age = (year-1990)/2",
		 Query(default_namespace).Where(kFieldNameAge, CondEq, ArithmeticExpression("(year-1990)/2")), 1},
	};
	for (const auto& [sql, expected, count] : cases) {
		Query parsed;
		ASSERT_NO_THROW(parsed = Query::FromSQL(sql)) << sql;
		EXPECT_EQ(parsed, expected) << sql;
		EXPECT_EQ(SelectCount(parsed), count) << sql;
	}
}

TEST_F(ExpressionApi, ShardKeyInArithmeticIsRejected) {
	using reindexer::cluster::ShardingConfig;
	using reindexer::sharding::RoutingStrategy;

	ShardingConfig cfg;
	ShardingConfig::Namespace ns;
	ns.ns = "ns";
	ns.index = "location";
	ns.defaultShard = 0;
	ShardingConfig::Key key;
	key.shardId = 0;
	key.values.emplace_back(Variant{"key1"});
	ns.keys.push_back(std::move(key));
	cfg.namespaces.push_back(std::move(ns));

	RoutingStrategy routing{cfg};
	{
		Query q("ns");
		q.Where("location", CondEq, ArithmeticExpression("1+0"));
		try {
			std::ignore = routing.GetHostsIdsKeyPair(Impl(q));
			FAIL() << "expected error for shard key compared with arithmetic";
		} catch (const Error& err) {
			EXPECT_STREQ(err.what(), "Shard key cannot be used in arithmetic expression");
		}
	}
	{
		Query q("ns");
		q.Where(ArithmeticExpression("location+0"), CondEq, VariantArray{Variant{0}});
		try {
			std::ignore = routing.GetHostsIdsKeyPair(Impl(q));
			FAIL() << "expected error for shard key inside arithmetic";
		} catch (const Error& err) {
			EXPECT_STREQ(err.what(), "Shard key cannot be used in arithmetic expression");
		}
	}
	{
		Query q("ns");
		q.Where(ArithmeticExpression("flat_array_len(location)"), CondGt, VariantArray{Variant{0}});
		try {
			std::ignore = routing.GetHostsIdsKeyPair(Impl(q));
			FAIL() << "expected error for shard key used as an arithmetic function argument";
		} catch (const Error& err) {
			EXPECT_STREQ(err.what(), "Shard key cannot be used in arithmetic expression");
		}
	}
	{
		Query q("ns");
		q.Where("location", CondEq, "key1").Where(ArithmeticExpression("id+1"), CondGe, VariantArray{Variant{0}});
		EXPECT_NO_THROW(std::ignore = routing.GetHostsIdsKeyPair(Impl(q)));
	}
	{
		Query q("ns");
		q.Where(ArithmeticExpression("location+1"), CondGt, Query("other").Select("id"));
		try {
			std::ignore = routing.GetHostsIdsKeyPair(Impl(q));
			FAIL() << "expected error for shard key inside arithmetic compared with a subquery";
		} catch (const Error& err) {
			EXPECT_STREQ(err.what(), "Shard key cannot be used in arithmetic expression");
		}
	}
}

TEST_F(ExpressionApi, WhereArithmeticSubquery) {
	constexpr std::string_view kNs = "where_arith_subq_ns";
	constexpr std::string_view kOther = "where_arith_subq_other";
	rt.OpenNamespace(kNs);
	rt.OpenNamespace(kOther);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
								 IndexDeclaration{kFieldNameAge, "hash", "int", IndexOpts(), 0}});
	DefineNamespaceDataset(kOther, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
									IndexDeclaration{kFieldNameAge, "hash", "int", IndexOpts(), 0}});
	for (int id = 1; id <= 4; ++id) {
		Item item = NewItem(kNs);
		item[kFieldNameId] = id;
		item[kFieldNameAge] = id * 10;
		Upsert(kNs, item);
		Item other = NewItem(kOther);
		other[kFieldNameId] = id;
		other[kFieldNameAge] = 2;
		Upsert(kOther, other);
	}

	const auto subIds = Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondEq, 2);
	const auto left = Query(kNs).Where(ArithmeticExpression(std::string(kFieldNameId) + "+1"), CondGt, Query(subIds));
	const auto right = Query(kNs).Where(Query(subIds), CondEq, ArithmeticExpression(std::string(kFieldNameAge) + "/10"));

	EXPECT_EQ(SelectCount(left), 3);
	EXPECT_EQ(SelectCount(right), 1);

	const auto roundTrip = [](const Query& q) {
		const Query fromSql = Query::FromSQL(q.GetSQL());
		EXPECT_EQ(fromSql, q) << q.GetSQL();
		const Query fromJson = Query::FromJSON(q.GetJSON());
		EXPECT_EQ(fromJson, q) << q.GetJSON();
	};
	roundTrip(left);
	roundTrip(right);

	const auto dsl = fmt::format(
		R"({{"namespace":"{}","filters":[{{"op":"and","cond":"GT","left_expression":{{"type":"expression","value":"{}+1"}},"subquery":{{"namespace":"{}","select_filter":["{}"]}}}}]}})",
		kNs, kFieldNameId, kOther, kFieldNameId);
	const Query fromDsl = Query::FromJSON(dsl);
	EXPECT_EQ(fromDsl,
			  Query(kNs).Where(ArithmeticExpression(std::string(kFieldNameId) + "+1"), CondGt, Query(kOther).Select(kFieldNameId)));

	const auto dslRight = fmt::format(
		R"({{"namespace":"{}","filters":[{{"op":"and","cond":"EQ","subquery":{{"namespace":"{}","select_filter":["{}"]}},"right_expression":{{"type":"expression","value":"{}+1"}}}}]}})",
		kNs, kOther, kFieldNameId, kFieldNameId);
	EXPECT_EQ(Query::FromJSON(dslRight),
			  Query(kNs).Where(Query(kOther).Select(kFieldNameId), CondEq, ArithmeticExpression(std::string(kFieldNameId) + "+1")));

	EXPECT_THROW(std::ignore = Query(kNs).Where(ArithmeticExpression("id+1"), CondLike, Query(kOther).Select(kFieldNameId)), Error);
	EXPECT_THROW(std::ignore = Query(kNs).Where(Query(kOther).Select(kFieldNameId), CondRange, ArithmeticExpression("id+1")), Error);

	EXPECT_EQ(Query(kNs).Where(ArithmeticExpression("6"), CondGt, Query(kOther).Select(kFieldNameId)),
			  Query(kNs).Where(Query(kOther).Select(kFieldNameId), CondLt, 6));
	EXPECT_EQ(Query(kNs).Where(Query(kOther).Select(kFieldNameId), CondGe, ArithmeticExpression("-6")),
			  Query(kNs).Where(Query(kOther).Select(kFieldNameId), CondGe, -6));

	const auto lenQuery = Query(kNs).Where(reindexer::functions::FlatArrayLen(std::string(kFieldNameId)), CondEq,
										   Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondEq, 1));
	const auto nowQuery =
		Query(kNs).Where(Query(kNs).Select(kFieldNameAge).Where(kFieldNameId, CondEq, 1), CondLe, reindexer::functions::Now());
	EXPECT_EQ(SelectCount(lenQuery), 4);
	EXPECT_EQ(SelectCount(nowQuery), 4);
	const auto sqlRoundTrip = [](const Query& q) {
		const Query fromSql = Query::FromSQL(q.GetSQL());
		EXPECT_EQ(fromSql, q) << q.GetSQL();
	};
	sqlRoundTrip(lenQuery);
	sqlRoundTrip(nowQuery);
	EXPECT_EQ(Query::FromJSON(lenQuery.GetJSON()),
			  Query(kNs).Where(ArithmeticExpression(reindexer::functions::FlatArrayLen(std::string(kFieldNameId)).ToString()), CondEq,
							   Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondEq, 1)));
	EXPECT_EQ(Query::FromJSON(nowQuery.GetJSON()), Query(kNs).Where(Query(kNs).Select(kFieldNameAge).Where(kFieldNameId, CondEq, 1), CondLe,
																	ArithmeticExpression(reindexer::functions::Now().ToString())));

	const auto lenDsl = fmt::format(
		R"json({{"namespace":"{}","filters":[{{"op":"and","cond":"EQ","left_expression":{{"type":"expression","value":"flat_array_len({})"}},"subquery":{{"namespace":"{}","select_filter":["{}"]}}}}]}})json",
		kNs, kFieldNameId, kOther, kFieldNameId);
	const Query lenFromDsl = Query::FromJSON(lenDsl);
	EXPECT_EQ(lenFromDsl, Query(kNs).Where(ArithmeticExpression("flat_array_len(" + std::string(kFieldNameId) + ")"), CondEq,
										   Query(kOther).Select(kFieldNameId)));
	EXPECT_EQ(SelectCount(lenFromDsl), 4);
	const auto nowDsl = fmt::format(
		R"json({{"namespace":"{}","filters":[{{"op":"and","cond":"EQ","subquery":{{"namespace":"{}","select_filter":["{}"]}},"right_expression":{{"type":"expression","value":"now()"}}}}]}})json",
		kNs, kOther, kFieldNameId);
	EXPECT_EQ(Query::FromJSON(nowDsl), Query(kNs).Where(Query(kOther).Select(kFieldNameId), CondEq, ArithmeticExpression("now()")));
	const auto arithDsl = fmt::format(
		R"json({{"namespace":"{}","filters":[{{"op":"and","cond":"GT","left_expression":{{"type":"expression","value":"now()-0"}},"subquery":{{"namespace":"{}","select_filter":["{}"]}}}}]}})json",
		kNs, kOther, kFieldNameId);
	EXPECT_EQ(Query::FromJSON(arithDsl), Query(kNs).Where(ArithmeticExpression("now()-0"), CondGt, Query(kOther).Select(kFieldNameId)));
	const auto nowOnTheLeft = fmt::format(
		R"json({{"namespace":"{}","filters":[{{"cond":"GT","left_expression":{{"type":"expression","value":"now()"}},"subquery":{{"namespace":"{}","select_filter":["{}"],"filters":[{{"cond":"EQ","field":"{}","value":1}}]}}}}]}})json",
		kNs, kOther, kFieldNameId, kFieldNameId);
	EXPECT_EQ(Query::FromJSON(nowOnTheLeft),
			  Query(kNs).Where(ArithmeticExpression("now()"), CondGt, Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondEq, 1)));
	const auto lenOnTheRight = fmt::format(
		R"json({{"namespace":"{}","filters":[{{"cond":"EQ","subquery":{{"namespace":"{}","select_filter":["{}"]}},"right_expression":{{"type":"expression","value":"flat_array_len({})"}}}}]}})json",
		kNs, kOther, kFieldNameId, kFieldNameId);
	EXPECT_EQ(Query::FromJSON(lenOnTheRight), Query(kNs).Where(Query(kOther).Select(kFieldNameId), CondEq,
															   ArithmeticExpression("flat_array_len(" + std::string(kFieldNameId) + ")")));
	for (const auto& dslQuery : {Query::FromJSON(nowOnTheLeft), Query::FromJSON(lenOnTheRight)}) {
		EXPECT_EQ(Query::FromSQL(dslQuery.GetSQL()), dslQuery) << dslQuery.GetSQL();
		EXPECT_EQ(SelectCount(dslQuery), 4) << dslQuery.GetSQL();
	}

	const auto firstId = [&] { return Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondEq, 1); };
	const std::string lenPlusZero = "flat_array_len(" + std::string(kFieldNameId) + ") + (1-1)";
	for (const auto& query : {Query(kNs).Where(firstId(), CondLt, ArithmeticExpression("now() - 0")),
							  Query(kNs).Where(firstId(), CondEq, ArithmeticExpression(lenPlusZero)),
							  Query(kNs).Where(ArithmeticExpression(lenPlusZero), CondEq, firstId())}) {
		roundTrip(query);
		EXPECT_EQ(SelectCount(query), 4) << query.GetSQL();
	}
}

TEST_F(ExpressionApi, WhereSubqueryWithConstantExpression) {
	constexpr std::string_view kNs = "where_subq_const_ns";
	constexpr std::string_view kOther = "where_subq_const_other";
	rt.OpenNamespace(kNs);
	rt.OpenNamespace(kOther);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	DefineNamespaceDataset(kOther, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
									IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0}});
	for (int id = 1; id <= 3; ++id) {
		rt.UpsertJSON(kNs, fmt::format(R"json({{"id":{}}})json", id));
		rt.UpsertJSON(kOther, fmt::format(R"json({{"id":{},"year":{}}})json", id, 2000 + id));
	}
	const auto years = [&] { return Query(kOther).Select(kFieldNameYear); };
	const auto firstYear = [&] { return Query(kOther).Select(kFieldNameYear).Where(kFieldNameId, CondEq, 1); };

	const auto divisionByZeroQuery = Query(kNs).Where(ArithmeticExpression("1/0"), CondGt, firstYear());
	const auto divisionByZero = SelectError(divisionByZeroQuery);
	EXPECT_EQ(divisionByZero.code(), errLogic) << divisionByZero.what();
	EXPECT_THAT(divisionByZero.what(), ::testing::HasSubstr("Division by zero"));
	EXPECT_EQ(Query::FromSQL(divisionByZeroQuery.GetSQL()), divisionByZeroQuery);

	for (const std::string_view expr : {"1+1", "now()-60", "id+1"}) {
		const auto rangeLeft = BuildError([&] { return Query(kNs).Where(years(), CondRange, ArithmeticExpression(expr)); });
		EXPECT_EQ(rangeLeft.code(), errQueryExec) << expr;
		EXPECT_EQ(rangeLeft.whatStr(), LeftSubqueryConditionError(CondRange, "expression")) << expr;
		const auto rangeLeftSql = BuildError(
			[&] { return Query::FromSQL(fmt::format("SELECT * FROM {} WHERE (SELECT year FROM {}) RANGE ({})", kNs, kOther, expr)); });
		EXPECT_THAT(rangeLeftSql.whatStr(), ::testing::HasSubstr(LeftSubqueryConditionError(CondRange, "expression"))) << expr;
		for (const CondType cond : {CondLike, CondDWithin}) {
			const auto message = fmt::format("Condition {} is not supported by arithmetic expressions in WHERE", CondTypeToStrShort(cond));
			const auto subqueryLeft = BuildError([&] { return Query(kNs).Where(years(), cond, ArithmeticExpression(expr)); });
			EXPECT_EQ(subqueryLeft.whatStr(), message) << expr;
			const auto subqueryRight = BuildError([&] { return Query(kNs).Where(ArithmeticExpression(expr), cond, years()); });
			EXPECT_EQ(subqueryRight.whatStr(), message) << expr;
		}
	}
	for (const CondType cond : {CondRange, CondLike}) {
		const std::string sql = fmt::format("SELECT * FROM {} WHERE (SELECT year FROM {}) {} now()", kNs, kOther, CondTypeToStrShort(cond));
		for (const auto& nowErr : {BuildError([&] { return Query(kNs).Where(years(), cond, reindexer::functions::Now()); }),
								   BuildError([&] { return Query(kNs).Where(years(), cond, ArithmeticExpression("now()")); }),
								   BuildError([&] { return Query::FromSQL(sql); })}) {
			EXPECT_THAT(nowErr.whatStr(), ::testing::HasSubstr(LeftSubqueryConditionError(cond, "function"))) << sql;
		}
	}

	const auto leftExpressionDsl = [&](CondType cond, std::string_view expr) {
		return Query::FromJSON(fmt::format(
			R"json({{"namespace":"{}","filters":[{{"cond":"{}","left_expression":{{"type":"expression","value":"{}"}},"subquery":{{"namespace":"{}","select_filter":["{}"]}}}}]}})json",
			kNs, CondTypeToStrShort(cond), expr, kOther, kFieldNameYear));
	};
	for (const CondType cond : {CondRange, CondAllSet}) {
		for (const std::string_view expr : {"2001", "-2001", "2001+1", "now()", "now()*0+2002"}) {
			const Query query = Query(kNs).Where(ArithmeticExpression(expr), cond, years());
			EXPECT_EQ(leftExpressionDsl(cond, expr), query) << expr;
			EXPECT_EQ(Query::FromSQL(
						  fmt::format("SELECT * FROM {} WHERE {} {} (SELECT year FROM {})", kNs, expr, CondTypeToStrShort(cond), kOther)),
					  query)
				<< expr;
		}
		const auto sqlErr = BuildError([&] {
			return Query::FromSQL(
				fmt::format("SELECT * FROM {} WHERE '2001' {} (SELECT year FROM {})", kNs, CondTypeToStrShort(cond), kOther));
		});
		EXPECT_THAT(sqlErr.whatStr(), ::testing::HasSubstr(fmt::format("Condition {} is not supported with value on the left side",
																	   CondTypeToStrShort(cond))));
	}
	const auto twoYears = [&] {
		return Query(kOther).Select(kFieldNameYear).Where(kFieldNameId, CondLe, 2).Sort(kFieldNameYear, SortOrder::Asc);
	};
	const std::tuple<std::string_view, CondType, Query, size_t> constantOnTheLeft[]{
		{"2001", CondAllSet, firstYear(), 3}, {"2000+1", CondAllSet, firstYear(), 3}, {"now()*0+2001", CondAllSet, firstYear(), 3},
		{"2002", CondAllSet, firstYear(), 0}, {"-2001", CondAllSet, firstYear(), 0},  {"2001", CondRange, twoYears(), 3},
		{"2000+1", CondRange, twoYears(), 3}, {"2003", CondRange, twoYears(), 0},	  {"-2001", CondRange, twoYears(), 0},
	};
	for (const auto& [expr, cond, subquery, count] : constantOnTheLeft) {
		const Query query = Query(kNs).Where(ArithmeticExpression(expr), cond, Query(subquery));
		const auto sql = fmt::format("SELECT * FROM {} WHERE {} {} ({})", kNs, expr, CondTypeToStrShort(cond), subquery.GetSQL());
		EXPECT_EQ(Query::FromSQL(sql), query) << sql;
		EXPECT_EQ(SelectCount(query), count) << sql;
		EXPECT_EQ(SelectCount(Query::FromSQL(query.GetSQL())), count) << sql;
		EXPECT_EQ(SelectCount(Query::FromJSON(query.GetJSON())), count) << sql;
	}
	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("now()*0+2002"), CondAllSet, firstYear())), 0);
	EXPECT_EQ(SelectCount(Query(kNs).Where(ArithmeticExpression("now()*0+2001"), CondAllSet, firstYear())), 3);
	EXPECT_EQ(SelectCount(Query(kNs).Where(years(), CondAllSet, ArithmeticExpression("2001"))), 3);
	EXPECT_EQ(SelectCount(Query(kNs).Where(years(), CondAllSet, ArithmeticExpression("2005"))), 0);

	const auto firstIds = [&] { return Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondLt, 3); };
	const std::pair<std::string_view, size_t> allSetOnTheRight[]{
		{"2", 3}, {"1+1", 3}, {"now()*0+2", 3}, {"5", 0}, {"4+1", 0}, {"id+0", 2}, {"flat_array_len(id)", 3},
	};
	for (const auto& [expr, count] : allSetOnTheRight) {
		const auto sql = fmt::format("SELECT * FROM {} WHERE ({}) ALLSET {}", kNs, firstIds().GetSQL(), expr);
		const Query query = Query::FromSQL(sql);
		EXPECT_EQ(SelectCount(query), count) << sql;
		EXPECT_EQ(SelectCount(Query(kNs).Where(firstIds(), CondAllSet, ArithmeticExpression(expr))), count) << sql;
		EXPECT_EQ(SelectCount(Query::FromSQL(query.GetSQL())), count) << sql;
		EXPECT_EQ(SelectCount(Query::FromJSON(query.GetJSON())), count) << sql;
	}
	const auto fieldErr = BuildError([&] { return Query(kNs).Where(firstIds(), CondAllSet, ArithmeticExpression("id")); });
	EXPECT_EQ(fieldErr.whatStr(), LeftSubqueryConditionError(CondAllSet, "field"));
}

TEST_F(ExpressionApi, SubqueryAgainstRowIndependentSide) {
	constexpr std::string_view kNs = "subq_rows_ns";
	constexpr std::string_view kOther = "subq_rows_other";
	rt.OpenNamespace(kNs);
	rt.OpenNamespace(kOther);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	DefineNamespaceDataset(kOther, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
									IndexDeclaration{kFieldNameYear, "tree", "int", IndexOpts(), 0}});
	for (int id = 1; id <= 4; ++id) {
		rt.UpsertJSON(kNs, fmt::format(R"json({{"id":{}}})json", id));
		rt.UpsertJSON(kOther, fmt::format(R"json({{"id":{},"year":{}}})json", id, 2000 + id));
	}
	const auto ids = [&] { return Query(kOther).Select(kFieldNameId); };
	const auto noIds = [&] { return Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondLt, 0); };
	const auto firstId = [&] { return Query(kOther).Select(kFieldNameId).Where(kFieldNameId, CondEq, 1); };
	const auto years = [&] { return Query(kOther).Select(kFieldNameYear); };
	const auto noYears = [&] { return Query(kOther).Select(kFieldNameYear).Where(kFieldNameId, CondLt, 0); };
	const auto firstYear = [&] { return Query(kOther).Select(kFieldNameYear).Where(kFieldNameId, CondEq, 1); };
	using reindexer::functions::Now;

	const std::pair<Query, size_t> cases[]{
		{Query(kNs).Where(ArithmeticExpression("6"), CondGt, ids()), 4},
		{Query(kNs).Where(ArithmeticExpression("2"), CondGt, ids()), 4},
		{Query(kNs).Where(ArithmeticExpression("1"), CondGt, ids()), 0},
		{Query(kNs).Where(ArithmeticExpression("6"), CondGt, noIds()), 0},
		{Query(kNs).Where(ids(), CondLt, ArithmeticExpression("1")), 0},
		{Query(kNs).Where(ids(), CondGe, ArithmeticExpression("-1")), 4},
		{Query(kNs).Where(years(), CondLt, Now()), 4},
		{Query(kNs).Where(years(), CondLt, ArithmeticExpression("now()")), 4},
		{Query(kNs).Where(years(), CondGt, Now()), 0},
		{Query(kNs).Where(noYears(), CondLt, Now()), 0},
		{Query(kNs).Where(ArithmeticExpression("2*3"), CondGt, firstId()), 4},
		{Query(kNs).Where(ArithmeticExpression("1*1"), CondGt, firstId()), 0},
		{Query(kNs).Where(firstId(), CondLt, ArithmeticExpression("2*3")), 4},
		{Query(kNs).Where(ArithmeticExpression("now()"), CondGt, firstYear()), 4},
		{Query(kNs).Where(ArithmeticExpression("now()-60"), CondGt, firstYear()), 4},
		{Query(kNs).Where(firstYear(), CondLt, ArithmeticExpression("now()-60")), 4},
		{Query(kNs).Where(firstYear(), CondGt, ArithmeticExpression("now()-60")), 0},
		{Query(kNs).Where(ArithmeticExpression("now()"), CondLt, firstYear()), 0},
		{Query(kNs).Where(ArithmeticExpression("now()"), CondGt, years()), 4},
		{Query(kNs).Where(ArithmeticExpression("now()"), CondLt, years()), 0},
		{Query(kNs).Where(ArithmeticExpression("now()"), CondGt, noYears()), 0},
		{Query(kNs).Where(ArithmeticExpression("now()*0+2002"), CondSet, years()), 4},
		{Query(kNs).Where(ArithmeticExpression("now()*0+2005"), CondSet, years()), 0},
	};
	for (const auto& [query, count] : cases) {
		EXPECT_EQ(SelectCount(query), count) << query.GetSQL();
		EXPECT_EQ(SelectCount(Query::FromJSON(query.GetJSON())), count) << query.GetJSON();
		EXPECT_EQ(SelectCount(Query::FromSQL(query.GetSQL())), count) << query.GetSQL();
	}

	const auto expectCardinalityError = [&](const Query& q) {
		for (const Query& query : {q, Query::FromSQL(q.GetSQL()), Query::FromJSON(q.GetJSON())}) {
			const auto err = SelectError(query);
			EXPECT_EQ(err.code(), errParams) << query.GetSQL() << ": " << err.what();
			EXPECT_THAT(err.what(),
						::testing::MatchesRegex(".*Condition Cond(Gt|Lt) must have exact 1 argument, but [0-9]+ arguments were provided"))
				<< query.GetSQL();
		}
	};
	for (const std::string_view expr : {"id+1", "id*0+6", "2*3", "now()-60"}) {
		for (const auto& subquery : {ids(), noIds(), years(), noYears()}) {
			expectCardinalityError(Query(kNs).Where(ArithmeticExpression(expr), CondGt, Query(subquery)));
			expectCardinalityError(Query(kNs).Where(Query(subquery), CondLt, ArithmeticExpression(expr)));
			expectCardinalityError(Query(kNs).Where(Query(subquery), CondGt, ArithmeticExpression(expr)));
		}
	}
	for (const auto& subquery : {ids(), noIds()}) {
		const auto fieldErr = SelectError(Query(kNs).Where(kFieldNameId, CondGt, Query(subquery)));
		EXPECT_EQ(fieldErr.code(), errParams) << fieldErr.what();
		for (const auto& err : {SelectError(Query(kNs).Where(ArithmeticExpression("id+0"), CondGt, Query(subquery))),
								SelectError(Query(kNs).Where(Query(subquery), CondLt, ArithmeticExpression("id+0"))),
								SelectError(Query(kNs).Where(Query(subquery), CondLt, ArithmeticExpression(kFieldNameId))),
								SelectError(Query(kNs).Where(reindexer::functions::FlatArrayLen("id"), CondGt, Query(subquery)))}) {
			EXPECT_EQ(err.code(), fieldErr.code()) << err.what();
			EXPECT_EQ(err.whatStr(), fieldErr.whatStr());
		}
	}
}

TEST_F(ExpressionApi, SubqueryComparisonContracts) {
	constexpr std::string_view kNs = "subq_contract_ns";
	constexpr std::string_view kOther = "subq_contract_other";
	rt.OpenNamespace(kNs);
	rt.OpenNamespace(kOther);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
								 IndexDeclaration{"arr", "hash", "int", IndexOpts().Array(), 0}});
	DefineNamespaceDataset(kOther, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
									IndexDeclaration{"val", "tree", "int", IndexOpts(), 0}});
	rt.UpsertJSON(kNs, R"json({"id":1,"arr":[1]})json");
	rt.UpsertJSON(kNs, R"json({"id":2,"arr":[1,2]})json");
	rt.UpsertJSON(kNs, R"json({"id":3,"arr":[1,2,3]})json");
	rt.UpsertJSON(kNs, R"json({"id":4,"arr":[1,2,3,4]})json");
	rt.UpsertJSON(kOther, R"json({"id":1,"val":2})json");
	rt.UpsertJSON(kOther, R"json({"id":2,"val":2})json");
	rt.UpsertJSON(kOther, R"json({"id":3,"val":3})json");
	rt.UpsertJSON(kOther, R"json({"id":4,"val":5})json");

	enum class [[nodiscard]] SubquerySide { Right, Left };
	struct [[nodiscard]] Outcome {
		std::vector<int> ids;
		std::string error;
	};
	struct [[nodiscard]] Case {
		SubquerySide side;
		CondType cond;
		Outcome empty, single, multiple, duplicates;
	};
	const auto ids = [](std::initializer_list<int> values) { return Outcome{std::vector<int>(values), {}}; };
	const auto fails = [](std::string_view error) { return Outcome{{}, std::string{error}}; };
	const auto oneValue = fails("must have exact 1 argument");
	const auto twoValues = fails("must have exact 2 argument");
	const auto rangeLeft = fails(LeftSubqueryConditionError(CondRange, "expression"));
	const Case cases[]{
		{SubquerySide::Right, CondEq, ids({}), ids({3}), ids({3}), ids({2})},
		{SubquerySide::Right, CondSet, ids({}), ids({3}), ids({3}), ids({2})},
		{SubquerySide::Right, CondAllSet, ids({}), ids({3}), ids({}), ids({2})},
		{SubquerySide::Right, CondGt, oneValue, ids({4}), oneValue, oneValue},
		{SubquerySide::Right, CondRange, twoValues, twoValues, ids({3, 4}), ids({2})},
		{SubquerySide::Left, CondEq, ids({}), ids({3}), ids({3}), ids({2})},
		{SubquerySide::Left, CondSet, ids({}), ids({3}), ids({3}), ids({2})},
		{SubquerySide::Left, CondAllSet, ids({}), ids({3}), ids({3}), ids({2})},
		{SubquerySide::Left, CondLt, oneValue, ids({4}), oneValue, oneValue},
		{SubquerySide::Left, CondRange, rangeLeft, rangeLeft, rangeLeft, rangeLeft},
	};

	const auto dslCond = [](CondType cond) {
		switch (cond) {
			case CondEq:
				return "EQ";
			case CondSet:
				return "SET";
			case CondAllSet:
				return "ALLSET";
			case CondGt:
				return "GT";
			case CondLt:
				return "LT";
			case CondRange:
				return "RANGE";
			case CondAny:
			case CondLe:
			case CondGe:
			case CondEmpty:
			case CondLike:
			case CondDWithin:
			case CondKnn:
				break;
		}
		throw Error(errLogic, "Unexpected condition {}", reindexer::CondTypeToStr(cond));
	};
	const auto run = [&](const auto& build) {
		Query query;
		try {
			query = build();
		} catch (const Error& err) {
			return Outcome{{}, err.whatStr()};
		}
		QueryResults qr;
		if (const auto err = rt.reindexer->Select(query, qr); !err.ok()) {
			return Outcome{{}, err.whatStr()};
		}
		Outcome outcome;
		for (auto it : qr) {
			outcome.ids.push_back(it.GetItem()[kFieldNameId].As<int>());
		}
		std::ranges::sort(outcome.ids);
		return outcome;
	};

	const std::pair<std::string_view, std::vector<int>> shapes[]{
		{"empty", {-1}}, {"single", {3}}, {"multiple", {3, 4}}, {"duplicates", {1, 2}}};
	const auto allSetByFunction = fails("Condition ALLSET is not supported by flat_array_len()");
	const std::pair<std::string_view, const Outcome*> operands[]{{"id+0", nullptr}, {"flat_array_len(arr)", &allSetByFunction}};
	for (const auto& [operand, allSetOverride] : operands) {
		for (const auto& c : cases) {
			const bool overridden = allSetOverride && c.side == SubquerySide::Right && c.cond == CondAllSet;
			const Outcome* expected[]{overridden ? allSetOverride : &c.empty, overridden ? allSetOverride : &c.single,
									  overridden ? allSetOverride : &c.multiple, overridden ? allSetOverride : &c.duplicates};
			for (size_t shape = 0; shape < std::size(shapes); ++shape) {
				const auto& [shapeName, otherIds] = shapes[shape];
				VariantArray idValues;
				std::string idList;
				for (int id : otherIds) {
					idValues.emplace_back(id);
					idList += (idList.empty() ? "" : ",") + std::to_string(id);
				}
				const std::string subSql = fmt::format("(SELECT val FROM {} WHERE id IN ({}) ORDER BY val)", kOther, idList);
				const std::string subDsl = fmt::format(
					R"json({{"namespace":"{}","select_filter":["val"],"filters":[{{"cond":"SET","field":"id","value":[{}]}}],"sort":[{{"field":"val"}}]}})json",
					kOther, idList);
				const auto subApi = [&] {
					return Query(kOther).Select("val").Where(kFieldNameId, CondSet, idValues).Sort("val", SortOrder::Asc);
				};
				const bool subqueryLeft = c.side == SubquerySide::Left;
				const std::string sql =
					subqueryLeft ? fmt::format("SELECT * FROM {} WHERE {} {} {}", kNs, subSql, CondTypeToStrShort(c.cond), operand)
								 : fmt::format("SELECT * FROM {} WHERE {} {} {}", kNs, operand, CondTypeToStrShort(c.cond), subSql);
				const std::string dsl = fmt::format(
					R"json({{"namespace":"{}","filters":[{{"cond":"{}","{}":{{"type":"expression","value":"{}"}},"subquery":{}}}]}})json",
					kNs, dslCond(c.cond), subqueryLeft ? "right_expression" : "left_expression", operand, subDsl);
				const std::pair<std::string_view, Outcome> actual[]{
					{"api", run([&] {
						 return subqueryLeft ? Query(kNs).Where(subApi(), c.cond, ArithmeticExpression(operand))
											 : Query(kNs).Where(ArithmeticExpression(operand), c.cond, subApi());
					 })},
					{"sql", run([&] { return Query::FromSQL(sql); })},
					{"dsl", run([&] { return Query::FromJSON(dsl); })},
				};
				for (const auto& [api, outcome] : actual) {
					const auto trace = fmt::format("{} {}: {}", api, shapeName, sql);
					if (expected[shape]->error.empty()) {
						EXPECT_EQ(outcome.error, "") << trace;
						EXPECT_EQ(outcome.ids, expected[shape]->ids) << trace;
					} else {
						EXPECT_THAT(outcome.error, ::testing::HasSubstr(expected[shape]->error)) << trace;
					}
				}
			}
		}
	}
}

TEST_F(ExpressionApi, SubqueryExplainFieldOrKeys) {
	constexpr std::string_view kNs = "subq_explain_ns";
	constexpr std::string_view kOther = "subq_explain_other";
	rt.OpenNamespace(kNs);
	rt.OpenNamespace(kOther);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	DefineNamespaceDataset(kOther, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	for (int id = 1; id <= 3; ++id) {
		rt.UpsertJSON(kNs, fmt::format(R"json({{"id":{}}})json", id));
		rt.UpsertJSON(kOther, fmt::format(R"json({{"id":{}}})json", id));
	}
	const auto ids = [&] { return Query(kOther).Select(kFieldNameId); };
	const std::tuple<Query, std::string_view, std::string_view> cases[]{
		{Query(kNs).Where(ids().Limit(10), CondSet, {1, 2}), "keys", "2"},
		{Query(kNs).Where(kFieldNameId, CondSet, ids()), "field", "id"},
		{Query(kNs).Where(ArithmeticExpression("flat_array_len(id)"), CondEq, ids()), "field", "flat_array_len(id)"},
		{Query(kNs).Where(ArithmeticExpression("id+1"), CondSet, ids()), "field", "id+1"},
	};
	for (const auto& [query, key, value] : cases) {
		QueryResults qr;
		const auto err = rt.reindexer->Select(Query(query).Explain(), qr);
		ASSERT_TRUE(err.ok()) << err.what() << " sql=" << query.GetSQL();
		const YAML::Node subqueries = YAML::Load(qr.GetExplainResults())["subqueries"];
		ASSERT_TRUE(subqueries.IsSequence()) << qr.GetExplainResults();
		ASSERT_EQ(subqueries.size(), 1) << qr.GetExplainResults();
		EXPECT_EQ(subqueries[0]["namespace"].as<std::string>(), kOther) << qr.GetExplainResults();
		EXPECT_EQ(subqueries[0][std::string(key)].as<std::string>(), value) << qr.GetExplainResults();
	}
}

TEST_F(ExpressionApi, RowIndependentSubqueryConditionIsEvaluatedOnce) {
	constexpr std::string_view kNs = "subq_once_ns";
	constexpr std::string_view kEmpty = "subq_once_empty";
	constexpr std::string_view kOther = "subq_once_other";
	for (const std::string_view ns : {kNs, kEmpty, kOther}) {
		rt.OpenNamespace(ns);
		DefineNamespaceDataset(ns, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	}
	for (int id = 1; id <= 3; ++id) {
		rt.UpsertJSON(kNs, fmt::format(R"json({{"id":{}}})json", id));
		rt.UpsertJSON(kOther, fmt::format(R"json({{"id":{}}})json", id));
	}
	const auto ids = [&] { return Query(kOther).Select(kFieldNameId); };
	const auto selectorTypes = [&](const Query& query) {
		QueryResults qr;
		const auto err = rt.reindexer->Select(Query(query).Explain(), qr);
		EXPECT_TRUE(err.ok()) << err.what() << " sql=" << query.GetSQL();
		std::vector<std::string> types;
		for (const auto& selector : YAML::Load(qr.GetExplainResults())["selectors"]) {
			types.push_back(selector["field"].as<std::string>() + ":" + selector["type"].as<std::string>());
		}
		return std::make_pair(qr.Count(), types);
	};
	using Types = std::vector<std::string>;
	EXPECT_EQ(selectorTypes(Query(kNs).Where(ArithmeticExpression("1+1"), CondSet, ids())),
			  std::make_pair(size_t{3}, Types{"-scan:SingleRange"}));
	EXPECT_EQ(selectorTypes(Query(kNs).Where(ids(), CondSet, ArithmeticExpression("now()-now()+2"))),
			  std::make_pair(size_t{3}, Types{"-scan:SingleRange"}));
	EXPECT_EQ(selectorTypes(Query(kNs).Where(ArithmeticExpression("1+5"), CondSet, ids())),
			  std::make_pair(size_t{0}, Types{"always_false:SingleRange"}));
	EXPECT_EQ(selectorTypes(Query(kNs).Where(ArithmeticExpression("id+0"), CondSet, ids())),
			  std::make_pair(size_t{3}, Types{"-scan:SingleRange", "id+0 IN (1,2,3):ArithmeticComparator"}));

	const auto err = SelectError(Query(kEmpty).Where(ArithmeticExpression("1/0"), CondGt, ids().Where(kFieldNameId, CondEq, 1)));
	EXPECT_EQ(err.code(), errLogic) << err.what();
	EXPECT_THAT(err.whatStr(), ::testing::HasSubstr("Division by zero")) << err.what();
}

TEST_F(ExpressionApi, SubqueryOperandConditionMatrix) {
	constexpr std::string_view kNs = "subq_matrix_ns";
	constexpr std::string_view kOther = "subq_matrix_other";
	rt.OpenNamespace(kNs);
	rt.OpenNamespace(kOther);
	for (const std::string_view ns : {kNs, kOther}) {
		DefineNamespaceDataset(ns, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
									IndexDeclaration{kFieldNameName, "hash", "string", IndexOpts(), 0}});
	}
	for (int id = 1; id <= 3; ++id) {
		rt.UpsertJSON(kNs, fmt::format(R"json({{"id":{},"name":"abc{}"}})json", id, id));
	}
	for (int id = 1; id <= 2; ++id) {
		rt.UpsertJSON(kOther, fmt::format(R"json({{"id":{},"name":"abc{}"}})json", id, id));
	}

	struct [[nodiscard]] Outcome {
		std::vector<int> ids;
		std::string error;
	};
	const auto ids = [](std::initializer_list<int> values) { return Outcome{std::vector<int>(values), {}}; };
	const auto fails = [](std::string_view error) { return Outcome{{}, std::string{error}}; };
	const auto arithmetic = [&](CondType cond) {
		return fails(fmt::format("Condition {} is not supported by arithmetic expressions in WHERE", CondTypeToStrShort(cond)));
	};
	const auto valueOnTheLeft = [&](CondType cond) {
		return fails(fmt::format("Condition {} is not supported with value on the left side", CondTypeToStrShort(cond)));
	};
	const auto flatArrayLen = [&](CondType cond) {
		return fails(fmt::format("Condition {} is not supported by flat_array_len()", CondTypeToStrShort(cond)));
	};
	const std::pair<std::string_view, Outcome> cases[]{
		{"id ALLSET (SELECT id FROM {other})", ids({})},
		{"id ALLSET (SELECT id FROM {other} WHERE id = 2)", ids({2})},
		{"name LIKE (SELECT name FROM {other})", fails("Condition CondLike must have exact 1 argument, but 2 arguments were provided")},
		{"name LIKE (SELECT name FROM {other} WHERE id = 1)", ids({1})},
		{"'abc' LIKE (SELECT name FROM {other})", valueOnTheLeft(CondLike)},
		{"5 LIKE (SELECT id FROM {other})", valueOnTheLeft(CondLike)},
		{"'abc' RANGE (SELECT name FROM {other})", valueOnTheLeft(CondRange)},
		{"1 + 2 LIKE (SELECT id FROM {other})", arithmetic(CondLike)},
		{"now() LIKE (SELECT id FROM {other})", arithmetic(CondLike)},
		{"now() ALLSET (SELECT id FROM {other})", ids({})},
		{"flat_array_len(id) LIKE (SELECT id FROM {other})", flatArrayLen(CondLike)},
		{"flat_array_len(id) ALLSET (SELECT id FROM {other})", flatArrayLen(CondAllSet)},
		{"flat_array_len(id) RANGE (SELECT id FROM {other} ORDER BY id)", ids({1, 2, 3})},
		{"(SELECT name FROM {other}) LIKE name", fails(LeftSubqueryConditionError(CondLike, "field"))},
		{"(SELECT id FROM {other}) ALLSET id", fails(LeftSubqueryConditionError(CondAllSet, "field"))},
		{"(SELECT id FROM {other}) RANGE (1,2)", ids({1, 2, 3})},
		{"(SELECT id FROM {other}) RANGE (5,6)", ids({})},
		{"(SELECT name FROM {other}) LIKE 'abc'", ids({})},
		{"(SELECT name FROM {other}) LIKE 'abc%'", ids({1, 2, 3})},
		{"(SELECT id FROM {other}) ALLSET now()", ids({})},
		{"(SELECT id FROM {other}) < now()", ids({1, 2, 3})},
		{"(SELECT id FROM {other}) > now()", ids({})},
		{"(SELECT name FROM {other} WHERE id = 1) = now()", fails("Can't convert 'abc1' to number")},
		{"now() = (SELECT name FROM {other} WHERE id = 1)", fails("Can't convert 'abc1' to number")},
		{"(SELECT id FROM {other}) LIKE now()", fails(LeftSubqueryConditionError(CondLike, "function"))},
		{"(SELECT id FROM {other}) ALLSET flat_array_len(id)", ids({1, 2, 3})},
		{"(SELECT id FROM {other}) ALLSET id+1", ids({1})},
		{"2 ALLSET (SELECT id FROM {other} WHERE id = 2)", ids({1, 2, 3})},
		{"2 RANGE (SELECT id FROM {other} ORDER BY id)", ids({1, 2, 3})},
		{"(SELECT id FROM {other}) LIKE flat_array_len(id)", arithmetic(CondLike)},
	};
	const auto run = [&](const Query& query) {
		QueryResults qr;
		if (const auto err = rt.reindexer->Select(query, qr); !err.ok()) {
			return Outcome{{}, err.whatStr()};
		}
		Outcome outcome;
		for (auto it : qr) {
			outcome.ids.push_back(it.GetItem()[kFieldNameId].As<int>());
		}
		std::ranges::sort(outcome.ids);
		return outcome;
	};
	const auto check = [](const Outcome& actual, const Outcome& expected, std::string_view where) {
		if (expected.error.empty()) {
			EXPECT_EQ(actual.error, "") << where;
			EXPECT_EQ(actual.ids, expected.ids) << where;
		} else {
			EXPECT_THAT(actual.error, ::testing::HasSubstr(expected.error)) << where;
			EXPECT_TRUE(actual.ids.empty()) << where;
		}
	};
	for (const auto& [condition, expected] : cases) {
		const auto sql = fmt::format("SELECT * FROM {} WHERE {}", kNs, fmt::format(fmt::runtime(condition), fmt::arg("other", kOther)));
		Query query;
		try {
			query = Query::FromSQL(sql);
		} catch (const Error& err) {
			check(Outcome{{}, err.whatStr()}, expected, sql);
			continue;
		}
		check(run(query), expected, sql);
		check(run(Query::FromSQL(query.GetSQL())), expected, query.GetSQL());
		check(run(Query::FromJSON(query.GetJSON())), expected, query.GetJSON());
	}
}

TEST_F(ExpressionApi, SqlQuotingIsNeededOnlyForUnquotedKeywordFields) {
	const std::pair<std::string_view, std::string_view> cases[]{
		{"id+1", "id+1"},
		{"flat_array_len(not)+1", "flat_array_len(not)+1"},
		{R"("not"+1)", R"("not"+1)"},
		{R"("select"*2)", R"("select"*2)"},
		{"obj.not+1", "obj.not+1"},
		{"now()-60", "now()-60"},
		{"not+1", R"("not"+1)"},
		{"NOT+1", R"("NOT"+1)"},
		{"(select)*2", R"(("select")*2)"},
		{"-equal_position", R"(-"equal_position")"},
		{"id+flat_array_len(arr)*Select", R"(id+flat_array_len(arr)*"Select")"},
	};
	for (const auto& [expr, sqlExpr] : cases) {
		const ArithmeticExpression expression{expr};
		EXPECT_EQ(expression.NeedsSqlQuoting(), expr != sqlExpr) << expr;
		const auto sql = Query(default_namespace).Where(kFieldNameId, CondEq, ArithmeticExpression{expr}).GetSQL();
		EXPECT_EQ(sql, fmt::format("SELECT * FROM {} WHERE {} = {}", default_namespace, kFieldNameId, sqlExpr)) << expr;
		EXPECT_EQ(Query::FromSQL(sql), Query(default_namespace).Where(kFieldNameId, CondEq, ArithmeticExpression{sqlExpr})) << sql;
	}
}

TEST_F(ExpressionApi, ArithmeticInWithManyValues) {
	constexpr std::string_view kNs = "arith_in_many_ns";
	constexpr std::string_view kOther = "arith_in_many_other";
	rt.OpenNamespace(kNs);
	rt.OpenNamespace(kOther);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	DefineNamespaceDataset(kOther, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0}});
	for (int id = 1; id <= 60; ++id) {
		rt.UpsertJSON(kNs, fmt::format(R"json({{"id":{}}})json", id));
	}
	for (int id = 2; id <= 120; id += 2) {
		rt.UpsertJSON(kOther, fmt::format(R"json({{"id":{}}})json", id));
	}
	const auto selectIds = [&](const Query& q) {
		QueryResults qr;
		const auto err = rt.reindexer->Select(q, qr);
		EXPECT_TRUE(err.ok()) << err.what() << " sql=" << q.GetSQL();
		std::vector<int> ids;
		for (auto it : qr) {
			ids.push_back(it.GetItem()[kFieldNameId].As<int>());
		}
		std::ranges::sort(ids);
		return ids;
	};
	std::vector<int> evenIds;
	for (int id = 2; id <= 60; id += 2) {
		evenIds.push_back(id);
	}
	std::vector<int> allIds(60);
	std::iota(allIds.begin(), allIds.end(), 1);
	const auto evens = [&] { return Query(kOther).Select(kFieldNameId); };
	VariantArray manyInts;
	for (int id = 60; id >= 2; id -= 2) {
		manyInts.emplace_back(id);
		manyInts.emplace_back(id);
	}
	manyInts.emplace_back(-5);
	manyInts.emplace_back(std::numeric_limits<int64_t>::max());
	VariantArray mixed = manyInts;
	mixed.emplace_back(1.5);
	const std::pair<Query, std::vector<int>> cases[]{
		{Query(kNs).Where(ArithmeticExpression("id+0"), CondSet, evens()), evenIds},
		{Query(kNs).Where(ArithmeticExpression("id*1.0"), CondSet, evens()), evenIds},
		{Query(kNs).Where(evens(), CondSet, ArithmeticExpression("id+0")), evenIds},
		{Query(kNs).Where(ArithmeticExpression("id+0"), CondSet, manyInts), evenIds},
		{Query(kNs).Where(ArithmeticExpression("id*1.0"), CondSet, manyInts), evenIds},
		{Query(kNs).Where(ArithmeticExpression("id+0"), CondSet, mixed), evenIds},
		{Query(kNs).Where(ArithmeticExpression("1+1"), CondSet, evens()), allIds},
		{Query(kNs).Where(ArithmeticExpression("1+2"), CondSet, evens()), {}},
		{Query(kNs).Where(ArithmeticExpression("now()-now()+2"), CondSet, manyInts), allIds},
	};
	for (const auto& [query, expected] : cases) {
		EXPECT_EQ(selectIds(query), expected) << query.GetSQL();
		EXPECT_EQ(selectIds(Query::FromSQL(query.GetSQL())), expected) << query.GetSQL();
	}
}

}  // namespace reindexer_tests
