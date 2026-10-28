#include <fmt/format.h>
#include <gmock/gmock.h>

#include <algorithm>
#include <vector>

#include "tools/stringstools.h"

#include "arithmetic_expression_precedence_cases.h"
#include "cluster/config.h"
#include "cluster/sharding/sharding.h"
#include "core/function/expression_ast.h"
#include "core/system_ns_names.h"
#include "core/type_consts_helpers.h"
#include "expression_api.h"

namespace reindexer_tests {

using reindexer::CondTypeToStrShort;
using reindexer::Error;
using reindexer::IndexOpts;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::expressions::ArithmeticExpression;

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
		EXPECT_EQ(Impl(parsed).GetStrictMode(), reindexer::strictModeFromString(std::string{mode})) << dsl;
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

TEST_F(ExpressionApi, SqlParserRejectsArithmeticWhere) {
	for (const auto sql : {"SELECT * FROM test_namespace WHERE year*2 > 10", "SELECT * FROM test_namespace WHERE age*2 = year-2000"}) {
		try {
			std::ignore = Query::FromSQL(sql);
			FAIL() << "expected SQL parse error for arithmetic WHERE: " << sql;
		} catch (const Error& err) {
			EXPECT_EQ(err.code(), errParseSQL) << sql << ": " << err.what();
			EXPECT_THAT(err.what(), ::testing::HasSubstr("condition operator")) << sql;
		}
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
}

}  // namespace reindexer_tests
