#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "core/function/expression_ast.h"
#include "core/query/expression/arithmetic_expression.h"
#include "core/query/knn_search_params.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"
#include "estl/tokenizer.h"
#include "gtests/tests/fixtures/reindexer_api.h"
#include "tools/serilize/serializer.h"
#include "tools/serilize/wrserializer.h"
#include "tools/stringstools.h"

namespace {

using reindexer::Query;
using reindexer::Variant;
using reindexer::VariantArray;
using reindexer::expressions::ArithmeticExpression;
using namespace std::string_literals;
using namespace std::string_view_literals;

std::string sqlName(std::string_view name) {
	reindexer::WrSerializer ser;
	reindexer::PrintQuotedFieldName(ser, name);
	return std::string{ser.Slice()};
}

std::string sqlString(std::string_view str) {
	reindexer::WrSerializer ser;
	reindexer::PrintQuotedString(ser, str);
	return std::string{ser.Slice()};
}

Query binaryRoundTrip(const Query& query) {
	reindexer::WrSerializer wser;
	query.Serialize(wser, QueryFormatV2);
	reindexer::Serializer ser{wser.Slice()};
	return Query::Deserialize(ser, QueryFormatV2);
}

const std::string kSpecialStrings[]{R"(a"b)",
									"a'b",
									R"(a\b)",
									R"(a\nb)",
									"a\nb",
									"tab\there",
									R"(a""b)",
									R"(a\'b)",
									R"(\)",
									R"(")",
									"'",
									"esc\x1b",
									"ctl\a\v\f\r\b",
									R"(\q\x41\z)",
									R"(a\\b)",
									"a''b\\''",
									"+",
									"-",
									"/",
									"(",
									")",
									"[",
									"]",
									"{",
									"}",
									",",
									";",
									"|",
									"||",
									"=",
									"<>",
									"!",
									"--",
									"{}",
									"x],['y"};

}  // namespace

TEST(SqlEscaping, StringLiteralsAreDecodedOnce) {
	const std::pair<std::string_view, std::string> cases[]{
		{R"('\0')", "\0"s},
		{R"('\a')", "\a"},
		{R"('\b')", "\b"},
		{R"('\t')", "\t"},
		{R"('\n')", "\n"},
		{R"('\v')", "\v"},
		{R"('\f')", "\f"},
		{R"('\r')", "\r"},
		{R"('\e')", "\x1b"},
		{R"('\'')", "'"},
		{R"('\"')", R"(")"},
		{R"('\\')", R"(\)"},
		{R"('some\'string')", "some'string"},
		{R"('some''string')", "some'string"},
		{R"('some"string')", R"(some"string)"},
		{R"('''')", "'"},
		{R"('')", ""},
		{R"('a''''b')", "a''b"},
		{R"('\\n')", R"(\n)"},
		{R"('\q')", R"(\q)"},
		{R"('\x41')", R"(\x41)"},
		{R"('\N')", R"(\N)"},
		{R"('\=\/')", R"(\=\/)"},
		{R"('\\\\')", R"(\\)"},
		{R"('\\\'')", R"(\')"},
		{R"('\\''')", R"(\')"},
		{R"('\\\\n')", R"(\\n)"},
		{R"('\\\n')", "\\\n"},
		{R"(`a\'b`)", "a'b"},
		{R"(`a``b`)", "a`b"},
		{R"('\*crisis')", R"(\*crisis)"},
		{R"('\\"quoted\\"')", R"(\"quoted\")"},
	};
	for (const auto& [literal, expected] : cases) {
		const auto sql = fmt::format("SELECT * FROM ns WHERE f = {}", literal);
		SCOPED_TRACE(sql);
		try {
			const Query parsed = Query::FromSQL(sql);
			EXPECT_EQ(parsed, Query{"ns"}.Where("f", CondEq, expected));
			EXPECT_EQ(Query::FromSQL(parsed.GetSQL()), parsed) << parsed.GetSQL();
		} catch (const std::exception& err) {
			ADD_FAILURE() << err.what();
		}
	}
}

TEST(SqlEscaping, QuotedNamesAreDecodedOnce) {
	const std::pair<std::string_view, std::string> cases[]{
		{R"("a""b")", R"(a"b)"},	  {R"("a\"b")", R"(a"b)"},	  {R"("a'b")", "a'b"},			 {R"("a\'b")", "a'b"},
		{R"("a\nb")", "a\nb"},		  {R"("a\\nb")", R"(a\nb)"},  {R"("a\qb")", R"(a\qb)"},		 {R"("a\\\"b")", R"(a\"b)"},
		{R"("""")", R"(")"},		  {R"("a\tb")", "a\tb"},	  {R"("a\eb")", "a\x1b"s + "b"}, {R"("a\0b")", "a\0b"s},
		{R"("a\\\\nb")", R"(a\\nb)"}, {R"("a""""b")", R"(a""b)"},
	};
	for (const auto& [literal, expected] : cases) {
		SCOPED_TRACE(literal);
		try {
			const auto where = fmt::format("SELECT * FROM ns WHERE {} = 1", literal);
			const Query parsed = Query::FromSQL(where);
			EXPECT_EQ(parsed, Query{"ns"}.Where(expected, CondEq, 1)) << where;
			EXPECT_EQ(Query::FromSQL(parsed.GetSQL()), parsed) << parsed.GetSQL();

			const auto orderBy = fmt::format("SELECT * FROM ns ORDER BY {} DESC", literal);
			EXPECT_EQ(Query::FromSQL(orderBy), Query{"ns"}.Sort(sqlName(expected), SortOrder::Desc)) << orderBy;

			const auto update = fmt::format("UPDATE ns SET {} = 1", literal);
			const Query parsedUpdate = Query::FromSQL(update);
			const Query expectedUpdate = Query{"ns"}.Set(expected, {int64_t{1}});
			EXPECT_EQ(parsedUpdate, expectedUpdate) << parsedUpdate.GetSQL() << " vs " << expectedUpdate.GetSQL() << " / "
													<< parsedUpdate.GetJSON() << " vs " << expectedUpdate.GetJSON();
		} catch (const std::exception& err) {
			ADD_FAILURE() << err.what();
		}
	}
	EXPECT_THROW(std::ignore = Query::FromSQL(R"(SELECT * FROM ns WHERE "a\" = 1)"), reindexer::Error);
}

TEST(SqlEscaping, UnterminatedLiteralsAreErrors) {
	constexpr std::string_view kQueries[]{
		R"(SELECT * FROM ns WHERE f = 'it'' AND x = 1)",
		R"(SELECT * FROM ns WHERE f = 'abc)",
		R"(SELECT * FROM ns WHERE f = 'abc\')",
		"SELECT * FROM ns WHERE f = `abc",
		R"(SELECT * FROM ns WHERE "a\" = 1)",
		R"(SELECT * FROM ns WHERE "a"" = 1)",
		R"(UPDATE ns SET arr = arr || ['x])",
		R"(UPDATE ns SET f = 'x)",
		"SELECT * FROM ns ORDER BY 'id ",
		"SELECT * FROM ns ORDER BY 'id + field",
		"SELECT * FROM ns ORDER BY 'id + field desc",
	};
	for (const auto sql : kQueries) {
		try {
			const Query query = Query::FromSQL(sql);
			ADD_FAILURE() << sql << " is parsed as " << query.GetSQL();
		} catch (const reindexer::Error& err) {
			EXPECT_THAT(err.what(), ::testing::HasSubstr("Not found close")) << sql;
		}
	}
	EXPECT_THROW(std::ignore = reindexer::ExpressionAst::Parse(R"(arr || ['x])", false), reindexer::Error);
	EXPECT_THROW(std::ignore = reindexer::ExpressionAst::Parse(R"("a + 1)", false), reindexer::Error);
}

TEST(SqlEscaping, GeneratedSqlEscapesNamesAndStrings) {
	EXPECT_EQ(Query{"ns"}.Where(R"(a"b)", CondEq, R"(it's "q" \ )"s + "\n\t\x1b").GetSQL(),
			  R"(SELECT * FROM ns WHERE "a\"b" = 'it\'s "q" \\ \n\t\e')");
	EXPECT_EQ(Query{"ns"}.Where("a'b", CondEq, "\0\a\b\v\f\r"s).GetSQL(), R"(SELECT * FROM ns WHERE "a'b" = '\0\a\b\v\f\r')");
	EXPECT_EQ(Query{"ns"}.Where(R"(a\nb)", CondEq, R"(\q)").GetSQL(), R"(SELECT * FROM ns WHERE "a\\nb" = '\\q')");
	EXPECT_EQ(Query{"ns"}.Where("a\nb", CondEq, VariantArray{Variant{VariantArray::Create("x'y", 1)}}).GetSQL(),
			  R"(SELECT * FROM ns WHERE "a\nb" = {'x\'y', 1})");
	EXPECT_EQ(Query{"ns"}.Sort(R"("a\"b" + 'x')", SortOrder::Asc).GetSQL(), R"(SELECT * FROM ns ORDER BY '"a\\"b" + \'x\'')");
	EXPECT_EQ(Query{"ns"}.Sort(R"(a"b)", SortOrder::Asc, {"x'y"}).GetSQL(), R"(SELECT * FROM ns ORDER BY FIELD("a\"b", 'x\'y'))");
	EXPECT_EQ(Query{"ns"}.Set(R"(a"b)", {"x'y"}).GetSQL(), R"(UPDATE ns SET "a\"b" = 'x\'y')");
	EXPECT_EQ(Query{"ns"}.InnerJoin(Query{"other"}, R"(a"b)", CondEq, "id").GetSQL(),
			  R"(SELECT * FROM ns WHERE INNER JOIN other ON "ns.a\"b" = other.id)");
	EXPECT_EQ(Query{"ns"}.Where(ArithmeticExpression{R"("a""b"+flat_array_len("c\\d"))"}, CondEq, 1).GetSQL(),
			  R"(SELECT * FROM ns WHERE "a\"b"+flat_array_len("c\\d") = 1)");
}

TEST(SqlEscaping, NamesAndValuesRoundTrip) {
	using reindexer::functions::FlatArrayLen;
	using Builder = std::function<Query(const std::string&)>;
	struct {
		std::string_view description;
		Builder build;
		bool dslIsArithmetic{false};
	} const positions[]{
		{"field = value", [](const std::string& s) { return Query{"ns"}.Where(s, CondEq, s); }},
		{"field IN (values)", [](const std::string& s) { return Query{"ns"}.Where(s, CondSet, VariantArray::Create(s, "x"s)); }},
		{"field LIKE value", [](const std::string& s) { return Query{"ns"}.Where(s, CondLike, s); }},
		{"field = {composite}",
		 [](const std::string& s) { return Query{"ns"}.Where(s, CondEq, VariantArray{Variant{VariantArray::Create(s, 1)}}); }},
		{"field = id", [](const std::string& s) { return Query{"ns"}.WhereBetweenFields(s, CondEq, "id"); }},
		{"id < field", [](const std::string& s) { return Query{"ns"}.WhereBetweenFields("id", CondLt, s); }},
		{"field+1 = 1", [](const std::string& s) { return Query{"ns"}.Where(ArithmeticExpression(sqlName(s) + "+1"), CondEq, 1); }},
		{"flat_array_len(field) = 1", [](const std::string& s) { return Query{"ns"}.Where(FlatArrayLen(s), CondEq, 1); }, true},
		{"id = flat_array_len(field)", [](const std::string& s) { return Query{"ns"}.Where("id", CondEq, FlatArrayLen(s)); }},
		{"SELECT field", [](const std::string& s) { return Query{"ns"}.Select(s); }},
		{"MAX(field)", [](const std::string& s) { return Query{"ns"}.Aggregate(AggMax, {s}); }},
		{"(subquery) = value",
		 [](const std::string& s) { return Query{"ns"}.Where(Query{"other"}.Aggregate(AggMax, {s}), CondEq, VariantArray::Create(s)); }},
		{"equal_position",
		 [](const std::string& s) { return Query{"ns"}.Where(s, CondEq, 1).Where("arr", CondEq, 2).EqualPositions(s, "arr"); }},
		{"ORDER BY expression", [](const std::string& s) { return Query{"ns"}.Sort(sqlName(s), SortOrder::Desc); }},
		{"ORDER BY FIELD", [](const std::string& s) { return Query{"ns"}.Sort(s, SortOrder::Asc, {s, "x"s}); }},
		{"INNER JOIN ON", [](const std::string& s) { return Query{"ns"}.InnerJoin(Query{"other"}, s, CondEq, "id"); }},
		{"KNN", [](const std::string& s) { return Query{"ns"}.WhereKNN("vec", s, reindexer::HnswSearchParams{}.K(10).Ef(10)); }},
		{"UPDATE SET value", [](const std::string& s) { return Query{"ns"}.Set(s, {s}); }},
		{"UPDATE SET array",
		 [](const std::string& s) {
			 VariantArray values = VariantArray::Create(s, "x"s);
			 std::ignore = values.MarkArray();
			 return Query{"ns"}.Set(s, std::move(values));
		 }},
		{"UPDATE SET expression",
		 [](const std::string& s) { return Query{"ns"}.Set(s, {sqlName(s) + "||[" + sqlString(s) + "]"}, reindexer::HasExpression_True); }},
		{"UPDATE SET array_remove",
		 [](const std::string& s) {
			 return Query{"ns"}.Set(s, {"array_remove(" + sqlName(s) + ", ['x', " + sqlString(s) + "])"}, reindexer::HasExpression_True);
		 }},
		{"UPDATE SET object and value", [](const std::string& s) { return Query{"ns"}.SetObject(s, {R"({"x": "it's"})"}).Set("f", {s}); }},
		{"UPDATE SET value and object", [](const std::string& s) { return Query{"ns"}.Set("f", {s}).SetObject(s, {R"({"x": "it's"})"}); }},
		{"UPDATE DROP", [](const std::string& s) { return Query{"ns"}.Drop(s); }},
	};
	for (const auto& s : kSpecialStrings) {
		for (const auto& [description, build, dslIsArithmetic] : positions) {
			SCOPED_TRACE(fmt::format("{} with '{}'", description, s));
			const Query query = build(s);
			const auto sql = query.GetSQL();
			try {
				const Query fromSql = Query::FromSQL(sql);
				EXPECT_EQ(fromSql, query) << sql;
				EXPECT_EQ(fromSql.GetSQL(), sql);
			} catch (const std::exception& err) {
				ADD_FAILURE() << sql << ": " << err.what();
			}
			const auto dsl = query.GetJSON();
			try {
				const Query fromDsl = Query::FromJSON(dsl);
				EXPECT_EQ(fromDsl.GetSQL(), sql) << dsl;
				if (!dslIsArithmetic) {
					EXPECT_EQ(fromDsl, query) << dsl;
				}
			} catch (const std::exception& err) {
				ADD_FAILURE() << dsl << ": " << err.what();
			}
			try {
				EXPECT_EQ(binaryRoundTrip(query), query) << sql;
			} catch (const std::exception& err) {
				ADD_FAILURE() << "binary format of " << sql << ": " << err.what();
			}
		}
	}
}

TEST(SqlEscaping, QuotedPunctuationIsNotSyntax) {
	const std::pair<std::string_view, Query> valid[]{
		{R"(SELECT * FROM ns WHERE "(" = 1)", Query{"ns"}.Where("(", CondEq, 1)},
		{R"sql(SELECT * FROM ns WHERE ("(" = 1 OR ")" = 2) AND f = 3)sql",
		 Query{"ns"}.OpenBracket().Where("(", CondEq, 1).Or().Where(")", CondEq, 2).CloseBracket().Where("f", CondEq, 3)},
		{R"(SELECT * FROM ns WHERE f = '{')", Query{"ns"}.Where("f", CondEq, "{")},
		{R"(SELECT * FROM ns WHERE '{' = f)", Query{"ns"}.Where("f", CondEq, "{")},
		{R"(SELECT * FROM ns WHERE f LIKE '(')", Query{"ns"}.Where("f", CondLike, "(")},
		{R"(SELECT * FROM ns WHERE f LIKE '{')", Query{"ns"}.Where("f", CondLike, "{")},
		{R"(SELECT * FROM ns WHERE f IN (')', ',', '(', '{', '}'))",
		 Query{"ns"}.Where("f", CondSet, VariantArray::Create(")", ",", "(", "{", "}"))},
		{R"(SELECT * FROM ns WHERE "{" + 1 = 2)", Query{"ns"}.Where(ArithmeticExpression{R"("{" + 1)"}, CondEq, 2)},
		{R"(SELECT * FROM ns WHERE "+" + 1 = 2)", Query{"ns"}.Where(ArithmeticExpression{R"("+" + 1)"}, CondEq, 2)},
		{R"(SELECT * FROM ns WHERE f = (SELECT "=" FROM other))", Query{"ns"}.Where("f", CondEq, Query{"other"}.Select("="))},
		{R"(SELECT * FROM ns WHERE f = (SELECT "in" FROM other))", Query{"ns"}.Where("f", CondEq, Query{"other"}.Select("in"))},
		{R"(SELECT * FROM ns INNER JOIN other ON "ns.(" = other.id)", Query{"ns"}.InnerJoin(Query{"other"}, "(", CondEq, "id")},
		{R"(SELECT * FROM ns ORDER BY FIELD(f, '{', ')', ','))", Query{"ns"}.Sort("f", SortOrder::Asc, {"{", ")", ","})},
		{R"(SELECT * FROM ns WHERE KNN(vec, '[', k=10, ef=10))",
		 Query{"ns"}.WhereKNN("vec", "["s, reindexer::HnswSearchParams{}.K(10).Ef(10))},
		{R"(UPDATE ns SET f = '[')", Query{"ns"}.Set("f", {"["})},
		{R"(UPDATE ns SET f = '{')", Query{"ns"}.Set("f", {"{"})},
		{R"(UPDATE ns SET f = "{")", Query{"ns"}.Set("f", {R"("{")"}, reindexer::HasExpression_True)},
		{R"(UPDATE ns SET f = "[")", Query{"ns"}.Set("f", {R"("[")"}, reindexer::HasExpression_True)},
		{R"(UPDATE ns SET f = "," WHERE id = 1)", Query{"ns"}.Set("f", {R"(",")"}, reindexer::HasExpression_True).Where("id", CondEq, 1)},
		{R"(UPDATE ns SET f = "a" || ";", g = 1)",
		 Query{"ns"}.Set("f", {R"("a" || ";")"}, reindexer::HasExpression_True).Set("g", {int64_t(1)})},
	};
	for (const auto& [sql, expected] : valid) {
		SCOPED_TRACE(sql);
		try {
			const Query parsed = Query::FromSQL(sql);
			EXPECT_EQ(parsed, expected) << parsed.GetSQL() << " vs " << expected.GetSQL();
			EXPECT_EQ(Query::FromSQL(parsed.GetSQL()), parsed) << parsed.GetSQL();
		} catch (const std::exception& err) {
			ADD_FAILURE() << err.what();
		}
	}

	const auto arrayQuery = Query::FromSQL("UPDATE ns SET arr = ['[', ']', ',', '{', '}']");
	ASSERT_EQ(Impl(arrayQuery).UpdateFields().size(), 1);
	EXPECT_EQ(Impl(arrayQuery).UpdateFields().front().Values(), VariantArray::Create("[", "]", ",", "{", "}"));

	constexpr std::string_view kInvalid[]{
		R"(SELECT * FROM ns WHERE f IN ('a' ',' 'b'))",
		R"(SELECT * FROM ns WHERE f IN ('a' ')')",
		R"(SELECT * FROM ns WHERE f IN '(' 'a'))",
		R"(SELECT * FROM ns WHERE f = {1 ',' 2})",
		R"(SELECT * FROM ns WHERE f = {1 '}')",
		R"(SELECT * FROM ns WHERE f '=' 1)",
		R"(SELECT * FROM ns WHERE f 'in' (1, 2))",
		R"(SELECT * FROM ns WHERE (f = 1 ')')",
		R"(SELECT * FROM ns WHERE flat_array_len '(' f) = 1)",
		R"(SELECT * FROM ns WHERE flat_array_len(f ')' = 1)",
		R"(SELECT * FROM ns INNER JOIN other ON id '=' other.id)",
		R"(SELECT * FROM ns INNER JOIN '(' SELECT * FROM other) ON id = other.id)",
		R"(SELECT a ',' b FROM ns)",
		R"(SELECT MAX '(' a) FROM ns)",
		R"(SELECT * FROM ns ORDER BY a ',' b)",
		R"(SELECT * FROM ns ORDER BY FIELD '(' a, 1))",
		R"(SELECT * FROM ns ORDER BY FIELD(a ',' 1))",
		R"(SELECT * FROM ns WHERE KNN '(' vec, [1], k=10))",
		R"(SELECT * FROM ns WHERE KNN(vec, [1 ',' 2], k=10))",
		R"(SELECT * FROM ns WHERE KNN(vec, [1 ']', k=10))",
		R"(SELECT * FROM ns WHERE KNN(vec, [1], k '=' 10))",
		R"(SELECT * FROM ns ';')",
		R"(UPDATE ns SET f '=' 1)",
		R"(UPDATE ns SET arr = ['a' ',' 'b'])",
		R"(UPDATE ns SET arr = ['a' ']')",
		R"(UPDATE ns SET arr = array_remove '(' arr, 'a'))",
		R"(UPDATE ns SET arr = array_remove(arr ',' 'a'))",
		R"(UPDATE ns SET arr = array_remove(arr, 'a' ')')",
		R"(UPDATE ns SET arr = array_remove(arr, 'a') '|' '|' arr)",
		R"(UPDATE ns DROP a ',' b)",
	};
	for (const auto sql : kInvalid) {
		EXPECT_THROW(std::ignore = Query::FromSQL(sql), reindexer::Error) << sql;
	}
	EXPECT_THROW(std::ignore = reindexer::ExpressionAst::Parse(R"(a '+' 1)", false), reindexer::Error);
	EXPECT_THROW(std::ignore = reindexer::ExpressionAst::Parse(R"(a "+" 1)", false), reindexer::Error);
	EXPECT_THROW(std::ignore = reindexer::ExpressionAst::Parse(R"(arr |'|' ['x'])", false), reindexer::Error);
	EXPECT_THROW(std::ignore = reindexer::ExpressionAst::Parse(R"(arr || ['x' ',' 'y'])", false), reindexer::Error);
	EXPECT_THROW(std::ignore = reindexer::ExpressionAst::Parse(R"(now '(' ))", false), reindexer::Error);
}

TEST(SqlEscaping, ConditionKeywordsInSubquerySelect) {
	for (const auto keyword : {"is", "in", "range", "like", "allset", "IN"}) {
		const Query queries[]{Query{"ns"}.Where("f", CondEq, Query{"other"}.Select(keyword)),
							  Query{"ns"}.Where("f", CondSet, Query{"other"}.Select(keyword)),
							  Query{"ns"}.Where(Query{"other"}.Select(keyword), CondEq, VariantArray::Create(int64_t(1)))};
		for (const auto& query : queries) {
			const auto sql = query.GetSQL();
			SCOPED_TRACE(sql);
			try {
				EXPECT_EQ(Query::FromSQL(sql), query);
			} catch (const std::exception& err) {
				ADD_FAILURE() << err.what();
			}
		}
	}
}

TEST(SqlEscaping, ObjectsAndScalarsInOneUpdate) {
	const Query query = Query{"ns"}
							.Set("a", {"it's"})
							.SetObject("obj", {R"({"x":"it's \"q\""})"})
							.Set("b", {R"({"x":1})"})
							.SetObject("arr", {R"({"y":1})", R"({"y":"'"})"})
							.Set("c", {R"("a" || ['x'])"}, reindexer::HasExpression_True)
							.Set("d", {R"(\)"});
	const auto sql = query.GetSQL();
	EXPECT_EQ(
		sql,
		R"(UPDATE ns SET a = 'it\'s', obj = {"x":"it's \"q\""}, b = '{"x":1}', arr = [{"y":1}, {"y":"'"}], c = "a" || ['x'], d = '\\')");
	const Query reparsed = Query::FromSQL(sql);
	EXPECT_EQ(reparsed, query) << reparsed.GetSQL();
	EXPECT_EQ(reparsed.GetSQL(), sql);

	const Query objectFirst = Query{"ns"}.SetObject("obj", {R"({"x":1})"}).Set("a", {"it's"});
	EXPECT_EQ(objectFirst.GetSQL(), R"(UPDATE ns SET obj = {"x":1}, a = 'it\'s')");
	EXPECT_EQ(Query::FromSQL(objectFirst.GetSQL()), objectFirst);
}

TEST(SqlEscaping, ReadyNamesAreNotDecodedButExpressionsAre) {
	const auto fieldOf = [](const ArithmeticExpression& expr) {
		const auto fields = expr.Ast().ReferencedFields();
		EXPECT_EQ(fields.size(), 1);
		return fields.empty() ? std::string{} : std::string{fields.front()};
	};
	EXPECT_EQ(fieldOf(ArithmeticExpression{R"("a""b" + 1)"}), R"(a"b)");
	EXPECT_EQ(fieldOf(ArithmeticExpression{R"("a\"b" + 1)"}), R"(a"b)");
	EXPECT_EQ(fieldOf(ArithmeticExpression{R"("a\\nb" + 1)"}), R"(a\nb)");
	EXPECT_EQ(fieldOf(ArithmeticExpression{R"("a\nb" + 1)"}), "a\nb");
	EXPECT_EQ(fieldOf(ArithmeticExpression{R"("a\\\\nb" + 1)"}), R"(a\\nb)");
	EXPECT_EQ(fieldOf(ArithmeticExpression{R"("a\qb" + 1)"}), R"(a\qb)");

	const Query byName = Query{"ns"}.Where(R"(a\nb)", CondEq, 10);
	EXPECT_EQ(Query::FromSQL(R"(SELECT * FROM ns WHERE "a\\nb" = 10)"), byName);
	EXPECT_EQ(Query::FromJSON(R"json({"namespace":"ns","filters":[{"cond":"EQ","field":"a\\nb","value":10}]})json"), byName);
	EXPECT_EQ(
		Query::FromJSON(
			R"json({"namespace":"ns","filters":[{"cond":"EQ","left_expression":{"type":"field","value":"a\\nb"},"right_expression":{"type":"values","value":10}}]})json"),
		byName);
	EXPECT_NE(Query::FromSQL(R"(SELECT * FROM ns WHERE "a\nb" = 10)"), byName);
	EXPECT_EQ(Query::FromSQL(R"(SELECT * FROM ns WHERE "a\nb" = 10)"), Query{"ns"}.Where("a\nb", CondEq, 10));

	const Query byExpression = Query{"ns"}.Where(ArithmeticExpression{R"("a\\nb" + 1)"}, CondEq, 10);
	EXPECT_EQ(
		Query::FromJSON(
			R"json({"namespace":"ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"a\\\\nb\" + 1"},"right_expression":{"type":"values","value":10}}]})json"),
		byExpression);
	EXPECT_NE(
		Query::FromJSON(
			R"json({"namespace":"ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"a\\nb\" + 1"},"right_expression":{"type":"values","value":10}}]})json"),
		byExpression);

	const Query byValue = Query{"ns"}.Where("f", CondEq, R"(a\nb)");
	EXPECT_EQ(Query::FromJSON(R"json({"namespace":"ns","filters":[{"cond":"EQ","field":"f","value":"a\\nb"}]})json"), byValue);
	EXPECT_EQ(Query::FromSQL(R"(SELECT * FROM ns WHERE f = 'a\\nb')"), byValue);
	EXPECT_EQ(byValue.GetSQL(), R"(SELECT * FROM ns WHERE f = 'a\\nb')");

	const Query updateByValue = Query{"ns"}.Set(R"(a\nb)", {R"(c\nd)"});
	EXPECT_EQ(Query::FromJSON(
				  R"json({"namespace":"ns","type":"update","update_fields":[{"name":"a\\nb","type":"value","values":["c\\nd"]}]})json"),
			  updateByValue);
	EXPECT_EQ(Query::FromSQL(R"(UPDATE ns SET "a\\nb" = 'c\\nd')"), updateByValue);
}

TEST(SqlEscaping, SameFieldNameInSqlDslAndApi) {
	const Query byName = Query{"ns"}.Where(R"(a"b)", CondEq, 10);
	EXPECT_EQ(Query{"ns"}.Where("a\"b", CondEq, 10), byName);
	EXPECT_EQ(Query::FromSQL(R"(SELECT * FROM ns WHERE "a""b" = 10)"), byName);
	EXPECT_EQ(Query::FromSQL(R"(SELECT * FROM ns WHERE "a\"b" = 10)"), byName);
	EXPECT_EQ(
		Query::FromJSON(
			R"json({"namespace":"ns","filters":[{"cond":"EQ","left_expression":{"type":"field","value":"a\"b"},"right_expression":{"type":"values","value":10}}]})json"),
		byName);
	EXPECT_EQ(byName.GetSQL(), R"(SELECT * FROM ns WHERE "a\"b" = 10)");

	const Query byExpression = Query{"ns"}.Where(ArithmeticExpression{R"("a""b" + 1)"}, CondEq, 10);
	EXPECT_EQ(Query{"ns"}.Where(ArithmeticExpression{R"("a\"b" + 1)"}, CondEq, 10), byExpression);
	EXPECT_EQ(Query::FromSQL(R"(SELECT * FROM ns WHERE "a""b" + 1 = 10)"), byExpression);
	EXPECT_EQ(Query::FromSQL(R"(SELECT * FROM ns WHERE "a\"b" + 1 = 10)"), byExpression);
	EXPECT_EQ(
		Query::FromJSON(
			R"json({"namespace":"ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"a\"\"b\" + 1"},"right_expression":{"type":"values","value":10}}]})json"),
		byExpression);
	EXPECT_EQ(
		Query::FromJSON(
			R"json({"namespace":"ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"a\\\"b\" + 1"},"right_expression":{"type":"values","value":10}}]})json"),
		byExpression);
	EXPECT_EQ(byExpression.GetSQL(), R"(SELECT * FROM ns WHERE "a\"b"+1 = 10)");
	EXPECT_THAT(byExpression.GetJSON(), ::testing::HasSubstr(R"json("type":"expression","value":"\"a\\\"b\"+1")json"));
}

TEST(SqlEscaping, UpdateExpressionsKeepEscapes) {
	const std::pair<std::string_view, std::string_view> cases[]{
		{R"(UPDATE ns SET arr = arr || ['fie\'ld'])", R"(arr || ['fie\'ld'])"},
		{R"(UPDATE ns SET arr = arr||['fie''ld', 'a"b'])", R"(arr||['fie''ld', 'a"b'])"},
		{R"(UPDATE ns SET arr = "a\"b" || ['x\\y'])", R"("a\"b" || ['x\\y'])"},
		{R"(UPDATE ns SET arr = "a""b" || "c\nd")", R"("a""b" || "c\nd")"},
		{R"(UPDATE ns SET arr = ['fie\'ld'] || arr)", R"(['fie\'ld'] || arr)"},
		{R"(UPDATE ns SET arr = array_remove(arr, 'fie\'ld'))", R"(array_remove(arr, 'fie\'ld'))"},
		{R"(UPDATE ns SET arr = arr || ['fie\'ld'] WHERE id = 1)", R"(arr || ['fie\'ld'])"},
		{R"(UPDATE ns SET arr = ['fie\'ld'] || arr   WHERE id = 1)", R"(['fie\'ld'] || arr)"},
		{R"(UPDATE ns SET arr = array_remove(arr, 'fie\'ld')  WHERE id = 1)", R"(array_remove(arr, 'fie\'ld'))"},
		{"UPDATE ns SET f = a - -1 WHERE id = 1", "a - -1"},
		{"UPDATE ns SET f = a - -- comment\n -1 WHERE id = 1", "a - -1"},
		{"UPDATE ns SET arr = ['x'] || arr -- comment\nWHERE id = 1", "['x'] || arr"},
		{"UPDATE ns SET f = a--1", "a"},
	};
	for (const auto& [sql, expression] : cases) {
		SCOPED_TRACE(sql);
		try {
			const Query query = Query::FromSQL(sql);
			const auto impl = Impl(query);
			ASSERT_EQ(impl.UpdateFields().size(), 1);
			const auto& entry = impl.UpdateFields().front();
			EXPECT_TRUE(entry.IsExpression());
			ASSERT_EQ(entry.Values().size(), 1);
			EXPECT_EQ(entry.Values().front().As<std::string>(), expression);
			const auto generated = query.GetSQL();
			const Query reparsed = Query::FromSQL(generated);
			EXPECT_EQ(reparsed, query) << generated;
			EXPECT_EQ(reparsed.GetSQL(), generated);
			EXPECT_EQ(Query::FromJSON(query.GetJSON()), query) << query.GetJSON();
		} catch (const std::exception& err) {
			ADD_FAILURE() << err.what();
		}
	}

	const auto ast = reindexer::ExpressionAst::Parse(R"(arr || ['fie\'ld', 'a''b', 'c\\d', '\q'])", false);
	ASSERT_TRUE(ast);
	ASSERT_EQ(ast.Root()->Type(), reindexer::ExprNodeType::Binary);
	const auto& concat = static_cast<const reindexer::ExprBinary&>(*ast.Root());
	ASSERT_EQ(concat.right->Type(), reindexer::ExprNodeType::ArrayLiteral);
	EXPECT_EQ(static_cast<const reindexer::ExprArrayLiteral&>(*concat.right).values,
			  VariantArray::Create("fie'ld", "a'b", R"(c\d)", R"(\q)"));
	EXPECT_EQ(ast.Root()->Dump(), R"(arr||['fie\'ld','a\'b','c\\d','\\q'])");
	EXPECT_EQ(reindexer::ExpressionAst::Parse(ast.Root()->Dump(), false).Root()->Dump(), ast.Root()->Dump());
}

namespace reindexer_tests {

using reindexer::IndexOpts;

class [[nodiscard]] SqlEscapingApi : public ReindexerApi {
public:
	void SetUp() override {
		ReindexerApi::SetUp();
		rt.OpenNamespace(kNs);
		DefineNamespaceDataset(kNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0},
									 IndexDeclaration{"arr", "hash", "string", IndexOpts().Array(), 0}});
	}

	std::vector<int> SelectIds(const Query& q) {
		auto qr = rt.Select(q);
		return ids(qr);
	}
	std::vector<int> SelectIds(std::string_view sql) {
		auto qr = rt.ExecSQL(sql);
		return ids(qr);
	}
	std::vector<std::string> Arr(int id) {
		auto qr = rt.Select(Query{kNs}.Where("id", CondEq, id));
		EXPECT_EQ(qr.Count(), 1);
		if (qr.Count() != 1) {
			return {};
		}
		Item item = qr.begin().GetItem(false);
		std::vector<std::string> result;
		for (const auto& v : VariantArray(item["arr"])) {
			result.emplace_back(v.As<std::string>());
		}
		return result;
	}

	std::string Json(int id) {
		auto qr = rt.Select(Query{kNs}.Where("id", CondEq, id));
		EXPECT_EQ(qr.Count(), 1);
		if (qr.Count() != 1) {
			return {};
		}
		reindexer::WrSerializer ser;
		const auto err = qr.begin().GetJSON(ser, false);
		EXPECT_TRUE(err.ok()) << err.what();
		return std::string{ser.Slice()};
	}

	static constexpr std::string_view kNs = "escaping_ns";

private:
	static std::vector<int> ids(QueryResults& qr) {
		std::vector<int> result;
		for (auto& it : qr) {
			Item item = it.GetItem(false);
			result.emplace_back(item["id"].As<int>());
		}
		std::ranges::sort(result);
		return result;
	}
};

TEST_F(SqlEscapingApi, EquivalentSelectionsAcrossSqlDslAndApi) {
	rt.UpsertJSON(kNs, R"json({"id":1,"len":2,"a\"b":10,"a\\nb":20,"a\nb":30,"a\"rr":[1,2],"s":"it's \"quoted\" \\n"})json");
	rt.UpsertJSON(kNs, R"json({"id":2,"len":2,"a\"b":11,"a\\nb":21,"a\nb":31,"a\"rr":[1,2,3],"s":"it's \"quoted\" \n"})json");
	const std::vector<int> first{1};

	struct {
		std::string_view description;
		std::vector<Query> queries;
		std::vector<std::string_view> sqls;
	} const cases[]{
		{"name with a double quote",
		 {Query{kNs}.Where(R"(a"b)", CondEq, 10), Query{kNs}.Where(ArithmeticExpression{R"("a""b" + 1)"}, CondEq, 11),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"field","value":"a\"b"},"right_expression":{"type":"values","value":10}}]})json"),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"a\\\"b\" + 1"},"right_expression":{"type":"values","value":11}}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE "a""b" = 10)", R"(SELECT * FROM escaping_ns WHERE "a\"b" = 10)",
		  R"(SELECT * FROM escaping_ns WHERE "a""b" + 1 = 11)", R"(SELECT * FROM escaping_ns WHERE "a\"b" + 1 = 11)"}},
		{"flat_array_len of a name with a double quote",
		 {Query{kNs}.Where(reindexer::functions::FlatArrayLen(R"(a"rr)"), CondEq, 2),
		  Query{kNs}.Where("len", CondEq, reindexer::functions::FlatArrayLen(R"(a"rr)")),
		  Query{kNs}.Where(ArithmeticExpression{R"(flat_array_len("a""rr") + 0)"}, CondEq, 2),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"flat_array_len(\"a\\\"rr\")"},"right_expression":{"type":"values","value":2}}]})json"),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"flat_array_len(\"a\"\"rr\") + 0"},"right_expression":{"type":"values","value":2}}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE flat_array_len("a""rr") = 2)",
		  R"(SELECT * FROM escaping_ns WHERE flat_array_len("a\"rr") = 2)",
		  R"(SELECT * FROM escaping_ns WHERE 2 = flat_array_len("a""rr"))",
		  R"(SELECT * FROM escaping_ns WHERE flat_array_len("a\"rr") + 0 = 2)",
		  R"(SELECT * FROM escaping_ns WHERE len = flat_array_len("a""rr"))",
		  R"(SELECT * FROM escaping_ns WHERE flat_array_len("a\"rr") = len)"}},
		{"name with a backslash and n",
		 {Query{kNs}.Where(R"(a\nb)", CondEq, 20), Query{kNs}.Where(ArithmeticExpression{R"("a\\nb" + 1)"}, CondEq, 21),
		  Query::FromJSON(R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","field":"a\\nb","value":20}]})json"),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"a\\\\nb\" + 1"},"right_expression":{"type":"values","value":21}}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE "a\\nb" = 20)", R"(SELECT * FROM escaping_ns WHERE "a\\nb" + 1 = 21)"}},
		{"name with a line feed",
		 {Query{kNs}.Where("a\nb", CondEq, 30), Query{kNs}.Where(ArithmeticExpression{R"("a\nb" + 1)"}, CondEq, 31),
		  Query::FromJSON(R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","field":"a\nb","value":30}]})json"),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"a\\nb\" + 1"},"right_expression":{"type":"values","value":31}}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE "a\nb" = 30)", R"(SELECT * FROM escaping_ns WHERE "a\nb" + 1 = 31)"}},
		{"string value",
		 {Query{kNs}.Where("s", CondEq, R"(it's "quoted" \n)"),
		  Query::FromJSON(R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","field":"s","value":"it's \"quoted\" \\n"}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE s = 'it\'s "quoted" \\n')", R"(SELECT * FROM escaping_ns WHERE s = 'it''s "quoted" \\n')",
		  R"(SELECT * FROM escaping_ns WHERE s = 'it\'s \"quoted\" \\n')"}},
	};
	for (const auto& [description, queries, sqls] : cases) {
		SCOPED_TRACE(description);
		for (const auto& q : queries) {
			EXPECT_EQ(SelectIds(q), first) << q.GetSQL();
			EXPECT_EQ(SelectIds(q.GetSQL()), first) << q.GetSQL();
			EXPECT_EQ(SelectIds(Query::FromJSON(q.GetJSON())), first) << q.GetJSON();
			EXPECT_EQ(SelectIds(binaryRoundTrip(q)), first) << q.GetSQL();
		}
		for (const auto& sql : sqls) {
			EXPECT_EQ(SelectIds(sql), first) << sql;
		}
	}
}

TEST_F(SqlEscapingApi, QuotedPunctuationInSelections) {
	rt.UpsertJSON(kNs, R"json({"id":1,"+":1,"(":10,"s":"{","arr":[","]})json");
	rt.UpsertJSON(kNs, R"json({"id":2,"+":2,"(":20,"s":"}","arr":["]"]})json");
	const std::vector<int> first{1};

	struct {
		std::string_view description;
		std::vector<Query> queries;
		std::vector<std::string_view> sqls;
	} const cases[]{
		{"name that looks like an operator",
		 {Query{kNs}.Where("+", CondEq, 1), Query{kNs}.Where(ArithmeticExpression{R"("+" + 1)"}, CondEq, 2),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"+\" + 1"},"right_expression":{"type":"values","value":2}}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE "+" = 1)", R"(SELECT * FROM escaping_ns WHERE "+" + 1 = 2)",
		  R"(SELECT * FROM escaping_ns WHERE 2 = "+" + 1)"}},
		{"name that looks like a bracket",
		 {Query{kNs}.Where("(", CondEq, 10), Query{kNs}.Where(ArithmeticExpression{R"("(" * 2)"}, CondEq, 20),
		  Query::FromJSON(
			  R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","left_expression":{"type":"expression","value":"\"(\" * 2"},"right_expression":{"type":"values","value":20}}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE "(" = 10)", R"(SELECT * FROM escaping_ns WHERE ("(" = 10))",
		  R"(SELECT * FROM escaping_ns WHERE "(" * 2 = 20)"}},
		{"string that looks like a brace",
		 {Query{kNs}.Where("s", CondEq, "{"),
		  Query::FromJSON(R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","field":"s","value":"{"}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE s = '{')", R"(SELECT * FROM escaping_ns WHERE '{' = s)",
		  R"(SELECT * FROM escaping_ns WHERE s IN ('{', ','))", R"(SELECT * FROM escaping_ns WHERE s LIKE '{')"}},
		{"string that looks like a comma",
		 {Query{kNs}.Where("arr", CondEq, ","),
		  Query::FromJSON(R"json({"namespace":"escaping_ns","filters":[{"cond":"EQ","field":"arr","value":","}]})json")},
		 {R"(SELECT * FROM escaping_ns WHERE arr = ',')", R"(SELECT * FROM escaping_ns WHERE arr IN (',', ')'))"}},
	};
	for (const auto& [description, queries, sqls] : cases) {
		SCOPED_TRACE(description);
		for (const auto& q : queries) {
			EXPECT_EQ(SelectIds(q), first) << q.GetSQL();
			EXPECT_EQ(SelectIds(q.GetSQL()), first) << q.GetSQL();
			EXPECT_EQ(SelectIds(Query::FromJSON(q.GetJSON())), first) << q.GetJSON();
		}
		for (const auto& sql : sqls) {
			EXPECT_EQ(SelectIds(sql), first) << sql;
		}
	}
}

TEST_F(SqlEscapingApi, ObjectsAndScalarsInOneUpdate) {
	for (const auto sql : {R"(UPDATE escaping_ns SET obj = {"x":"it's"}, s = 'it\'s "q"' WHERE id = 1)",
						   R"(UPDATE escaping_ns SET s = 'it\'s "q"', obj = {"x":"it's"} WHERE id = 1)"}) {
		for (const auto& query : {std::string{sql}, Query::FromSQL(sql).GetSQL()}) {
			SCOPED_TRACE(query);
			rt.UpsertJSON(kNs, R"json({"id":1,"arr":[],"obj":{"x":"old"},"s":"old"})json");
			std::ignore = rt.ExecSQL(query);
			EXPECT_EQ(Json(1), R"json({"id":1,"arr":[],"obj":{"x":"it's"},"s":"it's \"q\""})json");
		}
	}
}

TEST_F(SqlEscapingApi, UpdateWithEscapedStrings) {
	const auto reset = [&] { rt.UpsertJSON(kNs, R"json({"id":1,"arr":["word","fie'ld","fie\\'ld"]})json"); };
	const auto update = [&](std::string_view sql) { std::ignore = rt.ExecSQL(sql); };
	const auto updateDsl = [&](std::string_view expression) {
		std::ignore = rt.Update(Query::FromJSON(fmt::format(
			R"json({{"namespace":"escaping_ns","type":"update","update_fields":[{{"name":"arr","type":"expression","values":["{}"]}}]}})json",
			reindexer::escapeString(expression))));
	};
	const std::vector<std::string> original{"word", "fie'ld", R"(fie\'ld)"};

	struct {
		std::string_view sql;
		std::string_view dslExpression;
		std::vector<std::string> expected;
	} const cases[]{
		{R"(UPDATE escaping_ns SET arr = arr || ['fie\'ld'])", R"(arr || ['fie\'ld'])", {"word", "fie'ld", R"(fie\'ld)", "fie'ld"}},
		{R"(UPDATE escaping_ns SET arr = ['fie''ld'] || arr WHERE id = 1)",
		 R"(['fie''ld'] || arr)",
		 {"fie'ld", "word", "fie'ld", R"(fie\'ld)"}},
		{R"(UPDATE escaping_ns SET arr = arr || ['fie\\\'ld', '\q'])",
		 R"(arr || ['fie\\\'ld', '\q'])",
		 {"word", "fie'ld", R"(fie\'ld)", R"(fie\'ld)", R"(\q)"}},
		{R"(UPDATE escaping_ns SET arr = array_remove(arr, 'fie\'ld') WHERE id = 1)",
		 R"(array_remove(arr, 'fie\'ld'))",
		 {"word", R"(fie\'ld)"}},
		{R"(UPDATE escaping_ns SET arr = array_remove(arr, 'fie\\\'ld'))", R"(array_remove(arr, 'fie\\\'ld'))", {"word", "fie'ld"}},
		{R"(UPDATE escaping_ns SET arr = array_remove(arr, ['word', 'fie''ld']) || ['\\'])",
		 R"(array_remove(arr, ['word', 'fie''ld']) || ['\\'])",
		 {R"(fie\'ld)", R"(\)"}},
		{R"(UPDATE escaping_ns SET arr = arr || [','])", "arr || [',']", {"word", "fie'ld", R"(fie\'ld)", ","}},
		{R"(UPDATE escaping_ns SET arr = arr || ['x', ']', 'y'])",
		 "arr || ['x', ']', 'y']",
		 {"word", "fie'ld", R"(fie\'ld)", "x", "]", "y"}},
		{R"(UPDATE escaping_ns SET arr = ['[', '+', '|'] || arr WHERE id = 1)",
		 "['[', '+', '|'] || arr",
		 {"[", "+", "|", "word", "fie'ld", R"(fie\'ld)"}},
		{R"(UPDATE escaping_ns SET arr = array_remove(arr, ['word', ')', ',']) || [';'])",
		 "array_remove(arr, ['word', ')', ',']) || [';']",
		 {"fie'ld", R"(fie\'ld)", ";"}},
	};
	for (const auto& [sql, dslExpression, expected] : cases) {
		SCOPED_TRACE(sql);
		reset();
		ASSERT_EQ(Arr(1), original);
		update(sql);
		EXPECT_EQ(Arr(1), expected);

		reset();
		updateDsl(dslExpression);
		EXPECT_EQ(Arr(1), expected) << dslExpression;

		reset();
		update(Query::FromSQL(sql).GetSQL());
		EXPECT_EQ(Arr(1), expected) << Query::FromSQL(sql).GetSQL();
	}
}

}  // namespace reindexer_tests
