#include <gtest/gtest.h>

#include "core/query/query.h"
#include "core/query/sql/sqlparser.h"

struct [[nodiscard]] SQLParserTests : public ::testing::TestWithParam<std::string> {};

// --gtest_filter=*/SQLParserTests.NumberInDoubleQuotesParsedCorrectly/*
TEST_P(SQLParserTests, NumberInDoubleQuotesParsedCorrectly) { EXPECT_NO_THROW(auto q = reindexer::SQLParser::Parse(GetParam())); }

// --gtest_filter=Update/SQLParserTests.NumberInDoubleQuotesParsedCorrectly/*
INSTANTIATE_TEST_SUITE_P(Update, SQLParserTests,
						 ::testing::Values("UPDATE test_namespace DROP \"123\"", "UPDATE test_namespace DROP \"123abc\"",
										   "UPDATE test_namespace DROP \"123abc123\"",
										   "UPDATE test_namespace DROP  \"123\", \"123abc\", \"123abc123\"",
										   "UPDATE test_namespace SET \"123\" = \"123\"",
										   "UPDATE test_namespace SET \"123abc\" = \"123abc\"",
										   "UPDATE test_namespace SET \"123abc123\" = \"123abc123\"",
										   "UPDATE test_namespace SET"
										   " \"123\" = \"123\","
										   " \"123abc\" = \"123abc\","
										   " \"123abc123\" = \"123abc123\","
										   " \"123abc\" = \"123\","
										   " \"123abc123\" = \"123abc\","));

// --gtest_filter=Select/SQLParserTests.NumberInDoubleQuotesParsedCorrectly/*
INSTANTIATE_TEST_SUITE_P(Select, SQLParserTests,
						 ::testing::Values("SELECT * FROM ns WHERE \"123\" = 'some_value'", "SELECT * FROM ns WHERE \"123\" = 123",
										   "SELECT * FROM ns WHERE \"123abc\" = 123",
										   "SELECT * FROM ns WHERE \"123abc123\" = 123 AND \"123\" = 'some_value'",
										   "SELECT * FROM ns WHERE \"123\" = 123 AND \"abc123\" = 'some_value' AND \"123abc123\" = 123",
										   "SELECT * FROM ns WHERE \"123\" = 123 AND \"abc123\" = \"123abc123\" AND \"123abc123\" = 123"));

// --gtest_filter=SelectAndOrderBy/SQLParserTests.NumberInDoubleQuotesParsedCorrectly/*
INSTANTIATE_TEST_SUITE_P(SelectAndOrderBy, SQLParserTests,
						 ::testing::Values(R"(SELECT * FROM ns ORDER BY "123")", R"(SELECT * FROM ns ORDER BY "123abc")",
										   R"(SELECT * FROM ns ORDER BY "123abc123")",
										   R"(SELECT * FROM ns ORDER BY "123", "123abc", "123abc123")",
										   R"(SELECT * FROM ns ORDER BY "123" ASC)", R"(SELECT * FROM ns ORDER BY "123abc" ASC)",
										   R"(SELECT * FROM ns ORDER BY '5 * "123"')", R"(SELECT * FROM ns ORDER BY '5 * "123abc"')",
										   R"(SELECT * FROM ns ORDER BY "123abc123" ASC)",
										   R"(SELECT * FROM ns ORDER BY "123" ASC, "123abc" ASC, "123abc123" ASC)",
										   R"(SELECT * FROM ns ORDER BY '"123" + 123')", R"(SELECT * FROM ns ORDER BY '123 + "123"')",
										   R"(SELECT * FROM ns ORDER BY '"ns.123" + 123')", R"(SELECT * FROM ns ORDER BY '123 + "ns.123"')",
										   R"(SELECT * FROM ns ORDER BY "123" DESC, "123abc" DESC, "123abc123" DESC)")

);

// --gtest_filter=Delete/SQLParserTests.NumberInDoubleQuotesParsedCorrectly/*
INSTANTIATE_TEST_SUITE_P(Delete, SQLParserTests,
						 ::testing::Values("DELETE FROM ns WHERE \"123\" = 'some_value'", "DELETE FROM ns WHERE \"123\" = 123",
										   "DELETE FROM ns WHERE \"123abc\" = 123",
										   "DELETE FROM ns WHERE \"123abc123\" = 123 AND \"123\" = 'some_value'",
										   "DELETE FROM ns WHERE \"123\" = 123 AND \"abc123\" = 'some_value' AND \"123abc123\" = 123",
										   "DELETE FROM ns WHERE \"123\" = 123 AND \"abc123\" = \"123abc123\" AND \"123abc123\" = 123"));

struct [[nodiscard]] SQLParserWhere : public ::testing::TestWithParam<std::pair<std::string, std::string>> {};

TEST_P(SQLParserWhere, CheckWhereError) {
	auto [sql, errString] = GetParam();
	try {
		auto q = reindexer::SQLParser::Parse(sql);
		FAIL() << "[" << sql << "] is parsed";
	} catch (reindexer::Error& err) {
		EXPECT_EQ(err.what(), errString);
	} catch (...) {
		FAIL() << "Unknown error";
	}
}

// --gtest_filter=TestsSqlWhere/SQLParserWhere.CheckWhereError/*
INSTANTIATE_TEST_SUITE_P(
	TestsSqlWhere, SQLParserWhere,
	::testing::Values(
		std::pair{"select * from ns where", "Expected condition after 'WHERE'"},
		std::pair{"select * from ns where 123", "Number is invalid at this location. (text = '123'  location = line: 1 column: 23 26)"},
		std::pair{"select * from ns where 'abc'", "String is invalid at this location. (text = 'abc'  location = line: 1 column: 24 27)"},
		std::pair{"delete from ns where", "Expected condition after 'WHERE'"},
		std::pair{"delete from ns where 123", "Number is invalid at this location. (text = '123'  location = line: 1 column: 21 24)"},
		std::pair{"delete from ns where 'abc'", "String is invalid at this location. (text = 'abc'  location = line: 1 column: 22 25)"},
		std::pair{"update ns set a=1 where", "Expected condition after 'WHERE'"},
		std::pair{"update ns set a=1 where 123", "Number is invalid at this location. (text = '123'  location = line: 1 column: 24 27)"},
		std::pair{"update ns set a=1 where 'abc'", "String is invalid at this location. (text = 'abc'  location = line: 1 column: 25 28)"})

);
