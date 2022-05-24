#include <gtest/gtest.h>

#include "core/query/query.h"
#include "core/query/sql/sqlparser.h"

struct SQLParserTests : public ::testing::TestWithParam<std::string> {
	reindexer::Query query;
};

// --gtest_filter=*/SQLParserTests.NumberInDoubleQuotesParsedCorrectly/*
TEST_P(SQLParserTests, NumberInDoubleQuotesParsedCorrectly) { EXPECT_NO_THROW(reindexer::SQLParser(query).Parse(GetParam())); }

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

// --gtest_filter=Delete/SQLParserTests.NumberInDoubleQuotesParsedCorrectly/*
INSTANTIATE_TEST_SUITE_P(Delete, SQLParserTests,
						 ::testing::Values("DELETE FROM ns WHERE \"123\" = 'some_value'", "DELETE FROM ns WHERE \"123\" = 123",
										   "DELETE FROM ns WHERE \"123abc\" = 123",
										   "DELETE FROM ns WHERE \"123abc123\" = 123 AND \"123\" = 'some_value'",
										   "DELETE FROM ns WHERE \"123\" = 123 AND \"abc123\" = 'some_value' AND \"123abc123\" = 123",
										   "DELETE FROM ns WHERE \"123\" = 123 AND \"abc123\" = \"123abc123\" AND \"123abc123\" = 123"));
