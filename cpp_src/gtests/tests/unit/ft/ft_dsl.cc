#include <gtest/gtest-param-test.h>
#include "core/ft/ftdsl.h"
#include "ft_api.h"

using namespace std::string_view_literals;

class [[nodiscard]] FTDSLParserApi : public FTApi {
protected:
	std::string_view GetDefaultNamespace() noexcept override { return "ft_dsl_default_namespace"; }

	template <typename T>
	bool AreFloatingValuesEqual(T a, T b) {
		return std::abs(a - b) < std::numeric_limits<T>::epsilon();
	}
};

TEST_P(FTDSLParserApi, MatchSymbolTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("*search*this*");
	EXPECT_TRUE(ftdsl.NumTerms() == 2);
	EXPECT_TRUE(ftdsl.GetTerm(0).Opts().suff);
	EXPECT_TRUE(ftdsl.GetTerm(0).Opts().pref);
	EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"search");
	EXPECT_TRUE(!ftdsl.GetTerm(1).Opts().suff);
	EXPECT_TRUE(ftdsl.GetTerm(1).Opts().pref);
	EXPECT_TRUE(ftdsl.GetTerm(1).Pattern() == L"this");
}

TEST_P(FTDSLParserApi, MisspellingTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("black~ -white");
	EXPECT_TRUE(ftdsl.NumTerms() == 2);
	EXPECT_TRUE(ftdsl.GetTerm(0).Opts().typos);
	EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"black");
	EXPECT_TRUE(!ftdsl.GetTerm(1).Opts().typos);
	EXPECT_TRUE(ftdsl.GetTerm(1).Opts().op == OpNot);
	EXPECT_TRUE(ftdsl.GetTerm(1).Pattern() == L"white");
}

TEST_P(FTDSLParserApi, FieldsPartOfRequest) {
	FTDSLQueryParams params;
	params.fields = {{"name", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 0}},
					 {"title", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 1}}};
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("@name^1.5,+title^0.5 rush");
	EXPECT_EQ(ftdsl.NumTerms(), 1);
	EXPECT_EQ(ftdsl.GetTerm(0).Pattern(), L"rush");
	EXPECT_EQ(ftdsl.GetTerm(0).Opts().fieldsOpts.size(), 2);
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetTerm(0).Opts().fieldsOpts[0].boost, 1.5f));
	EXPECT_FALSE(ftdsl.GetTerm(0).Opts().fieldsOpts[0].needSumRank);
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetTerm(0).Opts().fieldsOpts[1].boost, 0.5f));
	EXPECT_TRUE(ftdsl.GetTerm(0).Opts().fieldsOpts[1].needSumRank);
}

TEST_P(FTDSLParserApi, TermRelevancyBoostTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("+mongodb^0.5 +arangodb^0.25 +reindexer^2.5");
	EXPECT_TRUE(ftdsl.NumTerms() == 3);
	EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"mongodb");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetTerm(0).Opts().boost, 0.5f));
	EXPECT_TRUE(ftdsl.GetTerm(1).Pattern() == L"arangodb");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetTerm(1).Opts().boost, 0.25f));
	EXPECT_TRUE(ftdsl.GetTerm(2).Pattern() == L"reindexer");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetTerm(2).Opts().boost, 2.5f));
}

TEST_P(FTDSLParserApi, WrongRelevancyTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	EXPECT_THROW(ftdsl.Parse("+wrong +boost^X"), reindexer::Error);
}

TEST_P(FTDSLParserApi, DistanceTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("'long nose'~3");
		EXPECT_TRUE(ftdsl.NumTerms() == 2);
		EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"long");
		EXPECT_TRUE(ftdsl.GetTerm(1).Pattern() == L"nose");
		EXPECT_TRUE(ftdsl.GetTerm(0).Opts().distance == INT_MAX);
		EXPECT_TRUE(ftdsl.GetTerm(1).Opts().distance == 3);
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("'+long +nose'~3");
		EXPECT_TRUE(ftdsl.NumTerms() == 2);
		EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"long");
		EXPECT_TRUE(ftdsl.GetTerm(1).Pattern() == L"nose");
		EXPECT_TRUE(ftdsl.GetTerm(0).Opts().distance == INT_MAX);
		EXPECT_TRUE(ftdsl.GetTerm(1).Opts().distance == 3);
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("'-long nose'~3"), reindexer::Error);
	}
}

TEST_P(FTDSLParserApi, WrongDistanceTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("'this is a wrong distance'~X"), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("'long nose'~-1"), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("'long nose'~0"), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("'long nose'~2.89"), reindexer::Error);
	}
}

TEST_P(FTDSLParserApi, QuotesTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("\"forgot to close this quote"), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("'different quotes\""), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("'\\\"phrase'");
		EXPECT_TRUE(ftdsl.NumTerms() == 1);
		EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"\"phrase");
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("'\\\'phrase'");
		EXPECT_TRUE(ftdsl.NumTerms() == 1);
		EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"'phrase");
	}
}

TEST_P(FTDSLParserApi, WrongFieldNameTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	params.fields = {{"id", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 0}},
					 {"fk_id", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 1}},
					 {"location", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 2}}};
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	EXPECT_THROW(ftdsl.Parse("@name,text,desc Thrones"), reindexer::Error);
}

TEST_P(FTDSLParserApi, BinaryOperatorsTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("+Jack -John +Joe");
	EXPECT_TRUE(ftdsl.NumTerms() == 3);
	EXPECT_TRUE(ftdsl.GetTerm(0).Opts().op == OpAnd);
	EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"jack");
	EXPECT_TRUE(ftdsl.GetTerm(1).Opts().op == OpNot);
	EXPECT_TRUE(ftdsl.GetTerm(1).Pattern() == L"john");
	EXPECT_TRUE(ftdsl.GetTerm(2).Opts().op == OpAnd);
	EXPECT_TRUE(ftdsl.GetTerm(2).Pattern() == L"joe");
}

TEST_P(FTDSLParserApi, EscapingCharacterTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("\\-hell \\+well \\+bell");
	EXPECT_TRUE(ftdsl.NumTerms() == 3) << ftdsl.NumTerms();
	EXPECT_TRUE(ftdsl.GetTerm(0).Opts().op == OpOr);
	EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"-hell");
	EXPECT_TRUE(ftdsl.GetTerm(1).Opts().op == OpOr);
	EXPECT_TRUE(ftdsl.GetTerm(1).Pattern() == L"+well");
	EXPECT_TRUE(ftdsl.GetTerm(2).Opts().op == OpOr);
	EXPECT_TRUE(ftdsl.GetTerm(2).Pattern() == L"+bell");
}

TEST_P(FTDSLParserApi, ExactMatchTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("=moskva77");
	EXPECT_TRUE(ftdsl.NumTerms() == 1);
	EXPECT_TRUE(ftdsl.GetTerm(0).Opts().exact);
	EXPECT_TRUE(ftdsl.GetTerm(0).Pattern() == L"moskva77");
}

INSTANTIATE_TEST_SUITE_P(, FTDSLParserApi,
						 ::testing::Values(reindexer::FtFastConfig::Optimization::Memory, reindexer::FtFastConfig::Optimization::CPU),
						 [](const auto& info) {
							 switch (info.param) {
								 case reindexer::FtFastConfig::Optimization::Memory:
									 return "OptimizationByMemory";
								 case reindexer::FtFastConfig::Optimization::CPU:
									 return "OptimizationByCPU";
								 default:
									 assert(false);
									 std::abort();
							 }
						 });
