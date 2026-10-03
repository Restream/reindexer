#include <gtest/gtest-param-test.h>
#include <initializer_list>
#include <tuple>
#include "core/ft/ftdsl.h"
#include "ft_api.h"

namespace reindexer_tests {

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
	EXPECT_TRUE(ftdsl.NumEntries() == 2);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().suff);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().pref);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Pattern() == u"search");
	EXPECT_TRUE(!ftdsl.GetEntry(1).Term().Opts().suff);
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Opts().pref);
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Pattern() == u"this");
}

TEST_P(FTDSLParserApi, MisspellingTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("black~ -white");
	EXPECT_TRUE(ftdsl.NumEntries() == 2);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().typos);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Pattern() == u"black");
	EXPECT_TRUE(!ftdsl.GetEntry(1).Term().Opts().typos);
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Opts().op == OpNot);
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Pattern() == u"white");
}

TEST_P(FTDSLParserApi, FieldsPartOfRequest) {
	FTDSLQueryParams params;
	params.fields = {{"name", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 0}},
					 {"title", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 1}}};
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("@name^1.5,+title^0.5 rush");
	EXPECT_EQ(ftdsl.NumEntries(), 1);
	EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"rush");
	EXPECT_EQ(ftdsl.GetEntry(0).Term().Opts().fieldsOpts.size(), 2);
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetEntry(0).Term().Opts().fieldsOpts[0].boost, 1.5f));
	EXPECT_FALSE(ftdsl.GetEntry(0).Term().Opts().fieldsOpts[0].needSumRank);
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetEntry(0).Term().Opts().fieldsOpts[1].boost, 0.5f));
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().fieldsOpts[1].needSumRank);
}

TEST_P(FTDSLParserApi, PhraseFieldSelection) {
	FTDSLQueryParams params;
	params.fields = {{"name", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 0}},
					 {"title", reindexer::FtIndexFieldPros{.isIndexed = true, .fieldNumber = 1}}};
	reindexer::SplitOptions opts;

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("@name \"hello world\"");
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsPhrase());
		const auto& phrase = ftdsl.GetEntry(0).Phrase();
		ASSERT_EQ(phrase.FieldsOpts().size(), 2);
		EXPECT_TRUE(AreFloatingValuesEqual(phrase.FieldsOpts()[0].boost, 1.0f));
		EXPECT_TRUE(AreFloatingValuesEqual(phrase.FieldsOpts()[1].boost, 0.0f));
	}

	for (std::string_view query : {"\"@name hello world\""sv, "\"hello @name world\""sv}) {
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		try {
			ftdsl.Parse(query);
			FAIL() << "Expected field selector inside phrase to be rejected";
		} catch (const reindexer::Error& err) {
			EXPECT_EQ(err.code(), errParseDSL);
			EXPECT_STREQ(err.what(), "Field selector is not allowed inside of search phrase; specify it before the opening quote");
		}
	}
}

TEST_P(FTDSLParserApi, TermRelevancyBoostTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("+mongodb^0.5 +arangodb^0.25 +reindexer^2.5");
	EXPECT_TRUE(ftdsl.NumEntries() == 3);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Pattern() == u"mongodb");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetEntry(0).Term().Opts().boost, 0.5f));
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Pattern() == u"arangodb");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetEntry(1).Term().Opts().boost, 0.25f));
	EXPECT_TRUE(ftdsl.GetEntry(2).Term().Pattern() == u"reindexer");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl.GetEntry(2).Term().Opts().boost, 2.5f));
}

TEST_P(FTDSLParserApi, WrongRelevancyTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	EXPECT_THROW(ftdsl.Parse("+wrong +boost^X"), reindexer::Error);
	EXPECT_THROW(ftdsl.Parse("+term^" + std::string(64, '1')), reindexer::Error);
}

TEST_P(FTDSLParserApi, DistanceTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("'long nose'~3");
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsPhrase());
		const auto& phrase = ftdsl.GetEntry(0).Phrase();
		ASSERT_EQ(phrase.NumTerms(), 2);
		EXPECT_EQ(phrase.GetTerm(0).Pattern(), u"long");
		EXPECT_EQ(phrase.GetTerm(1).Pattern(), u"nose");
		EXPECT_EQ(phrase.Distance(), 3);
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("'+long +nose'~3");
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsPhrase());
		const auto& phrase = ftdsl.GetEntry(0).Phrase();
		ASSERT_EQ(phrase.NumTerms(), 2);
		EXPECT_EQ(phrase.GetTerm(0).Pattern(), u"long");
		EXPECT_EQ(phrase.GetTerm(1).Pattern(), u"nose");
		EXPECT_EQ(phrase.Op(), OpOr);
		EXPECT_EQ(phrase.Distance(), 3);
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
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		EXPECT_THROW(ftdsl.Parse("'long nose'~" + std::string(32, '1')), reindexer::Error);
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
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsPhrase());
		ASSERT_EQ(ftdsl.GetEntry(0).Phrase().NumTerms(), 1);
		EXPECT_EQ(ftdsl.GetEntry(0).Phrase().GetTerm(0).Pattern(), u"\"phrase");
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("'\\\'phrase'");
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsPhrase());
		ASSERT_EQ(ftdsl.GetEntry(0).Phrase().NumTerms(), 1);
		EXPECT_EQ(ftdsl.GetEntry(0).Phrase().GetTerm(0).Pattern(), u"'phrase");
	}
	{
		opts.SetSymbols("-/+", "'-`+");
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("Д'Артаньян +'required phrase' -'excluded phrase'");
		ASSERT_EQ(ftdsl.NumEntries(), 3);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"д'артаньян");
		ASSERT_TRUE(ftdsl.GetEntry(1).IsPhrase());
		EXPECT_EQ(ftdsl.GetEntry(1).Phrase().Op(), OpAnd);
		ASSERT_TRUE(ftdsl.GetEntry(2).IsPhrase());
		EXPECT_EQ(ftdsl.GetEntry(2).Phrase().Op(), OpNot);
	}
}

TEST_P(FTDSLParserApi, WrongKbLayoutPattern) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	const auto checkTerms = [&](std::string_view query, std::initializer_list<std::tuple<const char16_t*, const char16_t*>> expectedTerms) {
		SCOPED_TRACE(query);
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse(query);
		ASSERT_EQ(ftdsl.NumEntries(), expectedTerms.size());
		size_t i = 0;
		for (const auto& [pattern, wrongKbLayoutPattern] : expectedTerms) {
			ASSERT_TRUE(ftdsl.GetEntry(i).IsTerm());
			EXPECT_EQ(ftdsl.GetEntry(i).Term().Pattern(), pattern);
			EXPECT_EQ(ftdsl.GetEntry(i).Term().WrongKbLayoutPattern(), wrongKbLayoutPattern);
			++i;
		}
	};

	{
// GCC false positive in std::variant::index() inlined from IsTerm (GCC 13): constant indexes beyond h_vector's inline capacity
// are checked against the inline buffer.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Warray-bounds"
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("plain fggff;gggg:hhhh next,last");
		ASSERT_EQ(ftdsl.NumEntries(), 6);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"plain");
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().WrongKbLayoutPattern().empty());
		ASSERT_TRUE(ftdsl.GetEntry(1).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(1).Term().Pattern(), u"fggff");
		EXPECT_EQ(ftdsl.GetEntry(1).Term().WrongKbLayoutPattern(), u"fggff;gggg:hhhh");
		ASSERT_TRUE(ftdsl.GetEntry(2).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(2).Term().Pattern(), u"gggg");
		EXPECT_TRUE(ftdsl.GetEntry(2).Term().WrongKbLayoutPattern().empty());
		ASSERT_TRUE(ftdsl.GetEntry(3).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(3).Term().Pattern(), u"hhhh");
		EXPECT_TRUE(ftdsl.GetEntry(3).Term().WrongKbLayoutPattern().empty());
		ASSERT_TRUE(ftdsl.GetEntry(4).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(4).Term().Pattern(), u"next");
		EXPECT_EQ(ftdsl.GetEntry(4).Term().WrongKbLayoutPattern(), u"next,last");
		ASSERT_TRUE(ftdsl.GetEntry(5).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(5).Term().Pattern(), u"last");
		EXPECT_TRUE(ftdsl.GetEntry(5).Term().WrongKbLayoutPattern().empty());
#pragma GCC diagnostic pop
	}

	checkTerms("hello, foo.bar", {{u"hello", u"hello,"}, {u"foo", u"foo.bar"}, {u"bar", u""}});
	checkTerms(".term", {{u"term", u".term"}});
	checkTerms("term.", {{u"term", u"term."}});
	checkTerms(".term.", {{u"term", u".term."}});
	checkTerms(".te.rm.", {{u"te", u".te.rm."}, {u"rm", u""}});
	checkTerms("...term", {{u"term", u"...term"}});
	checkTerms("term...term", {{u"term", u"term...term"}, {u"term", u""}});
	checkTerms("term. .term", {{u"term", u"term."}, {u"term", u".term"}});
	checkTerms("field[0].data", {{u"field", u"field[0].data"}, {u"0", u""}, {u"data", u""}});
	checkTerms(";.,", {{u"", u";.,"}});
	checkTerms(";., next", {{u"", u";.,"}, {u"next", u""}});
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse(";.,");
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		const auto& term = ftdsl.GetEntry(0).Term();
		EXPECT_TRUE(AreFloatingValuesEqual(term.Opts().termLenBoost, 0.0f));
		EXPECT_TRUE(AreFloatingValuesEqual(term.WrongKbLayoutTermLenBoost(), 1.0f));
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("*vjyb,njh");
		ASSERT_EQ(ftdsl.NumEntries(), 2);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsTerm());
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().suff);
		EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"vjyb");
		EXPECT_EQ(ftdsl.GetEntry(0).Term().WrongKbLayoutPattern(), u"vjyb,njh");
		EXPECT_FALSE(ftdsl.GetEntry(0).Term().WrongKbLayoutPref());
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().WrongKbLayoutSuff());
	}

	for (const auto& [query, pref, suff] : {std::tuple{"vjyb,njh", false, false}, std::tuple{"*vjyb,njh", false, true},
											std::tuple{"vjyb,njh*", true, false}, std::tuple{"*vjyb,njh*", true, true}}) {
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse(query);
		ASSERT_EQ(ftdsl.NumEntries(), 2);
		const auto& term = ftdsl.GetEntry(0).Term();
		EXPECT_EQ(term.WrongKbLayoutPattern(), u"vjyb,njh");
		EXPECT_EQ(term.WrongKbLayoutPref(), pref);
		EXPECT_EQ(term.WrongKbLayoutSuff(), suff);
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("a.b~");
		ASSERT_EQ(ftdsl.NumEntries(), 2);
		const auto& firstTerm = ftdsl.GetEntry(0).Term();
		EXPECT_EQ(firstTerm.Pattern(), u"a");
		EXPECT_EQ(firstTerm.WrongKbLayoutPattern(), u"a.b");
		EXPECT_TRUE(firstTerm.WrongKbLayoutTypos());
		EXPECT_FALSE(firstTerm.Opts().typos);
		const auto& secondTerm = ftdsl.GetEntry(1).Term();
		EXPECT_EQ(secondTerm.Pattern(), u"b");
		EXPECT_TRUE(secondTerm.Opts().typos);
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("[kt,jgtxrf");
		ASSERT_EQ(ftdsl.NumEntries(), 2);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsTerm());
		const auto& term = ftdsl.GetEntry(0).Term();
		EXPECT_EQ(term.Pattern(), u"kt");
		EXPECT_EQ(term.WrongKbLayoutPattern(), u"[kt,jgtxrf");
		EXPECT_TRUE(AreFloatingValuesEqual(term.Opts().termLenBoost, 2.0f / 10.0f));
		EXPECT_TRUE(AreFloatingValuesEqual(term.WrongKbLayoutTermLenBoost(), 1.0f));
		const auto& secondTerm = ftdsl.GetEntry(1).Term();
		EXPECT_EQ(secondTerm.Pattern(), u"jgtxrf");
		EXPECT_TRUE(AreFloatingValuesEqual(secondTerm.Opts().termLenBoost, 6.0f / 10.0f));
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("*[kt,jgtxrf");
		ASSERT_EQ(ftdsl.NumEntries(), 2);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsTerm());
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().suff);
		EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"kt");
		EXPECT_EQ(ftdsl.GetEntry(0).Term().WrongKbLayoutPattern(), u"[kt,jgtxrf");
		EXPECT_FALSE(ftdsl.GetEntry(0).Term().WrongKbLayoutPref());
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().WrongKbLayoutSuff());
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("word<next>part{last}");
		ASSERT_EQ(ftdsl.NumEntries(), 4);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsTerm());
		EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"word");
		EXPECT_EQ(ftdsl.GetEntry(0).Term().WrongKbLayoutPattern(), u"word<next>part{last}");
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse(":lfnm ;lfnm");
		ASSERT_EQ(ftdsl.NumEntries(), 2);
		EXPECT_EQ(ftdsl.GetEntry(0).Term().WrongKbLayoutPattern(), u":lfnm");
		EXPECT_EQ(ftdsl.GetEntry(1).Term().WrongKbLayoutPattern(), u";lfnm");
	}

	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse("=abc.def");
		ASSERT_EQ(ftdsl.NumEntries(), 2);
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().exact);
		EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"abc");
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().WrongKbLayoutPattern().empty());
		EXPECT_FALSE(ftdsl.GetEntry(1).Term().Opts().exact);
		EXPECT_EQ(ftdsl.GetEntry(1).Term().Pattern(), u"def");
	}

	{
		reindexer::SplitOptions optsWithDot;
		optsWithDot.SetSymbols(".", "");
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, optsWithDot);
		ftdsl.Parse("=abc.def");
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().exact);
		EXPECT_EQ(ftdsl.GetEntry(0).Term().Pattern(), u"abc.def");
		EXPECT_TRUE(ftdsl.GetEntry(0).Term().WrongKbLayoutPattern().empty());
	}

	for (const auto& [query, distance] : {std::tuple{"\"fggff;gggg:hhhh\"", 1}, std::tuple{"\"fggff;gggg:hhhh\"~3", 3}}) {
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
		ftdsl.Parse(query);
		ASSERT_EQ(ftdsl.NumEntries(), 1);
		ASSERT_TRUE(ftdsl.GetEntry(0).IsPhrase());
		const auto& phrase = ftdsl.GetEntry(0).Phrase();
		ASSERT_EQ(phrase.NumTerms(), 3);
		EXPECT_EQ(phrase.Distance(), distance);
		EXPECT_EQ(phrase.GetTerm(0).Pattern(), u"fggff");
		EXPECT_EQ(phrase.GetTerm(0).WrongKbLayoutPattern(), u"fggff;");
		EXPECT_EQ(phrase.GetTerm(1).Pattern(), u"gggg");
		EXPECT_EQ(phrase.GetTerm(1).WrongKbLayoutPattern(), u"gggg:");
		EXPECT_EQ(phrase.GetTerm(2).Pattern(), u"hhhh");
		EXPECT_TRUE(phrase.GetTerm(2).WrongKbLayoutPattern().empty());
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
	EXPECT_TRUE(ftdsl.NumEntries() == 3);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().op == OpAnd);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Pattern() == u"jack");
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Opts().op == OpNot);
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Pattern() == u"john");
	EXPECT_TRUE(ftdsl.GetEntry(2).Term().Opts().op == OpAnd);
	EXPECT_TRUE(ftdsl.GetEntry(2).Term().Pattern() == u"joe");
}

TEST_P(FTDSLParserApi, EscapingCharacterTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("\\-hell \\+well \\+belu");
	EXPECT_TRUE(ftdsl.NumEntries() == 3) << ftdsl.NumEntries();
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().op == OpOr);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Pattern() == u"-hell");
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Opts().op == OpOr);
	EXPECT_TRUE(ftdsl.GetEntry(1).Term().Pattern() == u"+well");
	EXPECT_TRUE(ftdsl.GetEntry(2).Term().Opts().op == OpOr);
	EXPECT_TRUE(ftdsl.GetEntry(2).Term().Pattern() == u"+belu");
}

TEST_P(FTDSLParserApi, ExactMatchTest) {
	FTDSLQueryParams params;
	reindexer::SplitOptions opts;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, opts);
	ftdsl.Parse("=moskva77");
	EXPECT_TRUE(ftdsl.NumEntries() == 1);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Opts().exact);
	EXPECT_TRUE(ftdsl.GetEntry(0).Term().Pattern() == u"moskva77");
}

INSTANTIATE_TEST_SUITE_P(, FTDSLParserApi, ::testing::Values(kRxFtTestTypes), [](const auto& info) {
	switch (info.param) {
		case reindexer::FTConfig::Optimization::Memory:
			return "OptimizationByMemory";
		case reindexer::FTConfig::Optimization::CPU:
			return "OptimizationByCPU";
		default:
			assert(false);
			std::abort();
	}
});

}  // namespace reindexer_tests
