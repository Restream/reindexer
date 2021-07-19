#include <iostream>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include "core/ft/ftdsl.h"
#include "debug/allocdebug.h"
#include "ft_api.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

using namespace std::string_view_literals;

TEST_F(FTApi, CompositeSelect) {
	Init(GetDefaultConfig(), NS1 | NS2);
	Add("An entity is something|"sv, "| that in exists entity as itself"sv, NS1 | NS2);
	Add("In law, a legal entity is|"sv, "|an entity that is capable of something bearing legal rights"sv, NS1 | NS2);
	Add("In politics, entity is used as|"sv, "| term for entity territorial divisions of some countries"sv, NS1 | NS2);

	for (const auto& query : CreateAllPermutatedQueries("", {"*entity", "somethin*"}, "")) {
		auto res = SimpleCompositeSelect(query);
		std::unordered_set<std::string_view> data{"An <b>entity</b> is <b>something</b>|"sv,
												  "| that in exists <b>entity</b> as itself"sv,
												  "An <b>entity</b> is <b>something</b>|d"sv,
												  "| that in exists entity as itself"sv,
												  "In law, a legal <b>entity</b> is|"sv,
												  "|an <b>entity</b> that is capable of <b>something</b> bearing legal rights"sv,
												  "al <b>entity</b> id"sv,
												  "|an entity that is capable of something bearing legal rights"sv,
												  "In politics, <b>entity</b> is used as|"sv,
												  "| term for <b>entity</b> territorial divisions of some countries"sv,
												  "s, <b>entity</b> id"sv,
												  "| term for entity territorial divisions of some countries"sv};

		PrintQueryResults("nm1", res);
		for (auto it : res) {
			Item ritem(it.GetItem(false));
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				auto field = ritem[idx].Name();
				if (field == "id") continue;
				auto it = data.find(ritem[field].As<string>());
				ASSERT_TRUE(it != data.end());
				data.erase(it);
			}
		}
		EXPECT_TRUE(data.empty());
	}
}

TEST_F(FTApi, CompositeSelectWithFields) {
	Init(GetDefaultConfig(), NS1 | NS2);
	AddInBothFields("An entity is something|"sv, "| that in exists entity as itself"sv, NS1 | NS2);
	AddInBothFields("In law, a legal entity is|"sv, "|an entity that is capable of something bearing legal rights"sv, NS1 | NS2);
	AddInBothFields("In politics, entity is used as|"sv, "| term for entity territorial divisions of some countries"sv, NS1 | NS2);

	for (const auto& query : CreateAllPermutatedQueries("", {"*entity", "somethin*"}, "")) {
		for (const char* field : {"ft1", "ft2"}) {
			auto res = CompositeSelectField(field, query);
			std::unordered_set<std::string_view> data{"An <b>entity</b> is <b>something</b>|"sv,
													  "An <b>entity</b> is <b>something</b>|d"sv,
													  "| that in exists <b>entity</b> as itself"sv,
													  "In law, a legal <b>entity</b> is|"sv,
													  "|an <b>entity</b> that is capable of <b>something</b> bearing legal rights"sv,
													  "an <b>entity</b> tdof <b>something</b> bd"sv,
													  "al <b>entity</b> id"sv,
													  "In politics, <b>entity</b> is used as|"sv,
													  "| term for <b>entity</b> territorial divisions of some countries"sv,
													  "ts <b>entity</b> ad"sv,
													  "s, <b>entity</b> id"sv,
													  "or <b>entity</b> td"sv};

			PrintQueryResults("nm1", res);
			for (auto it : res) {
				Item ritem(it.GetItem(false));
				for (auto idx = 1; idx < ritem.NumFields(); idx++) {
					auto curField = ritem[idx].Name();
					if (curField != field) continue;
					auto it = data.find(ritem[curField].As<string>());
					ASSERT_TRUE(it != data.end());
					data.erase(it);
				}
			}
			EXPECT_TRUE(data.empty());
		}
	}
}

TEST_F(FTApi, CompositeRankWithSynonyms) {
	auto cfg = GetDefaultConfig();
	cfg.synonyms = {{{"word"}, {"слово"}}};
	Init(cfg);
	Add("word"sv, "слово"sv);
	Add("world"sv, "world"sv);

	// rank of synonym is higher
	CheckAllPermutations("@", {"ft1^0.5", "ft2^2"}, " word~", {{"word", "!слово!"}, {"!world!", "!world!"}}, true, ", ");
}

TEST_F(FTApi, SelectWithEscaping) {
	reindexer::FtFastConfig ftCfg = GetDefaultConfig();
	ftCfg.extraWordSymbols = "+-\\";
	Init(ftCfg);
	Add("Go to -hell+hell+hell!!"sv);

	auto res = SimpleSelect("\\-hell\\+hell\\+hell");
	EXPECT_TRUE(res.Count() == 1);

	for (auto it : res) {
		Item ritem(it.GetItem(false));
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == "Go to !-hell+hell+hell!!!");
	}
}

TEST_F(FTApi, SelectWithPlus) {
	Init(GetDefaultConfig());

	Add("added three words"sv);
	Add("added something else"sv);

	CheckAllPermutations("", {"+added"}, "", {{"!added! something else", ""}, {"!added! three words", ""}});
}

TEST_F(FTApi, SelectWithPlusWithSingleAlternative) {
	auto cfg = GetDefaultConfig();
	cfg.enableKbLayout = false;
	cfg.enableTranslit = false;
	Init(cfg);

	Add("мониторы"sv);

	// FT search by single mandatory word with single alternative
	CheckAllPermutations("", {"+монитор*"}, "", {{"!мониторы!", ""}});
}

TEST_F(FTApi, SelectWithMinus) {
	Init(GetDefaultConfig());

	Add("including me, excluding you"sv);
	Add("including all of them"sv);

	CheckAllPermutations("", {"+including", "-excluding"}, "", {{"!including! all of them", ""}});
	CheckAllPermutations("", {"including", "-excluding"}, "", {{"!including! all of them", ""}});
}

TEST_F(FTApi, SelectWithFieldsList) {
	Init(GetDefaultConfig());

	Add("nm1"sv, "Never watch their games"sv, "Because nothing can be worse than Spartak Moscow"sv);
	Add("nm1"sv, "Spartak Moscow is the worst team right now"sv, "Yes, for sure"sv);

	CheckAllPermutations("@ft1 ", {"Spartak", "Moscow"}, "", {{"!Spartak Moscow! is the worst team right now", "Yes, for sure"}});
}

TEST_F(FTApi, SelectWithRelevanceBoost) {
	Init(GetDefaultConfig());

	Add("She was a very bad girl"sv);
	Add("All the naughty kids go to hell, not to heaven"sv);
	Add("I've never seen a man as cruel as him"sv);

	CheckAllPermutations("@ft1 ", {"girl^2", "kids", "cruel^3"}, "",
						 {{"I've never seen a man as !cruel! as him", ""},
						  {"She was a very bad !girl!", ""},
						  {"All the naughty !kids! go to hell, not to heaven", ""}},
						 true);
}

TEST_F(FTApi, SelectWithDistance) {
	Init(GetDefaultConfig());

	Add("Her nose was very very long"sv);
	Add("Her nose was exceptionally long"sv);
	Add("Her nose was long"sv);

	CheckAllPermutations("", {"'nose long'~3"}, "", {{"Her !nose! was !long!", ""}, {"Her !nose! was exceptionally !long!", ""}}, true);
}

TEST_F(FTApi, SelectWithTypos) {
	auto cfg = GetDefaultConfig();
	cfg.stopWords.clear();
	cfg.stemmers.clear();
	cfg.enableKbLayout = false;
	cfg.enableTranslit = false;

	cfg.maxTypos = 0;
	Init(cfg);
	Add("A");
	Add("AB");
	Add("ABC");
	Add("ABCD");
	Add("ABCDE");
	Add("ABCDEF");
	Add("ABCDEFG");
	Add("ABCDEFGH");
	// Only full match
	CheckAllPermutations("", {"A~"}, "", {{"!A!", ""}});
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}});
	CheckAllPermutations("", {"ABC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"ABCDEFGHI~"}, "", {});
	CheckAllPermutations("", {"XBCD~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {});

	cfg.maxTypos = 1;
	SetFTConfig(cfg);
	// Full match
	// Or one missed char in any word
	CheckAllPermutations("", {"ABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDEFGHI~"}, "", {{"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XBCD~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});

	cfg.maxTypos = 2;
	SetFTConfig(cfg);
	// Full match
	// Or up to by one missed char in any or both words
	// Or one typo
	CheckAllPermutations("", {"ABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"BXCX~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!ABCD!", ""}});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});

	cfg.maxTypos = 3;
	SetFTConfig(cfg);
	// Full match
	// Or up to by one missed char in any or both words
	// Or one missed char in one word and two missed chars in another one
	// Or up to two typos
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {{"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BADC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"BACDFE~"}, "", {{"!ABCDE!", ""}});
	CheckAllPermutations("", {"XBCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"XBXD~"}, "", {});
	CheckAllPermutations("", {"AXXD~"}, "", {});
	CheckAllPermutations("", {"XBCX~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"XXCD~"}, "", {});
	CheckAllPermutations("", {"XXABX~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});
	CheckAllPermutations("", {"AXX~"}, "", {});

	cfg.maxTypos = 4;
	SetFTConfig(cfg);
	// Full match
	// Or up to by two missed chars in any or both words
	// Or up to two typos
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BADC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"BACDFE~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BADCFE~"}, "", {});
	CheckAllPermutations("", {"XBCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXX~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XBXD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"AXXD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XBCX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"XXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XXABX~"}, "", {});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});
	CheckAllPermutations("", {"AXX~"}, "", {});
}

template <typename T>
bool AreFloatingValuesEqual(T a, T b) {
	return std::abs(a - b) < std::numeric_limits<T>::epsilon();
}

TEST_F(FTApi, FTDslParserMatchSymbolTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("*search*this*");
	EXPECT_TRUE(ftdsl.size() == 2);
	EXPECT_TRUE(ftdsl[0].opts.suff);
	EXPECT_TRUE(ftdsl[0].opts.pref);
	EXPECT_TRUE(ftdsl[0].pattern == L"search");
	EXPECT_TRUE(!ftdsl[1].opts.suff);
	EXPECT_TRUE(ftdsl[1].opts.pref);
	EXPECT_TRUE(ftdsl[1].pattern == L"this");
}

TEST_F(FTApi, FTDslParserMisspellingTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("black~ -white");
	EXPECT_TRUE(ftdsl.size() == 2);
	EXPECT_TRUE(ftdsl[0].opts.typos);
	EXPECT_TRUE(ftdsl[0].pattern == L"black");
	EXPECT_TRUE(!ftdsl[1].opts.typos);
	EXPECT_TRUE(ftdsl[1].opts.op == OpNot);
	EXPECT_TRUE(ftdsl[1].pattern == L"white");
}

TEST_F(FTApi, FTDslParserFieldsPartOfRequest) {
	FTDSLQueryParams params;
	params.fields = {{"name", 0}, {"title", 1}};
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("@name^1.5,+title^0.5 rush");
	EXPECT_EQ(ftdsl.size(), 1);
	EXPECT_EQ(ftdsl[0].pattern, L"rush");
	EXPECT_EQ(ftdsl[0].opts.fieldsOpts.size(), 2);
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl[0].opts.fieldsOpts[0].boost, 1.5f));
	EXPECT_FALSE(ftdsl[0].opts.fieldsOpts[0].needSumRank);
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl[0].opts.fieldsOpts[1].boost, 0.5f));
	EXPECT_TRUE(ftdsl[0].opts.fieldsOpts[1].needSumRank);
}

TEST_F(FTApi, FTDslParserTermRelevancyBoostTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("+mongodb^0.5 +arangodb^0.25 +reindexer^2.5");
	EXPECT_TRUE(ftdsl.size() == 3);
	EXPECT_TRUE(ftdsl[0].pattern == L"mongodb");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl[0].opts.boost, 0.5f));
	EXPECT_TRUE(ftdsl[1].pattern == L"arangodb");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl[1].opts.boost, 0.25f));
	EXPECT_TRUE(ftdsl[2].pattern == L"reindexer");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl[2].opts.boost, 2.5f));
}

TEST_F(FTApi, FTDslParserWrongRelevancyTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	EXPECT_THROW(ftdsl.parse("+wrong +boost^X"), Error);
}

TEST_F(FTApi, FTDslParserDistanceTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("'long nose'~3");
	EXPECT_TRUE(ftdsl.size() == 2);
	EXPECT_TRUE(ftdsl[0].pattern == L"long");
	EXPECT_TRUE(ftdsl[1].pattern == L"nose");
	EXPECT_TRUE(ftdsl[0].opts.distance == INT_MAX);
	EXPECT_TRUE(ftdsl[1].opts.distance == 3);
}

TEST_F(FTApi, FTDslParserWrongDistanceTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	EXPECT_THROW(ftdsl.parse("'this is a wrong distance'~X"), Error);
}

TEST_F(FTApi, FTDslParserNoClosingQuoteTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	EXPECT_THROW(ftdsl.parse("\"forgot to close this quote"), Error);
}

TEST_F(FTApi, FTDslParserWrongFieldNameTest) {
	FTDSLQueryParams params;
	params.fields = {{"id", 0}, {"fk_id", 1}, {"location", 2}};
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	EXPECT_THROW(ftdsl.parse("@name,text,desc Thrones"), Error);
}

TEST_F(FTApi, FTDslParserBinaryOperatorsTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("+Jack -John +Joe");
	EXPECT_TRUE(ftdsl.size() == 3);
	EXPECT_TRUE(ftdsl[0].opts.op == OpAnd);
	EXPECT_TRUE(ftdsl[0].pattern == L"jack");
	EXPECT_TRUE(ftdsl[1].opts.op == OpNot);
	EXPECT_TRUE(ftdsl[1].pattern == L"john");
	EXPECT_TRUE(ftdsl[2].opts.op == OpAnd);
	EXPECT_TRUE(ftdsl[2].pattern == L"joe");
}

TEST_F(FTApi, FTDslParserEscapingCharacterTest) {
	FTDSLQueryParams params;
	params.extraWordSymbols = "+-\\";
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("\\-hell \\+well \\+bell");
	EXPECT_TRUE(ftdsl.size() == 3) << ftdsl.size();
	EXPECT_TRUE(ftdsl[0].opts.op == OpOr);
	EXPECT_TRUE(ftdsl[0].pattern == L"-hell");
	EXPECT_TRUE(ftdsl[1].opts.op == OpOr);
	EXPECT_TRUE(ftdsl[1].pattern == L"+well");
	EXPECT_TRUE(ftdsl[2].opts.op == OpOr);
	EXPECT_TRUE(ftdsl[2].pattern == L"+bell");
}

TEST_F(FTApi, FTDslParserExactMatchTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("=moskva77");
	EXPECT_TRUE(ftdsl.size() == 1);
	EXPECT_TRUE(ftdsl[0].opts.exact);
	EXPECT_TRUE(ftdsl[0].pattern == L"moskva77");
}

TEST_F(FTApi, NumberToWordsSelect) {
	Init(GetDefaultConfig());
	Add("оценка 5 майкл джордан 23"sv, ""sv);

	CheckAllPermutations("", {"пять", "+двадцать", "+три"}, "", {{"оценка !5! майкл джордан !23!", ""}});
}

// Make sure FT seeks by a huge number set by string in DSL
TEST_F(FTApi, HugeNumberToWordsSelect) {
	// Initialize namespace
	Init(GetDefaultConfig());
	// Add a record with a big number
	Add("много 7343121521906522180408440 денег"sv, ""sv);
	// Execute FT query, where search words are set as strings
	QueryResults qr = SimpleSelect(
		"+семь +септиллионов +триста +сорок +три +секстиллиона +сто +двадцать +один +квинтиллион +пятьсот +двадцать +один +квадриллион "
		"+девятьсот +шесть +триллионов +пятьсот +двадцать +два +миллиарда +сто +восемьдесят +миллионов +четыреста +восемь +тысяч "
		"+четыреста +сорок");
	// Make sure it found this only string
	ASSERT_TRUE(qr.Count() == 1);
}

// Make sure way too huge numbers are ignored in FT
TEST_F(FTApi, HugeNumberToWordsSelect2) {
	// Initialize namespace
	Init(GetDefaultConfig());
	// Add a record with a huge number
	Add("1127343121521906522180408440"sv, ""sv);
	// Execute FT query, where search words are set as strings
	QueryResults qr;
	const std::string searchWord =
		"+один +октиллион +сто +двадцать +семь +септиллионов +триста +сорок +три +секстиллиона +сто +двадцать +один +квинтиллион +пятьсот "
		"+двадцать +один +квадриллион +девятьсот +шесть +триллионов +пятьсот +двадцать +два +миллиарда +сто +восемьдесят +миллионов "
		"+четыреста +восемь +тысяч +четыреста +сорок";
	Query q = std::move(Query("nm1").Where("ft3", CondEq, searchWord));
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	// Make sure it has found absolutely nothing
	ASSERT_TRUE(qr.Count() == 0);
}

TEST_F(FTApi, DeleteTest) {
	Init(GetDefaultConfig());

	std::unordered_map<string, int> data;
	for (int i = 0; i < 10000; ++i) {
		data.insert(Add(RuRandString()));
	}
	auto res = SimpleSelect("entity");
	for (int i = 0; i < 10000; ++i) {
		data.insert(Add(RuRandString()));
	}
	res = SimpleSelect("entity");

	data.insert(Add("An entity is something that exists as itself"sv));
	data.insert(Add("In law, a legal entity is an entity that is capable of bearing legal rights"sv));
	data.insert(Add("In politics, entity is used as term for territorial divisions of some countries"sv));
	data.insert(Add("Юридическое лицо — организация, которая имеет обособленное имущество"sv));
	data.insert(Add("Aftermath - the consequences or aftereffects of a significant unpleasant event"sv));
	data.insert(Add("Food prices soared in the aftermath of the drought"sv));
	data.insert(Add("In the aftermath of the war ..."sv));

	//  Delete(data[1].first);
	// Delete(data[1].first);

	Delete(data.find("In law, a legal entity is an entity that is capable of bearing legal rights")->second);
	res = SimpleSelect("entity");

	// for (auto it : res) {
	// 	Item ritem(it.GetItem());
	// 	std::cout << ritem["ft1"].As<string>() << std::endl;
	// }
	// TODO: add validation
}

TEST_F(FTApi, Stress) {
	Init(GetDefaultConfig());

	vector<string> data;
	vector<string> phrase;

	for (size_t i = 0; i < 100000; ++i) {
		data.push_back(RandString());
	}

	for (size_t i = 0; i < 7000; ++i) {
		phrase.push_back(data[rand() % data.size()] + "  " + data[rand() % data.size()] + " " + data[rand() % data.size()]);
	}

	for (size_t i = 0; i < phrase.size(); i++) {
		Add(phrase[i], phrase[rand() % phrase.size()]);
		if (i % 500 == 0) {
			for (size_t j = 0; j < i; j++) {
				auto res = StressSelect(phrase[j]);
				bool found = false;
				if (!res.Count()) {
					abort();
				}

				for (auto it : res) {
					Item ritem(it.GetItem(false));
					if (ritem["ft1"].As<string>() == phrase[j]) {
						found = true;
					}
				}
				if (!found) {
					abort();
				}
			}
		}
	}
}
TEST_F(FTApi, Unique) {
	Init(GetDefaultConfig());

	std::vector<string> data;
	std::set<size_t> check;
	std::set<string> checks;
	reindexer::logInstallWriter([](int, char*) { /*std::cout << buf << std::endl;*/ });

	for (int i = 0; i < 1000; ++i) {
		bool inserted = false;
		size_t n;
		string s;

		while (!inserted) {
			n = rand();
			auto res = check.insert(n);
			inserted = res.second;
		}

		inserted = false;

		while (!inserted) {
			s = RandString();
			auto res = checks.insert(s);
			inserted = res.second;
		}

		data.push_back(s + std::to_string(n));
	}

	for (size_t i = 0; i < data.size(); i++) {
		Add(data[i], data[i]);
		if (i % 5 == 0) {
			for (size_t j = 0; j < i; j++) {
				if (i == 40 && j == 26) {
					int a = 3;
					a++;
				}
				auto res = StressSelect(data[j]);
				if (res.Count() != 1) {
					for (auto it : res) {
						Item ritem(it.GetItem(false));
					}
					abort();
				}
			}
		}
	}
}

TEST_F(FTApi, SummationOfRanksInSeveralFields) {
	auto ftCfg = GetDefaultConfig(3);
	ftCfg.summationRanksByFieldsRatio = 0.0f;
	Init(ftCfg, NS3);

	Add("nm3"sv, "word"sv, "word"sv, "word"sv);
	Add("nm3"sv, "word"sv, "test"sv, "test"sv);
	Add("nm3"sv, "test"sv, "word"sv, "test"sv);
	Add("nm3"sv, "test"sv, "test"sv, "word"sv);
	uint16_t rank = 0;
	// Do not sum ranks by fields, as it is not asked in request and sum ratio in config is zero
	const auto queries = CreateAllPermutatedQueries("@", {"ft1", "ft2", "ft3"}, " word", ",");
	for (size_t i = 0; i < queries.size(); ++i) {
		const auto& q = queries[i];
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		auto it = qr.begin();
		if (i == 0) {
			rank = it.GetItemRef().Proc();
		}
		for (const auto end = qr.end(); it != end; ++it) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	// Do not sum ranks by fields, inspite of it is asked in request, as sum ratio in config is zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "+ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		for (const auto& it : qr) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	// Do not sum ranks by fields, inspite of it is asked in request, as sum ratio in config is zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+*"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		for (const auto& it : qr) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	ftCfg.summationRanksByFieldsRatio = 1.0f;
	Error err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
	ASSERT_TRUE(err.ok()) << err.what();
	Add("nm3"sv, "test"sv, "test"sv, "test"sv);
	// Do not sum ranks by fields, inspite of sum ratio in config is not zero, as it is not asked in request
	for (const auto& q : CreateAllPermutatedQueries("@", {"ft1", "ft2", "ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		for (const auto& it : qr) {
			EXPECT_EQ(rank, it.GetItemRef().Proc()) << q;
		}
	}

	// Do sum ranks by fields, as it is asked in request and sum ratio in config is not zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "+ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		auto it = qr.begin();
		rank = it.GetItemRef().Proc() / 3;
		++it;
		for (const auto end = qr.end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// Do sum ranks by fields, as it is asked in request and sum ratio in config is not zero
	for (const auto& q : CreateAllPermutatedQueries("@", {"+*"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		auto it = qr.begin();
		rank = it.GetItemRef().Proc() / 3;
		++it;
		for (const auto end = qr.end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// ft2 is skipped as is not marked with +
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		auto it = qr.begin();
		rank = it.GetItemRef().Proc() / 2;
		++it;
		for (const auto end = qr.end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// ft2 is not skipped as it has max rank
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "ft2^2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		auto it = qr.begin();
		rank = it.GetItemRef().Proc() / 4;
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), (rank + 1) * 2) << q;
		EXPECT_GE(it.GetItemRef().Proc(), (rank - 1) * 2) << q;
		++it;
		for (const auto end = qr.end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// Ranks summated with ratio 0.5
	ftCfg.summationRanksByFieldsRatio = 0.5f;
	err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
	ASSERT_TRUE(err.ok()) << err.what();
	Add("nm3"sv, "test"sv, "test"sv, "test"sv);
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1", "+ft2", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 false);
		auto it = qr.begin();
		rank = it.GetItemRef().Proc() / (1.0 + 0.5 + 0.5 * 0.5);
		++it;
		for (const auto end = qr.end(); it != end; ++it) {
			EXPECT_LE(it.GetItemRef().Proc(), rank + 1) << q;
			EXPECT_GE(it.GetItemRef().Proc(), rank - 1) << q;
		}
	}

	// Ranks summated with ratio 0.5
	for (const auto& q : CreateAllPermutatedQueries("@", {"+ft1^1.5", "+ft2^1.3", "+ft3"}, " word", ",")) {
		const auto qr = SimpleSelect3(q);
		CheckResults(q, qr,
					 {{"!word!", "!word!", "!word!"}, {"!word!", "test", "test"}, {"test", "!word!", "test"}, {"test", "test", "!word!"}},
					 true);
		auto it = qr.begin();
		rank = it.GetItemRef().Proc() / (1.5 + 0.5 * 1.3 + 0.5 * 0.5);
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), (rank + 5) * 1.5) << q;
		EXPECT_GE(it.GetItemRef().Proc(), (rank - 5) * 1.5) << q;
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), (rank + 5) * 1.3) << q;
		EXPECT_GE(it.GetItemRef().Proc(), (rank - 5) * 1.3) << q;
		++it;
		EXPECT_LE(it.GetItemRef().Proc(), rank + 5) << q;
		EXPECT_GE(it.GetItemRef().Proc(), rank - 5) << q;
	}
}

TEST_F(FTApi, SelectSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"лыв", "лав"}, {"love"}}, {{"лар", "hate"}, {"rex", "looove"}}};
	Init(ftCfg);

	Add("nm1"sv, "test"sv, "love rex"sv);
	Add("nm1"sv, "test"sv, "no looove"sv);
	Add("nm1"sv, "test"sv, "no match"sv);

	CheckAllPermutations("", {"лыв"}, "", {{"test", "!love! rex"}});
	CheckAllPermutations("", {"hate"}, "", {{"test", "love !rex!"}, {"test", "no !looove!"}});
}

TEST_F(FTApi, SelectTranslitWithComma) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.logLevel = 5;
	Init(ftCfg);

	Add("nm1"sv, "хлебопечка"sv, ""sv);
	Add("nm1"sv, "электрон"sv, ""sv);
	Add("nm1"sv, "матэ"sv, ""sv);

	auto qr = SimpleSelect("@ft1 [kt,jgtxrf");
	EXPECT_EQ(qr.Count(), 1);
	auto item = qr[0].GetItem(false);
	EXPECT_EQ(item["ft1"].As<string>(), "!хлебопечка!");

	qr = SimpleSelect("@ft1 \\'ktrnhjy");
	EXPECT_EQ(qr.Count(), 1);
	item = qr[0].GetItem(false);
	EXPECT_EQ(item["ft1"].As<string>(), "!электрон!");

	qr = SimpleSelect("@ft1 vfn\\'");
	EXPECT_EQ(qr.Count(), 1);
	item = qr[0].GetItem(false);
	EXPECT_EQ(item["ft1"].As<string>(), "!матэ!");
}

TEST_F(FTApi, SelectMultiwordSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"whole world", "UN", "United Nations"},
					   {"UN", "ООН", "целый мир", "планета", "генеральная ассамблея организации объединенных наций"}},
					  {{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1"sv, "whole world"sv, "test"sv);
	Add("nm1"sv, "world whole"sv, "test"sv);
	Add("nm1"sv, "whole"sv, "world"sv);
	Add("nm1"sv, "world"sv, "test"sv);
	Add("nm1"sv, "whole"sv, "test"sv);
	Add("nm1"sv, "целый мир"sv, "test"sv);
	Add("nm1"sv, "целый"sv, "мир"sv);
	Add("nm1"sv, "целый"sv, "test"sv);
	Add("nm1"sv, "мир"sv, "test"sv);
	Add("nm1"sv, "планета"sv, "test"sv);
	Add("nm1"sv, "генеральная ассамблея организации объединенных наций"sv, "test"sv);
	Add("nm1"sv, "ассамблея генеральная наций объединенных организации"sv, "test"sv);
	Add("nm1"sv, "генеральная прегенеральная ассамблея"sv, "организации объединенных свободных наций"sv);
	Add("nm1"sv, "генеральная прегенеральная "sv, "организации объединенных свободных наций"sv);
	Add("nm1"sv, "UN"sv, "UN"sv);

	Add("nm1"sv, "word"sv, "test"sv);
	Add("nm1"sv, "test"sv, "word"sv);
	Add("nm1"sv, "word"sv, "слово"sv);
	Add("nm1"sv, "word"sv, "одно"sv);
	Add("nm1"sv, "слово"sv, "test"sv);
	Add("nm1"sv, "слово всего лишь одно"sv, "test"sv);
	Add("nm1"sv, "одно"sv, "test"sv);
	Add("nm1"sv, "слово"sv, "одно"sv);
	Add("nm1"sv, "слово одно"sv, "test"sv);
	Add("nm1"sv, "одно слово"sv, "word"sv);

	CheckAllPermutations("", {"world"}, "",
						 {{"whole !world!", "test"}, {"!world! whole", "test"}, {"whole", "!world!"}, {"!world!", "test"}});

	CheckAllPermutations("", {"whole", "world"}, "",
						 {{"!whole world!", "test"},
						  {"!world whole!", "test"},
						  {"!whole!", "!world!"},
						  {"!world!", "test"},
						  {"!whole!", "test"},
						  {"!целый мир!", "test"},
						  {"!целый!", "!мир!"},
						  {"!планета!", "test"},
						  {"!генеральная ассамблея организации объединенных наций!", "test"},
						  {"!ассамблея генеральная наций объединенных организации!", "test"},
						  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
						  {"!UN!", "!UN!"}});

	CheckAllPermutations("", {"UN"}, "",
						 {{"!целый мир!", "test"},
						  {"!целый!", "!мир!"},
						  {"!планета!", "test"},
						  {"!генеральная ассамблея организации объединенных наций!", "test"},
						  {"!ассамблея генеральная наций объединенных организации!", "test"},
						  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
						  {"!UN!", "!UN!"}});

	CheckAllPermutations("", {"United", "+Nations"}, "",
						 {{"!целый мир!", "test"},
						  {"!целый!", "!мир!"},
						  {"!планета!", "test"},
						  {"!генеральная ассамблея организации объединенных наций!", "test"},
						  {"!ассамблея генеральная наций объединенных организации!", "test"},
						  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
						  {"!UN!", "!UN!"}});

	CheckAllPermutations("", {"целый", "мир"}, "", {{"!целый мир!", "test"}, {"!целый!", "test"}, {"!мир!", "test"}, {"!целый!", "!мир!"}});

	CheckAllPermutations("", {"ООН"}, "", {});

	CheckAllPermutations("", {"word"}, "",
						 {{"!word!", "test"},
						  {"test", "!word!"},
						  {"!word!", "слово"},
						  {"!word!", "одно"},
						  {"!слово! всего лишь !одно!", "test"},
						  {"!слово!", "!одно!"},
						  {"!слово одно!", "test"},
						  {"!одно слово!", "!word!"}});
}

// issue #715
TEST_F(FTApi, SelectMultiwordSynonyms2) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"черный"}, {"серый космос"}}};
	Init(ftCfg);

	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 черный"sv, "SAMSUNG"sv);
	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 серый"sv, "SAMSUNG"sv);
	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 красный"sv, "SAMSUNG"sv);
	Add("nm1"sv, "Смартфон SAMSUNG Galaxy S20 серый космос"sv, "SAMSUNG"sv);

	// Check all combinations of "+samsung" and "+galaxy" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 черный", "!SAMSUNG!"},
						  {"Смартфон !SAMSUNG Galaxy! S20 серый", "!SAMSUNG!"},
						  {"Смартфон !SAMSUNG Galaxy! S20 красный", "!SAMSUNG!"},
						  {"Смартфон !SAMSUNG Galaxy! S20 серый космос", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy" and "+серый" in request
	CheckAllPermutations(
		"@ft1 ", {"+samsung", "+galaxy", "+серый"}, "",
		{{"Смартфон !SAMSUNG Galaxy! S20 !серый!", "!SAMSUNG!"}, {"Смартфон !SAMSUNG Galaxy! S20 !серый! космос", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+серый" and "+космос" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+серый", "+космос"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy" and "+черный" in request
	CheckAllPermutations(
		"@ft1 ", {"+samsung", "+galaxy", "+черный"}, "",
		{{"Смартфон !SAMSUNG Galaxy! S20 !черный!", "!SAMSUNG!"}, {"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+серый" and "+серый" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+серый"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+черный" and "+космос" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+космос"}, "",
						 {{"Смартфон !SAMSUNG Galaxy! S20 !серый космос!", "!SAMSUNG!"}});

	// Check all combinations of "+samsung", "+galaxy", "+черный" and "+somthing" in request
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+something"}, "", {});
	CheckAllPermutations("@ft1 ", {"+samsung", "+galaxy", "+черный", "+черный", "+черный", "+something"}, "", {});
}

TEST_F(FTApi, SelectWithMinusWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word", "several lexems"}, {"слово", "сколькото лексем"}}};
	Init(ftCfg);

	Add("nm1"sv, "word"sv, "test"sv);
	Add("nm1"sv, "several lexems"sv, "test"sv);
	Add("nm1"sv, "слово"sv, "test"sv);
	Add("nm1"sv, "сколькото лексем"sv, "test"sv);

	CheckAllPermutations("", {"test", "word"}, "",
						 {{"!word!", "!test!"}, {"several lexems", "!test!"}, {"!слово!", "!test!"}, {"!сколькото лексем!", "!test!"}});
	// Don't use synonyms
	CheckAllPermutations("", {"test", "-word"}, "", {{"several lexems", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
	CheckAllPermutations("", {"test", "several", "lexems"}, "",
						 {{"word", "!test!"}, {"!several lexems!", "!test!"}, {"!слово!", "!test!"}, {"!сколькото лексем!", "!test!"}});
	// Don't use synonyms
	CheckAllPermutations("", {"test", "several", "-lexems"}, "", {{"word", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
	// Don't use synonyms
	CheckAllPermutations("", {"test", "-several", "lexems"}, "", {{"word", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
}

// issue #627
TEST_F(FTApi, SelectMultiwordSynonymsWithExtraWords) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {
		{{"бронестекло", "защитное стекло", "бронированное стекло"}, {"бронестекло", "защитное стекло", "бронированное стекло"}}};
	Init(ftCfg);

	Add("nm1"sv, "защитное стекло для экрана samsung galaxy"sv, "test"sv);
	Add("nm1"sv, "защитное стекло для экрана iphone"sv, "test"sv);
	Add("nm1"sv, "бронированное стекло для samsung galaxy"sv, "test"sv);
	Add("nm1"sv, "бронированное стекло для экрана iphone"sv, "test"sv);
	Add("nm1"sv, "бронестекло для экрана samsung galaxy"sv, "test"sv);
	Add("nm1"sv, "бронестекло для экрана iphone"sv, "test"sv);

	CheckAllPermutations("", {"бронестекло"}, "",
						 {{"!защитное стекло! для экрана samsung galaxy", "test"},
						  {"!защитное стекло! для экрана iphone", "test"},
						  {"!бронированное стекло! для samsung galaxy", "test"},
						  {"!бронированное стекло! для экрана iphone", "test"},
						  {"!бронестекло! для экрана samsung galaxy", "test"},
						  {"!бронестекло! для экрана iphone", "test"}});

	CheckAllPermutations("", {"+бронестекло", "+iphone"}, "",
						 {{"!защитное стекло! для экрана !iphone!", "test"},
						  {"!бронированное стекло! для экрана !iphone!", "test"},
						  {"!бронестекло! для экрана !iphone!", "test"}});

	CheckAllPermutations("", {"+galaxy", "+бронестекло", "+samsung"}, "",
						 {{"!защитное стекло! для экрана !samsung galaxy!", "test"},
						  {"!бронированное стекло! для !samsung galaxy!", "test"},
						  {"!бронестекло! для экрана !samsung galaxy!", "test"}});

	CheckAllPermutations("", {"+galaxy", "+бронестекло", "экрана", "+samsung"}, "",
						 {{"!защитное стекло! для !экрана samsung galaxy!", "test"},
						  {"!бронированное стекло! для !samsung galaxy!", "test"},
						  {"!бронестекло! для !экрана samsung galaxy!", "test"}});

	CheckAllPermutations("", {"+galaxy", "+бронестекло", "какоетослово", "+samsung"}, "",
						 {{"!защитное стекло! для экрана !samsung galaxy!", "test"},
						  {"!бронированное стекло! для !samsung galaxy!", "test"},
						  {"!бронестекло! для экрана !samsung galaxy!", "test"}});

	CheckAllPermutations("", {"+бронестекло", "+iphone", "+samsung"}, "", {});
}

TEST_F(FTApi, ChangeSynonymsCfg) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	Add("nm1"sv, "UN"sv, "test"sv);
	Add("nm1"sv, "United Nations"sv, "test"sv);
	Add("nm1"sv, "ООН"sv, "test"sv);
	Add("nm1"sv, "организация объединенных наций"sv, "test"sv);

	Add("nm1"sv, "word"sv, "test"sv);
	Add("nm1"sv, "several lexems"sv, "test"sv);
	Add("nm1"sv, "слово"sv, "test"sv);
	Add("nm1"sv, "сколькото лексем"sv, "test"sv);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "", {{"!United Nations!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "", {{"!several lexems!", "test"}});

	// Add synonyms
	ftCfg.synonyms = {{{"UN", "United Nations"}, {"ООН", "организация объединенных наций"}}};
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}, {"!ООН!", "test"}, {"!организация объединенных наций!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "",
						 {{"!United Nations!", "test"}, {"!ООН!", "test"}, {"!организация объединенных наций!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "", {{"!several lexems!", "test"}});

	// Change synonyms
	ftCfg.synonyms = {{{"word", "several lexems"}, {"слово", "сколькото лексем"}}};
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "", {{"!United Nations!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}, {"!слово!", "test"}, {"!сколькото лексем!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "",
						 {{"!several lexems!", "test"}, {"!слово!", "test"}, {"!сколькото лексем!", "test"}});

	// Remove synonyms
	ftCfg.synonyms.clear();
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"UN"}, "", {{"!UN!", "test"}});
	CheckAllPermutations("", {"United", "Nations"}, "", {{"!United Nations!", "test"}});
	CheckAllPermutations("", {"word"}, "", {{"!word!", "test"}});
	CheckAllPermutations("", {"several", "lexems"}, "", {{"!several lexems!", "test"}});
}

TEST_F(FTApi, SelectWithRelevanceBoostWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}, {{"United Nations"}, {"ООН"}}};
	Init(ftCfg);

	Add("nm1"sv, "одно слово"sv, ""sv);
	Add("nm1"sv, ""sv, "ООН"sv);

	CheckAllPermutations("", {"word^2", "United^0.5", "Nations"}, "", {{"!одно слово!", ""}, {"", "!ООН!"}}, true);
	CheckAllPermutations("", {"word^0.5", "United^2", "Nations^0.5"}, "", {{"", "!ООН!"}, {"!одно слово!", ""}}, true);
}

TEST_F(FTApi, SelectWithFieldsBoostWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1"sv, "одно слово"sv, ""sv);
	Add("nm1"sv, "одно"sv, "слово"sv);
	Add("nm1"sv, ""sv, "одно слово"sv);

	CheckAllPermutations("@", {"ft1^2", "ft2^0.5"}, " word", {{"!одно слово!", ""}, {"!одно!", "!слово!"}, {"", "!одно слово!"}}, true,
						 ", ");
	CheckAllPermutations("@", {"ft1^0.5", "ft2^2"}, " word", {{"", "!одно слово!"}, {"!одно!", "!слово!"}, {"!одно слово!", ""}}, true,
						 ", ");
}

TEST_F(FTApi, SelectWithFieldsListWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1"sv, "одно слово"sv, ""sv);
	Add("nm1"sv, "одно"sv, "слово"sv);
	Add("nm1"sv, ""sv, "одно слово"sv);

	CheckAllPermutations("", {"word"}, "", {{"!одно слово!", ""}, {"!одно!", "!слово!"}, {"", "!одно слово!"}});
	CheckAllPermutations("@ft1 ", {"word"}, "", {{"!одно слово!", ""}});
	CheckAllPermutations("@ft2 ", {"word"}, "", {{"", "!одно слово!"}});
}

TEST_F(FTApi, RankWithPosition) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.fieldsCfg[0].positionWeight = 1.0;
	Init(ftCfg);

	Add("nm1"sv, "one two three word"sv, ""sv);
	Add("nm1"sv, "one two three four five six word"sv, ""sv);
	Add("nm1"sv, "one two three four word"sv, ""sv);
	Add("nm1"sv, "one word"sv, ""sv);
	Add("nm1"sv, "one two three four five word"sv, ""sv);
	Add("nm1"sv, "word"sv, ""sv);
	Add("nm1"sv, "one two word"sv, ""sv);

	CheckAllPermutations("", {"word"}, "",
						 {{"!word!", ""},
						  {"one !word!", ""},
						  {"one two !word!", ""},
						  {"one two three !word!", ""},
						  {"one two three four !word!", ""},
						  {"one two three four five !word!", ""},
						  {"one two three four five six !word!", ""}},
						 true);
}

TEST_F(FTApi, DifferentFieldRankPosition) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.fieldsCfg[0].positionWeight = 1.0;
	ftCfg.fieldsCfg[0].positionBoost = 10.0;
	Init(ftCfg);

	Add("nm1"sv, "one two three word"sv, "one word"sv);
	Add("nm1"sv, "one two three four five six word"sv, "one two word"sv);
	Add("nm1"sv, "one two three four word"sv, "one two three four five word"sv);
	Add("nm1"sv, "one word"sv, "one two three four five six word"sv);
	Add("nm1"sv, "one two three four five word"sv, "one two three word"sv);
	Add("nm1"sv, "word"sv, "one two three four word"sv);
	Add("nm1"sv, "one two word"sv, "word"sv);

	CheckAllPermutations("", {"word"}, "",
						 {{"!word!", "one two three four !word!"},
						  {"one !word!", "one two three four five six !word!"},
						  {"one two !word!", "!word!"},
						  {"one two three !word!", "one !word!"},
						  {"one two three four !word!", "one two three four five !word!"},
						  {"one two three four five !word!", "one two three !word!"},
						  {"one two three four five six !word!", "one two !word!"}},
						 true);

	ftCfg.fieldsCfg[0].positionWeight = 0.1;
	ftCfg.fieldsCfg[0].positionBoost = 1.0;
	ftCfg.fieldsCfg[1].positionWeight = 1.0;
	ftCfg.fieldsCfg[1].positionBoost = 10.0;
	SetFTConfig(ftCfg);

	CheckAllPermutations("", {"word"}, "",
						 {{"one two !word!", "!word!"},
						  {"one two three !word!", "one !word!"},
						  {"one two three four five six !word!", "one two !word!"},
						  {"one two three four five !word!", "one two three !word!"},
						  {"!word!", "one two three four !word!"},
						  {"one two three four !word!", "one two three four five !word!"},
						  {"one !word!", "one two three four five six !word!"}},
						 true);
}

TEST_F(FTApi, PartialMatchRank) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.partialMatchDecrease = 0;
	Init(ftCfg);

	Add("nm1"sv, "ТНТ4"sv, ""sv);
	Add("nm1"sv, ""sv, "ТНТ"sv);

	CheckAllPermutations("@", {"ft1^1.1", "ft2^1"}, " ТНТ*", {{"!ТНТ4!", ""}, {"", "!ТНТ!"}}, true, ", ");

	ftCfg.partialMatchDecrease = 100;
	SetFTConfig(ftCfg);

	CheckAllPermutations("@", {"ft1^1.1", "ft2^1"}, " ТНТ*", {{"", "!ТНТ!"}, {"!ТНТ4!", ""}}, true, ", ");
}

TEST_F(FTApi, SelectFullMatch) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.fullMatchBoost = 0.9;
	Init(ftCfg);

	Add("nm1"sv, "test"sv, "love"sv);
	Add("nm1"sv, "test"sv, "love second"sv);

	CheckAllPermutations("", {"love"}, "", {{"test", "!love! second"}, {"test", "!love!"}}, true);

	ftCfg.fullMatchBoost = 1.1;
	SetFTConfig(ftCfg);
	CheckAllPermutations("", {"love"}, "", {{"test", "!love!"}, {"test", "!love! second"}}, true);
}

TEST_F(FTApi, SetFtFieldsCfgErrors) {
	auto cfg = GetDefaultConfig(2);
	Init(cfg);
	cfg.fieldsCfg[0].positionWeight = 0.1;
	cfg.fieldsCfg[1].positionWeight = 0.2;
	// Задаем уникальный конфиг для поля ft, которого нет в индексе ft3
	auto err = SetFTConfig(cfg, "nm1", "ft3", {"ft", "ft2"});
	// Получаем ошибку
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Field 'ft' is not included to full text index");

	// Задаем уникальный конфиг дважды для одного поля ft1
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft1"});
	// Получаем ошибку
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Field 'ft1' is dublicated in fulltext configuration");

	err = rt.reindexer->OpenNamespace("nm3");
	ASSERT_TRUE(err.ok()) << err.what();
	DefineNamespaceDataset(
		"nm3", {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"ft", "text", "string", IndexOpts(), 0}});
	// Задаем уникальный конфиг для единственного поля ft в индексе ft
	err = SetFTConfig(cfg, "nm3", "ft", {"ft"});
	// Получаем ошибку
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Configuration for single field fulltext index cannot contain field specifications");

	// maxTypos < 0
	cfg.maxTypos = -1;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	// Error
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "FtFastConfig: Value of 'max_typos' - -1 is out of bounds: [0,4]");

	// maxTypos > 4
	cfg.maxTypos = 5;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	// Error
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "FtFastConfig: Value of 'max_typos' - 5 is out of bounds: [0,4]");
}
