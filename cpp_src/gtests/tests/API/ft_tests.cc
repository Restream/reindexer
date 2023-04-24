#include <gtest/gtest-param-test.h>
#include <condition_variable>
#include <iostream>
#include <limits>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include "core/ft/config/ftfastconfig.h"
#include "core/ft/ftdsl.h"
#include "debug/allocdebug.h"
#include "ft_api.h"
#include "tools/fsops.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

using namespace std::string_view_literals;

TEST_P(FTApi, CompositeSelect) {
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

		rt.PrintQueryResults("nm1", res);
		for (auto it : res) {
			auto ritem(it.GetItem(false));
			for (auto idx = 1; idx < ritem.NumFields(); idx++) {
				auto field = ritem[idx].Name();
				if (field == "id") continue;
				auto it = data.find(ritem[field].As<std::string>());
				ASSERT_TRUE(it != data.end());
				data.erase(it);
			}
		}
		EXPECT_TRUE(data.empty());
	}
}

TEST_P(FTApi, CompositeSelectWithFields) {
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

			rt.PrintQueryResults("nm1", res);
			for (auto it : res) {
				auto ritem(it.GetItem(false));
				for (auto idx = 1; idx < ritem.NumFields(); idx++) {
					auto curField = ritem[idx].Name();
					if (curField != field) continue;
					auto it = data.find(ritem[curField].As<std::string>());
					ASSERT_TRUE(it != data.end());
					data.erase(it);
				}
			}
			EXPECT_TRUE(data.empty());
		}
	}
}

TEST_P(FTApi, CompositeRankWithSynonyms) {
	auto cfg = GetDefaultConfig();
	cfg.synonyms = {{{"word"}, {"слово"}}};
	Init(cfg);
	Add("word"sv, "слово"sv);
	Add("world"sv, "world"sv);

	// rank of synonym is higher
	CheckAllPermutations("@", {"ft1^0.5", "ft2^2"}, " word~", {{"word", "!слово!"}, {"!world!", "!world!"}}, true, ", ");
}

TEST_P(FTApi, SelectWithEscaping) {
	reindexer::FtFastConfig ftCfg = GetDefaultConfig();
	ftCfg.extraWordSymbols = "+-\\";
	Init(ftCfg);
	Add("Go to -hell+hell+hell!!"sv);

	auto res = SimpleSelect("\\-hell\\+hell\\+hell");
	EXPECT_TRUE(res.Count() == 1);

	for (auto it : res) {
		auto ritem(it.GetItem(false));
		std::string val = ritem["ft1"].As<std::string>();
		EXPECT_TRUE(val == "Go to !-hell+hell+hell!!!");
	}
}

TEST_P(FTApi, SelectWithPlus) {
	Init(GetDefaultConfig());

	Add("added three words"sv);
	Add("added something else"sv);

	CheckAllPermutations("", {"+added"}, "", {{"!added! something else", ""}, {"!added! three words", ""}});
}

TEST_P(FTApi, SelectWithPlusWithSingleAlternative) {
	auto cfg = GetDefaultConfig();
	cfg.enableKbLayout = false;
	cfg.enableTranslit = false;
	Init(cfg);

	Add("мониторы"sv);

	// FT search by single mandatory word with single alternative
	CheckAllPermutations("", {"+монитор*"}, "", {{"!мониторы!", ""}});
}

TEST_P(FTApi, SelectWithMinus) {
	Init(GetDefaultConfig());

	Add("including me, excluding you"sv);
	Add("including all of them"sv);

	CheckAllPermutations("", {"+including", "-excluding"}, "", {{"!including! all of them", ""}});
	CheckAllPermutations("", {"including", "-excluding"}, "", {{"!including! all of them", ""}});
}

TEST_P(FTApi, SelectWithFieldsList) {
	Init(GetDefaultConfig());

	Add("nm1"sv, "Never watch their games"sv, "Because nothing can be worse than Spartak Moscow"sv);
	Add("nm1"sv, "Spartak Moscow is the worst team right now"sv, "Yes, for sure"sv);

	CheckAllPermutations("@ft1 ", {"Spartak", "Moscow"}, "", {{"!Spartak Moscow! is the worst team right now", "Yes, for sure"}});
}

TEST_P(FTApi, SelectWithRelevanceBoost) {
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

TEST_P(FTApi, SelectWithDistance) {
	Init(GetDefaultConfig());

	Add("Her nose was very very long"sv);
	Add("Her nose was exceptionally long"sv);
	Add("Her nose was long"sv);

	CheckResults("'nose long'~3", {{"Her !nose was long!", ""}, {"Her !nose was exceptionally long!", ""}}, true);
}

TEST_P(FTApi, SelectWithDistance2) {
	auto check = [&](bool withHighlight) {
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two!", ""}, {"!one ецщ!", ""}, {"empty !one two!", ""}, {"empty !one two! word", ""}};
			CheckResults(R"s("one two")s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two!", ""}, {"!one ецщ!", ""}, {"empty !one two!", ""}, {"empty !one two! word", ""}};
			CheckResults(R"s("one two"~1)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two!", ""},
																				  {"!one ецщ!", ""},
																				  {"empty !one two!", ""},
																				  {"!one empty two!", ""},
																				  {"empty !one two! word", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""}};
			CheckResults(R"s(+"one two"~3)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two!", ""},
																				  {"!one ецщ!", ""},
																				  {"empty !one two!", ""},
																				  {"!one empty two!", ""},
																				  {"empty !one two! word", ""},
																				  {"word !one empty two!", ""},
																				  {"word !one empty empty two! word", ""}};
			CheckResults(R"s("one two"~3)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two!", ""}, {"!one ецщ!", ""}, {"!empty one two!", ""}, {"!empty one two! word", ""}};
			CheckAllPermutations("", {"empty", R"s(+"one two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty one two!", ""}, {"!empty one two! word", ""}};
			CheckAllPermutations("", {"+empty", R"s(+"one two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"!one two!", ""},
																				  {"!one ецщ!", ""},
																				  {"!empty one two!", ""},
																				  {"!empty one two! word", ""},
																				  {"one !empty! two", ""},
																				  {"word one !empty empty! two word", ""},
																				  {"word one !empty empty empty! two word", ""},
																				  {"word one !empty! two", ""},
																				  {"word one !empty empty empty! two two word", ""},
																				  {"word one one !empty empty empty! two word", ""}};
			CheckAllPermutations("", {"empty", R"s("one two")s"}, "", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH),
								 false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two", ""},
																				  {"word one !empty! two", ""},
																				  {"word one !empty empty! two word", ""},
																				  {"word one !empty empty empty! two word", ""},
																				  {"word one !empty empty empty! two two word", ""},
																				  {"word one one !empty empty empty! two word", ""}};
			CheckAllPermutations("", {"empty", R"s(-"one two")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two", ""},
																				  {"word one !empty! two", ""},
																				  {"word one !empty empty! two word", ""},
																				  {"word one !empty empty empty! two word", ""},
																				  {"word one !empty empty empty! two two word", ""},
																				  {"word one one !empty empty empty! two word", ""}};
			CheckAllPermutations("", {R"s(-"one two")s", "+empty"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		CheckAllPermutations("", {R"s(-"one two")s", "-empty"}, "", {}, false, " ", withHighlight);
	};

	Init(GetDefaultConfig());

	Add("one"sv);
	Add("two"sv);
	Add("empty"sv);
	Add("one two"sv);
	Add("empty one two"sv);
	Add("empty one two word"sv);
	Add("one empty two"sv);
	Add("word one empty two"sv);
	Add("word one empty empty two word"sv);
	Add("word one empty empty empty two word"sv);
	Add("one ецщ"sv);
	Add("word one empty empty empty two two word"sv);
	Add("word one one empty empty empty two word"sv);

	check(true);
	check(false);
}

TEST_P(FTApi, SelectWithDistance3) {
	Init(GetDefaultConfig());

	Add("one"sv);
	Add("two"sv);
	Add("three"sv);
	Add("empty"sv);
	Add("one two three"sv);
	Add("empty one two three"sv);
	Add("empty one two three word"sv);
	Add("one empty two three"sv);
	Add("word one empty two three"sv);
	Add("word one empty empty two word three"sv);
	Add("word one empty empty empty two word three"sv);
	Add("one ецщ three"sv);
	Add("one two empty two three"sv);
	Add("one two empty two empty empty three"sv);
	auto check = [&](bool withHighlight) {
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two three!", ""}, {"!one ецщ three!", ""}, {"empty !one two three!", ""}, {"empty !one two three! word", ""}};
			CheckResults(R"s("one two three")s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two three!", ""}, {"!one ецщ three!", ""}, {"empty !one two three!", ""}, {"empty !one two three! word", ""}};
			CheckResults(R"s("one two three"~1)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two three!", ""},
																				  {"!one ецщ three!", ""},
																				  {"empty !one two three!", ""},
																				  {"!one empty two three!", ""},
																				  {"empty !one two three! word", ""},
																				  {"word !one empty two three!", ""},
																				  {"word !one empty empty two word three!", ""},
																				  {"!one two empty two three!", ""},
																				  {"!one two empty two empty empty three!", ""}};
			CheckResults(R"s("one two three"~3)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!one two three!", ""},
																				  {"!one ецщ three!", ""},
																				  {"empty !one two three!", ""},
																				  {"!one empty two three!", ""},
																				  {"empty !one two three! word", ""},
																				  {"word !one empty two three!", ""}};
			CheckResults(R"s("one two three"~2)s", withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false,
						 withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {
				{"!one two three!", ""}, {"!one ецщ three!", ""}, {"!empty one two three!", ""}, {"!empty one two three! word", ""}};
			CheckAllPermutations("", {"empty", R"s(+"one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}

		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty one two three!", ""},
																				  {"!empty one two three! word", ""}};
			CheckAllPermutations("", {"+empty", R"s(+"one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}

		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"!one two three!", ""},
																				  {"!one ецщ three!", ""},
																				  {"!empty one two three!", ""},
																				  {"one two !empty! two three", ""},
																				  {"!empty one two three! word", ""},
																				  {"one !empty! two three", ""},
																				  {"word one !empty empty! two word three", ""},
																				  {"word one !empty empty empty! two word three", ""},
																				  {"word one !empty! two three", ""},
																				  {"one two !empty! two !empty empty! three", ""}};
			CheckAllPermutations("", {"empty", R"s("one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two three", ""},
																				  {"word one !empty! two three", ""},
																				  {"word one !empty empty! two word three", ""},
																				  {"word one !empty empty empty! two word three", ""},
																				  {"one two !empty! two three", ""},
																				  {"one two !empty! two !empty empty! three", ""}};
			CheckAllPermutations("", {"empty", R"s(-"one two three")s"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		{
			std::vector<std::tuple<std::string, std::string>> expectedResultsH = {{"!empty!", ""},
																				  {"one !empty! two three", ""},
																				  {"word one !empty! two three", ""},
																				  {"word one !empty empty! two word three", ""},
																				  {"word one !empty empty empty! two word three", ""},
																				  {"one two !empty! two three", ""},
																				  {"one two !empty! two !empty empty! three", ""}};
			CheckAllPermutations("", {R"s(-"one two three")s", "+empty"}, "",
								 withHighlight ? expectedResultsH : DelHighlightSign(expectedResultsH), false, " ", withHighlight);
		}
		CheckResults(R"s(-"one two three"~3)s", {}, false, withHighlight);
	};
	check(true);
	check(false);
}
TEST_P(FTApi, SelectWithDistanceSubTerm) {
	Init(GetDefaultConfig());
	Add("one two empty щту two empty one ецщ"sv);
	CheckResults(R"s("one two")s", {{"!one two! empty !щту two! empty !one ецщ!", ""}}, false, true);
	CheckResults(R"s("one two")s", {{"one two empty щту two empty one ецщ", ""}}, false, false);
}

TEST_P(FTApi, SelectWithMultSynonymArea) {
	reindexer::FtFastConfig config = GetDefaultConfig();
	// hyponyms as synonyms
	config.synonyms = {{{"digit", "one", "two", "three", "big number"}, {"digit", "one", "two", "big number"}},
					   {{"animal", "cat", "dog", "lion"}, {"animal", "cat", "dog", "lion"}}};
	config.maxAreasInDoc = 100;
	Init(config);

	Add("digit cat empty one animal"sv);

	CheckResults(R"s("big number animal")s", {}, false, true);
	CheckResults(R"s("digit animal")s", {{"!digit cat! empty !one animal!", ""}}, false, true);
	CheckResults(R"s("two lion")s", {{"!digit cat! empty !one animal!", ""}}, false, true);
}

TEST_P(FTApi, SelectWithDistance2Field) {
	Init(GetDefaultConfig());
	Add("empty two empty two one"sv, "two"sv);
	// 24 bits - the number of words in the field
	CheckResults("'one two'~" + std::to_string((1 << 24) + 100), {}, false);
}

TEST_P(FTApi, SelectWithSeveralGroup) {
	Init(GetDefaultConfig());

	Add("one empty two word three four"sv);
	Add("word one empty two word three four word"sv);
	Add("one three two four"sv);
	Add("word three one two four word"sv);
	CheckAllPermutations("", {R"s(+"one two"~2)s", R"s(+"three four"~3)s"}, "",
						 {{"!one empty two! word !three four!", ""},
						  {"word !one empty two! word !three four! word", ""},
						  {"!one three two four!", ""},
						  {"word !three one two four! word", ""}},
						 false);
}

TEST_P(FTApi, ConcurrencyCheck) {
	const std::string kStorage = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex_FTApi/ConcurrencyCheck");
	reindexer::fs::RmDirAll(kStorage);
	Init(GetDefaultConfig(), NS1, kStorage);

	Add("Her nose was very very long"sv);
	Add("Her nose was exceptionally long"sv);
	Add("Her nose was long"sv);

	rt.reindexer.reset();
	Init(GetDefaultConfig(), NS1, kStorage);  // Restart rx to drop all the caches

	std::condition_variable cv;
	std::mutex mtx;
	bool ready = false;
	std::vector<std::thread> threads;
	std::atomic<unsigned> runningThreads = {0};
	constexpr unsigned kTotalThreads = 11;
	std::thread statsThread;
	std::atomic<bool> terminate = false;
	for (unsigned i = 0; i < kTotalThreads; ++i) {
		if (i == 0) {
			statsThread = std::thread([&] {
				std::unique_lock lck(mtx);
				++runningThreads;
				cv.wait(lck, [&] { return ready; });
				lck.unlock();
				while (!terminate) {
					reindexer::QueryResults qr;
					rt.reindexer->Select(reindexer::Query("#memstats"), qr);
				}
			});
		} else {
			threads.emplace_back(std::thread([&] {
				std::unique_lock lck(mtx);
				++runningThreads;
				cv.wait(lck, [&] { return ready; });
				lck.unlock();
				CheckResults("'nose long'~3", {{"Her !nose was long!", ""}, {"Her !nose was exceptionally long!", ""}}, true);
			}));
		}
	}
	while (runningThreads.load() < kTotalThreads) {
		std::this_thread::sleep_for(std::chrono::microseconds(100));
	}
	{
		std::lock_guard lck(mtx);
		ready = true;
		cv.notify_all();
	}
	for (auto& th : threads) {
		th.join();
	}
	terminate = true;
	statsThread.join();
}

TEST_P(FTApi, SelectWithTypos) {
	auto cfg = GetDefaultConfig();
	cfg.stopWords.clear();
	cfg.stemmers.clear();
	cfg.enableKbLayout = false;
	cfg.enableTranslit = false;
	const auto kDefaultMaxTypoDist = cfg.maxTypoDistance;

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
	// Or one missing char in any word
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
	cfg.maxTypoDistance = -1;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
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

	cfg.maxTypos = 2;
	cfg.maxTypoDistance = kDefaultMaxTypoDist;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	// Or one letter switch
	// Max typo distance is 0 (by default). Only the letter on the same position may be changed
	// Max letter permutation is 1 (by default). The same letter may be moved by 1
	CheckAllPermutations("", {"ABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {});
	CheckAllPermutations("", {"BXCX~"}, "", {});
	CheckAllPermutations("", {"ACBD~"}, "", {{"!ABCD!", ""}});
	// Not less than 2
	CheckAllPermutations("", {"AB~"}, "", {{"!AB!", ""}, {"!ABC!", ""}});
	CheckAllPermutations("", {"AC~"}, "", {{"!ABC!", ""}});
	CheckAllPermutations("", {"B~"}, "", {});
	CheckAllPermutations("", {"AX~"}, "", {});

	cfg.maxTypos = 3;
	cfg.maxTypoDistance = -1;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	// Or one missing char in one word and two missing chars in another one
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

	cfg.maxTypos = 3;
	cfg.maxTypoDistance = kDefaultMaxTypoDist;
	SetFTConfig(cfg);
	// Full match
	// Or up to two missing chars in any word
	// Or one letter switch and one missing char in any word
	// Max typo distance is 0 (by default). Only the letter on the same position may be changed
	// Max letter permutation is 1 (by default). The same letter may be moved by 1
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"XBCDEF~"}, "", {{"!ABCDE!", ""}, {"!ABCDEF!", ""}, {"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});

	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {});
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
	cfg.maxTypoDistance = -1;
	SetFTConfig(cfg);
	// Full match
	// Or up to two missing chars in any of the both words
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

	cfg.maxTypos = 4;
	cfg.maxTypoDistance = kDefaultMaxTypoDist;
	SetFTConfig(cfg);
	// Full match
	// Or one missing char in any word
	// Or one letter switch and one missing char in one of the words
	// Or two letters switch
	// Max typo distance is 0 (by default). Only the letter on the same position may be changed
	// Max letter permutation is 1 (by default). The same letter may be moved by 1
	CheckAllPermutations("", {"ABCD~"}, "", {{"!AB!", ""}, {"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"ABCDEFGH~"}, "", {{"!ABCDEF!", ""}, {"!ABCDEFG!", ""}, {"!ABCDEFGH!", ""}});
	CheckAllPermutations("", {"BCDEFX~"}, "", {{"!ABCDEFG!", ""}});
	CheckAllPermutations("", {"BCDXEFX~"}, "", {});
	CheckAllPermutations("", {"BCD~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"XABCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABXCD~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDX~"}, "", {{"!ABC!", ""}, {"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"XABCDX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}});
	CheckAllPermutations("", {"ABXXCD~"}, "", {{"!ABCD!", ""}});
	CheckAllPermutations("", {"ABCDXX~"}, "", {{"!ABCD!", ""}, {"!ABCDE!", ""}, {"!ABCDEF!", ""}});
	CheckAllPermutations("", {"BXCD~"}, "", {{"!ABCD!", ""}});
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

TEST_P(FTApi, FTDslParserMatchSymbolTest) {
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

TEST_P(FTApi, FTDslParserMisspellingTest) {
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

TEST_P(FTApi, FTDslParserFieldsPartOfRequest) {
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

TEST_P(FTApi, FTDslParserTermRelevancyBoostTest) {
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

TEST_P(FTApi, FTDslParserWrongRelevancyTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	EXPECT_THROW(ftdsl.parse("+wrong +boost^X"), reindexer::Error);
}

TEST_P(FTApi, FTDslParserDistanceTest) {
	FTDSLQueryParams params;

	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("'long nose'~3");
	EXPECT_TRUE(ftdsl.size() == 2);
	EXPECT_TRUE(ftdsl[0].pattern == L"long");
	EXPECT_TRUE(ftdsl[1].pattern == L"nose");
	EXPECT_TRUE(ftdsl[0].opts.distance == INT_MAX);
	EXPECT_TRUE(ftdsl[1].opts.distance == 3);
}

TEST_P(FTApi, FTDslParserWrongDistanceTest) {
	FTDSLQueryParams params;
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
		EXPECT_THROW(ftdsl.parse("'this is a wrong distance'~X"), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
		EXPECT_THROW(ftdsl.parse("'long nose'~-1"), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
		EXPECT_THROW(ftdsl.parse("'long nose'~0"), reindexer::Error);
	}
	{
		reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
		EXPECT_THROW(ftdsl.parse("'long nose'~2.89"), reindexer::Error);
	}
}

TEST_P(FTApi, FTDslParserNoClosingQuoteTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	EXPECT_THROW(ftdsl.parse("\"forgot to close this quote"), reindexer::Error);
}

TEST_P(FTApi, FTDslParserWrongFieldNameTest) {
	FTDSLQueryParams params;
	params.fields = {{"id", 0}, {"fk_id", 1}, {"location", 2}};
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	EXPECT_THROW(ftdsl.parse("@name,text,desc Thrones"), reindexer::Error);
}

TEST_P(FTApi, FTDslParserBinaryOperatorsTest) {
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

TEST_P(FTApi, FTDslParserEscapingCharacterTest) {
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

TEST_P(FTApi, FTDslParserExactMatchTest) {
	FTDSLQueryParams params;
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("=moskva77");
	EXPECT_TRUE(ftdsl.size() == 1);
	EXPECT_TRUE(ftdsl[0].opts.exact);
	EXPECT_TRUE(ftdsl[0].pattern == L"moskva77");
}

TEST_P(FTApi, NumberToWordsSelect) {
	Init(GetDefaultConfig());
	Add("оценка 5 майкл джордан 23"sv, ""sv);

	CheckAllPermutations("", {"пять", "+двадцать", "+три"}, "", {{"оценка !5! майкл джордан !23!", ""}});
}

// Make sure FT seeks by a huge number set by string in DSL
TEST_P(FTApi, HugeNumberToWordsSelect) {
	// Initialize namespace
	Init(GetDefaultConfig());
	// Add a record with a big number
	Add("много 7343121521906522180408440 денег"sv, ""sv);
	// Execute FT query, where search words are set as strings
	auto qr = SimpleSelect(
		"+семь +септиллионов +триста +сорок +три +секстиллиона +сто +двадцать +один +квинтиллион +пятьсот +двадцать +один +квадриллион "
		"+девятьсот +шесть +триллионов +пятьсот +двадцать +два +миллиарда +сто +восемьдесят +миллионов +четыреста +восемь +тысяч "
		"+четыреста +сорок");
	// Make sure it found this only string
	ASSERT_TRUE(qr.Count() == 1);
}

// Make sure way too huge numbers are ignored in FT
TEST_P(FTApi, HugeNumberToWordsSelect2) {
	// Initialize namespace
	Init(GetDefaultConfig());
	// Add a record with a huge number
	Add("1127343121521906522180408440"sv, ""sv);
	// Execute FT query, where search words are set as strings
	reindexer::QueryResults qr;
	const std::string searchWord =
		"+один +октиллион +сто +двадцать +семь +септиллионов +триста +сорок +три +секстиллиона +сто +двадцать +один +квинтиллион +пятьсот "
		"+двадцать +один +квадриллион +девятьсот +шесть +триллионов +пятьсот +двадцать +два +миллиарда +сто +восемьдесят +миллионов "
		"+четыреста +восемь +тысяч +четыреста +сорок";
	auto q{reindexer::Query("nm1").Where("ft3", CondEq, searchWord)};
	auto err = rt.reindexer->Select(q, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	// Make sure it has found absolutely nothing
	ASSERT_TRUE(qr.Count() == 0);
}

TEST_P(FTApi, DeleteTest) {
	Init(GetDefaultConfig());

	std::unordered_map<std::string, int> data;
	for (int i = 0; i < 10000; ++i) {
		data.insert(Add(rt.RuRandString()));
	}
	auto res = SimpleSelect("entity");
	for (int i = 0; i < 10000; ++i) {
		data.insert(Add(rt.RuRandString()));
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

TEST_P(FTApi, RebuildAfterDeletion) {
	Init(GetDefaultConfig());

	auto cfg = GetDefaultConfig();
	cfg.maxStepSize = 5;
	auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
	ASSERT_TRUE(err.ok()) << err.what();

	auto selectF = [this](const std::string& word) {
		const auto q{reindexer::Query("nm1").Where("ft1", CondEq, word)};
		reindexer::QueryResults res;
		auto err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		return res;
	};

	std::unordered_map<std::string, int> data;
	data.insert(Add("An entity is something that exists as itself"sv));
	data.insert(Add("In law, a legal entity is an entity that is capable of bearing legal rights"sv));
	data.insert(Add("In politics, entity is used as term for territorial divisions of some countries"sv));
	data.insert(Add("Юридическое лицо — организация, которая имеет обособленное имущество"sv));
	data.insert(Add("Aftermath - the consequences or aftereffects of a significant unpleasant event"sv));
	data.insert(Add("Food prices soared in the aftermath of the drought"sv));
	data.insert(Add("In the aftermath of the war ..."sv));

	auto res = selectF("entity");
	ASSERT_EQ(res.Count(), 3);

	Delete(data.find("In law, a legal entity is an entity that is capable of bearing legal rights")->second);
	res = selectF("entity");
	ASSERT_EQ(res.Count(), 2);
}

TEST_P(FTApi, Stress) {
	Init(GetDefaultConfig());

	std::vector<std::string> data;
	std::vector<std::string> phrase;

	data.reserve(100000);
	for (size_t i = 0; i < 100000; ++i) {
		data.push_back(rt.RandString());
	}

	phrase.reserve(7000);
	for (size_t i = 0; i < 7000; ++i) {
		phrase.push_back(data[rand() % data.size()] + "  " + data[rand() % data.size()] + " " + data[rand() % data.size()]);
	}

	std::atomic<bool> terminate = false;
	std::thread statsThread([&] {
		while (!terminate) {
			reindexer::QueryResults qr;
			rt.reindexer->Select(reindexer::Query("#memstats"), qr);
			std::this_thread::sleep_for(std::chrono::milliseconds(10));
		}
	});

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
					auto ritem(it.GetItem(false));
					if (ritem["ft1"].As<std::string>() == phrase[j]) {
						found = true;
					}
				}
				if (!found) {
					abort();
				}
			}
		}
	}
	terminate = true;
	statsThread.join();
}

TEST_P(FTApi, Unique) {
	Init(GetDefaultConfig());

	std::vector<std::string> data;
	std::set<size_t> check;
	std::set<std::string> checks;
	reindexer::logInstallWriter([](int, char*) { /*std::cout << buf << std::endl;*/ });

	for (int i = 0; i < 1000; ++i) {
		bool inserted = false;
		size_t n;
		std::string s;

		while (!inserted) {
			n = rand();
			auto res = check.insert(n);
			inserted = res.second;
		}

		inserted = false;

		while (!inserted) {
			s = rt.RandString();
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
					int a = 3;	// NOLINT(*unused-but-set-variable) This code is just to load CPU by non-rx stuff
					a++;
					(void)a;
				}
				auto res = StressSelect(data[j]);
				if (res.Count() != 1) {
					for (auto it : res) {
						auto ritem(it.GetItem(false));
					}
					abort();
				}
			}
		}
	}
}

TEST_P(FTApi, SummationOfRanksInSeveralFields) {
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
	auto err = SetFTConfig(ftCfg, "nm3", "ft", {"ft1", "ft2", "ft3"});
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

TEST_P(FTApi, SelectSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"лыв", "лав"}, {"love"}}, {{"лар", "hate"}, {"rex", "looove"}}};
	Init(ftCfg);

	Add("nm1"sv, "test"sv, "love rex"sv);
	Add("nm1"sv, "test"sv, "no looove"sv);
	Add("nm1"sv, "test"sv, "no match"sv);

	CheckAllPermutations("", {"лыв"}, "", {{"test", "!love! rex"}});
	CheckAllPermutations("", {"hate"}, "", {{"test", "love !rex!"}, {"test", "no !looove!"}});
}

TEST_P(FTApi, SelectTranslitWithComma) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.logLevel = 5;
	Init(ftCfg);

	Add("nm1"sv, "хлебопечка"sv, ""sv);
	Add("nm1"sv, "электрон"sv, ""sv);
	Add("nm1"sv, "матэ"sv, ""sv);

	auto qr = SimpleSelect("@ft1 [kt,jgtxrf");
	EXPECT_EQ(qr.Count(), 1);
	auto item = qr[0].GetItem(false);
	EXPECT_EQ(item["ft1"].As<std::string>(), "!хлебопечка!");

	qr = SimpleSelect("@ft1 \\'ktrnhjy");
	EXPECT_EQ(qr.Count(), 1);
	item = qr[0].GetItem(false);
	EXPECT_EQ(item["ft1"].As<std::string>(), "!электрон!");

	qr = SimpleSelect("@ft1 vfn\\'");
	EXPECT_EQ(qr.Count(), 1);
	item = qr[0].GetItem(false);
	EXPECT_EQ(item["ft1"].As<std::string>(), "!матэ!");
}

TEST_P(FTApi, SelectMultiwordSynonyms) {
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
TEST_P(FTApi, SelectMultiwordSynonyms2) {
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

TEST_P(FTApi, SelectWithMinusWithSynonyms) {
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
TEST_P(FTApi, SelectMultiwordSynonymsWithExtraWords) {
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

TEST_P(FTApi, ChangeSynonymsCfg) {
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

TEST_P(FTApi, SelectWithRelevanceBoostWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}, {{"United Nations"}, {"ООН"}}};
	Init(ftCfg);

	Add("nm1"sv, "одно слово"sv, ""sv);
	Add("nm1"sv, ""sv, "ООН"sv);

	CheckAllPermutations("", {"word^2", "United^0.5", "Nations"}, "", {{"!одно слово!", ""}, {"", "!ООН!"}}, true);
	CheckAllPermutations("", {"word^0.5", "United^2", "Nations^0.5"}, "", {{"", "!ООН!"}, {"!одно слово!", ""}}, true);
}

TEST_P(FTApi, SelectWithFieldsBoostWithSynonyms) {
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

TEST_P(FTApi, SelectWithFieldsListWithSynonyms) {
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

TEST_P(FTApi, RankWithPosition) {
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

TEST_P(FTApi, DifferentFieldRankPosition) {
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

TEST_P(FTApi, PartialMatchRank) {
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

TEST_P(FTApi, SelectFullMatch) {
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

TEST_P(FTApi, SetFtFieldsCfgErrors) {
	auto cfg = GetDefaultConfig(2);
	Init(cfg);
	cfg.fieldsCfg[0].positionWeight = 0.1;
	cfg.fieldsCfg[1].positionWeight = 0.2;
	// Задаем уникальный конфиг для поля ft, которого нет в индексе ft3
	auto err = SetFTConfig(cfg, "nm1", "ft3", {"ft", "ft2"});
	// Получаем ошибку
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Field 'ft' is not included to full text index");

	err = rt.reindexer->OpenNamespace("nm3");
	ASSERT_TRUE(err.ok()) << err.what();
	rt.DefineNamespaceDataset(
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

TEST_P(FTApi, MergeLimitConstraints) {
	auto cfg = GetDefaultConfig();
	Init(cfg);
	cfg.mergeLimit = kMinMergeLimitValue - 1;
	auto err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_EQ(err.code(), errParseJson);
	cfg.mergeLimit = kMaxMergeLimitValue + 1;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_EQ(err.code(), errParseJson);
	cfg.mergeLimit = kMinMergeLimitValue;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
	cfg.mergeLimit = kMaxMergeLimitValue;
	err = SetFTConfig(cfg, "nm1", "ft3", {"ft1", "ft2"});
	ASSERT_TRUE(err.ok()) << err.what();
}

TEST_P(FTApi, SnippetN) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);
	Add("one two three gg three empty empty empty empty three"sv);

	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,'{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Incorrect count of position arguments. Found 5 required 4.");
	}
	{  // check other case, error on not last argument
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,'{','{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token ','.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',pre_delim='}')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Argument already added 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',pre_delim='}',post_delim='!')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Argument already added 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Incorrect count of position arguments. Found 3 required 4.");
	}
	{  // check other case, error on not last argument
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>' , '</b>',5,pre_delim='{',post_delim='')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token ',', expecting positional argument (1 more positional args required)");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',pre_delim='}') g");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected character `g` after closing parenthesis.");
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token ',', expecting positional argument (2 more positional args required)");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token ','.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{',,post_delim='}')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token ','.");
	}

	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>''n','</b>',5,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token 'n'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>'n,'</b>',5,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token 'n'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>'5,5,5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token '5'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5'v',5,pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token 'v'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,\"pre_delim\"pre_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim= ='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token '='.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim='{'8)");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token '8'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim=)");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token 'pre_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,not_delim='{')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unknown argument name 'not_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,not_delim='{',pre_delim='}')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unknown argument name 'not_delim'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: The closing parenthesis is required, but found `5`");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n{('<b>','</b>',5,5}");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: An open parenthesis is required, but found `{`");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<b>','</b>',5,5,"post_delim"="v"})#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token 'v'.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n(<>,'</b>',5,5,"post_delim"='v'})#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Unexpected token '<>'");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<>','</b>',5,5,='v'})#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "snippet_n: Argument name is empty.");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<>','</b>','5a',5))#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Invalid snippet param before - 5a is not a number");
	}
	{
		reindexer::Query q("nm1");
		q.Where("ft1", CondEq, "three").AddFunction(R"#(ft1=snippet_n('<>','</b>',5,'5b'))#");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_FALSE(err.ok());
		EXPECT_EQ(err.what(), "Invalid snippet param after - 5b is not a number");
	}

	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction("ft1=snippet_n('<b>','</b>',5,5,pre_delim=',')");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		reindexer::WrSerializer wrSer;
		err = res.begin().GetJSON(wrSer, false);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(std::string(wrSer.Slice()), R"S({"ft1":", two <b>three</b> gg <b>three</b> empt ,mpty <b>three</b> "})S");
	}

	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>' , 		'</b>'
																											,5	,5 ,       pre_delim=','))S");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		reindexer::WrSerializer wrSer;
		err = res.begin().GetJSON(wrSer, false);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(std::string(wrSer.Slice()), R"S({"ft1":", two <b>three</b> gg <b>three</b> empt ,mpty <b>three</b> "})S");
	}

	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>','</b>',5,5,pre_delim=' g ', post_delim='h'))S");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":" g  two <b>three</b> gg <b>three</b> empth g mpty <b>three</b>h"})S");
		}
	}
	{
		reindexer::Query q("nm1");
		q.Select({"ft1"})
			.Where("ft1", CondEq, "three")
			.AddFunction(R"S(ft1=snippet_n('<b>','</b>','5',5,post_delim='h',pre_delim=' g '))S");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":" g  two <b>three</b> gg <b>three</b> empth g mpty <b>three</b>h"})S");
		}
	}
	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>','</b>',5,5,post_delim='h'))S");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":" two <b>three</b> gg <b>three</b> empthmpty <b>three</b>h"})S");
		}
	}
	{
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, "three").AddFunction(R"S(ft1=snippet_n('<b>','</b>',5,5,pre_delim='!'))S");
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), R"S({"ft1":"! two <b>three</b> gg <b>three</b> empt !mpty <b>three</b> "})S");
		}
	}
}

TEST_P(FTApi, SnippetNOthers) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	std::string_view s1 = "123456 one 789012"sv;
	[[maybe_unused]] auto [ss1, id1] = Add(s1);

	std::string_view s2 = "123456 one 789 one 987654321"sv;
	[[maybe_unused]] auto [ss2, id2] = Add(s2);

	std::string_view s3 = "123456 one two 789 one two 987654321"sv;
	[[maybe_unused]] auto [ss3, id3] = Add(s3);

	std::string_view s4 = "123456 one один два two 789 one один два two 987654321"sv;
	[[maybe_unused]] auto [ss4, id4] = Add(s4);

	auto check = [&](int index, const std::string& find, const std::string& fun, std::string_view answer) {
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, find).Where("id", CondEq, index).AddFunction(fun);
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), answer);
		}
	};
	check(id1, "one", R"S(ft1=snippet_n('<','>',5,5,pre_delim='[',post_delim=']',with_area=0))S", R"S({"ft1":"[3456 <one> 7890]"})S");
	check(id1, "one", R"S(ft1=snippet_n('<','>',5,5,pre_delim='[',post_delim=']'))S", R"S({"ft1":"[3456 <one> 7890]"})S");
	check(id2, "one", R"S(ft1=snippet_n('<','>',5,5,pre_delim='[',post_delim=']'))S", R"S({"ft1":"[3456 <one> 789 <one> 9876]"})S");
	check(id3, R"S("one two")S", R"S(ft1=snippet_n('<','>',2,2,pre_delim='[',post_delim=']'))S",
		  R"S({"ft1":"[6 <one two> 7][9 <one two> 9]"})S");
	check(id3, R"S("one two")S", R"S(ft1=snippet_n('<','>',2,2,pre_delim='[',post_delim=']',with_area=1))S",
		  R"S({"ft1":"[[5,16]6 <one two> 7][[17,28]9 <one two> 9]"})S");
	check(id4, R"S("one один два two")S", R"S(ft1=snippet_n('<','>',2,2,pre_delim='[',post_delim=']'))S",
		  R"S({"ft1":"[6 <one один два two> 7][9 <one один два two> 9]"})S");
	check(id4, R"S("one один два two")S", R"S(ft1=snippet_n('<','>',2,2,with_area=1,pre_delim='[',post_delim=']'))S",
		  R"S({"ft1":"[[5,25]6 <one один два two> 7][[26,46]9 <one один два two> 9]"})S");
}

TEST_P(FTApi, SnippetNOffset) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	std::string_view s1 = "one"sv;
	[[maybe_unused]] auto [ss1, id1] = Add(s1);

	std::string_view s2 = "один"sv;
	[[maybe_unused]] auto [ss2, id2] = Add(s2);

	std::string_view s3 = "asd one ghj"sv;
	[[maybe_unused]] auto [ss3, id3] = Add(s3);

	std::string_view s4 = "лмн один опр"sv;
	[[maybe_unused]] auto [ss4, id4] = Add(s4);

	std::string_view s5 = "лмн один опр один лмк"sv;
	[[maybe_unused]] auto [ss5, id5] = Add(s5);

	std::string_view s6 = "лмн опр jkl один"sv;
	[[maybe_unused]] auto [ss6, id6] = Add(s6);

	auto check = [&](int index, const std::string& find, const std::string& fun, std::string_view answer) {
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, find).Where("id", CondEq, index).AddFunction(fun);
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), answer);
		}
	};
	check(id1, "one", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[0,3]one "})S");
	check(id1, "one", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,3]one "})S");
	check(id2, "один", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[0,4]один "})S");
	check(id2, "один", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,4]один "})S");

	check(id3, "one", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[4,7]one "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',1,1,with_area=1))S", R"S({"ft1":"[3,8] one  "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',4,4,with_area=1))S", R"S({"ft1":"[0,11]asd one ghj "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,11]asd one ghj "})S");

	check(id6, "один", R"S(ft1=snippet_n('','',2,0,with_area=1))S", R"S({"ft1":"[10,16]l один "})S");
	check(id6, "один", R"S(ft1=snippet_n('','',2,2,with_area=1))S", R"S({"ft1":"[10,16]l один "})S");

	check(id4, "один", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[4,8]один "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',1,1,with_area=1))S", R"S({"ft1":"[3,9] один  "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',2,2,with_area=1))S", R"S({"ft1":"[2,10]н один о "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',4,4,with_area=1))S", R"S({"ft1":"[0,12]лмн один опр "})S");
	check(id4, "один", R"S(ft1=snippet_n('','',5,5,with_area=1))S", R"S({"ft1":"[0,12]лмн один опр "})S");

	check(id5, "один", R"S(ft1=snippet_n('','',0,0,with_area=1))S", R"S({"ft1":"[4,8]один [13,17]один "})S");
	check(id5, "один", R"S(ft1=snippet_n('','',2,2,with_area=1))S", R"S({"ft1":"[2,10]н один о [11,19]р один л "})S");

	check(id5, "один", R"S(ft1=snippet_n('','',3,3,with_area=1))S", R"S({"ft1":"[1,20]мн один опр один лм "})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',post_delim='))',with_area=1))S",
		  R"S({"ft1":"(([2,10]н {!один} о))(([11,19]р {!один} л))"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',3,3,pre_delim='(',post_delim=')',with_area=1))S",
		  R"S({"ft1":"([1,20]мн {!один} опр {!один} лм)"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',post_delim='))',with_area=0))S",
		  R"S({"ft1":"((н {!один} о))((р {!один} л))"})S");

	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',with_area=1))S",
		  R"S({"ft1":"(([2,10]н {!один} о (([11,19]р {!один} л "})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',3,3,pre_delim='(',with_area=1))S", R"S({"ft1":"([1,20]мн {!один} опр {!один} лм "})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,pre_delim='((',with_area=0))S", R"S({"ft1":"((н {!один} о ((р {!один} л "})S");

	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,post_delim='))',with_area=1))S",
		  R"S({"ft1":"[2,10]н {!один} о))[11,19]р {!один} л))"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',3,3,post_delim=')',with_area=1))S", R"S({"ft1":"[1,20]мн {!один} опр {!один} лм)"})S");
	check(id5, "один", R"S(ft1=snippet_n('{!','}',2,2,post_delim='))',with_area=0))S", R"S({"ft1":"н {!один} о))р {!один} л))"})S");
}

TEST_P(FTApi, SnippetNBounds) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	std::string_view s1 = "one"sv;
	[[maybe_unused]] auto [ss1, id1] = Add(s1);

	std::string_view s3 = "as|d one g!hj"sv;
	[[maybe_unused]] auto [ss3, id3] = Add(s3);

	auto check = [&](int index, const std::string& find, const std::string& fun, std::string_view answer) {
		reindexer::Query q("nm1");
		q.Select({"ft1"}).Where("ft1", CondEq, find).Where("id", CondEq, index).AddFunction(fun);
		reindexer::QueryResults res;
		reindexer::Error err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(res.Count(), 1);
		if (res.Count()) {
			reindexer::WrSerializer wrSer;
			err = res.begin().GetJSON(wrSer, false);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_EQ(wrSer.Slice(), answer);
		}
	};
	check(id1, "one", R"S(ft1=snippet_n('','',0,0,with_area=1,left_bound='|',right_bound='|'))S", R"S({"ft1":"[0,3]one "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1,left_bound='|',right_bound='!'))S", R"S({"ft1":"[3,10]d one g "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',1,1,with_area=1,left_bound='|',right_bound='!'))S", R"S({"ft1":"[4,9] one  "})S");

	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1,right_bound='!'))S", R"S({"ft1":"[0,10]as|d one g "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',6,5,with_area=1,right_bound='!'))S", R"S({"ft1":"[0,10]as|d one g "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',4,5,with_area=1,right_bound='!'))S", R"S({"ft1":"[1,10]s|d one g "})S");

	check(id3, "one", R"S(ft1=snippet_n('','',2,5,with_area=1,left_bound='|'))S", R"S({"ft1":"[3,13]d one g!hj "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',2,6,with_area=1,left_bound='!'))S", R"S({"ft1":"[3,13]d one g!hj "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',2,4,with_area=1,left_bound='!'))S", R"S({"ft1":"[3,12]d one g!h "})S");
	check(id3, "one", R"S(ft1=snippet_n('','',5,5,with_area=1,left_bound='!',right_bound='|'))S", R"S({"ft1":"[0,13]as|d one g!hj "})S");
}

TEST_P(FTApi, LargeMergeLimit) {
	// Check if results are bounded by merge limit
	auto ftCfg = GetDefaultConfig();
	ftCfg.mergeLimit = kMaxMergeLimitValue;
	Init(ftCfg);
	const std::string kBase1 = "aaaa";
	const std::string kBase2 = "bbbb";

	reindexer::fast_hash_set<std::string> strings1;
	constexpr unsigned kPartLen = (kMaxMergeLimitValue + 15000) / 2;
	for (unsigned i = 0; i < kPartLen; ++i) {
		while (true) {
			std::string val = kBase2 + rt.RandString(10, 10);
			if (strings1.emplace(val).second) {
				Add("nm1"sv, val);
				break;
			}
		}
	}
	reindexer::fast_hash_set<std::string> strings2;
	auto fit = strings1.begin();
	for (unsigned i = 0; i < kPartLen; ++i, ++fit) {
		while (true) {
			std::string val = kBase2 + rt.RandString(10, 10);
			if (strings2.emplace(val).second) {
				if (fit == strings2.end()) {
					fit = strings2.begin();
				}
				Add("nm1"sv, val, fit.key());
				break;
			}
		}
	}

	auto qr = SimpleSelect(fmt::sprintf("%s* %s*", kBase1, kBase2));
	ASSERT_EQ(qr.Count(), ftCfg.mergeLimit);
}

TEST_P(FTApi, TyposDistance) {
	// Check different max_typos_distance values with default max_typos and default max_symbol_permutation_distance (=1)
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("блачныйк"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct Case {
		std::string description;
		int maxTypoDistance;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"wrong_letter_default_config", std::numeric_limits<int>::max(), "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_default_config", std::numeric_limits<int>::max(), "=облочный~", {"облачный"}},
		{"extra_letter_default_config", std::numeric_limits<int>::max(), "=облачкный~", {"облачный"}},
		{"missing_letter_default_config", std::numeric_limits<int>::max(), "=обланый~", {"облачный"}},

		{"wrong_letter_0_typo_distance", 0, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_0_typo_distance", 0, "=облочный~", {"облачный"}},
		{"extra_letter_0_typo_distance", 0, "=облачкный~", {"облачный"}},
		{"missing_letter_0_typo_distance", 0, "=обланый~", {"облачный"}},

		{"wrong_letter_any_typo_distance", -1, "=аблачный~", {"облачный", "табачный", "блачныйк"}},
		{"wrong_letter_in_the_middle_any_typo_distance", -1, "=облочный~", {"облачный"}},
		{"extra_letter_any_typo_distance", -1, "=облачкный~", {"облачный"}},
		{"missing_letter_any_typo_distance", -1, "=обланый~", {"облачный"}},

		{"wrong_letter_2_typo_distance", 2, "=аблачный~", {"облачный", "табачный"}},
		{"wrong_letter_in_the_middle_2_typo_distance", -1, "=облочный~", {"облачный"}},
		{"extra_letter_2_typo_distance", 2, "=облачкный~", {"облачный"}},
		{"missing_letter_2_typo_distance", 2, "=обланый~", {"облачный"}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		if (c.maxTypoDistance != std::numeric_limits<int>::max()) {
			cfg.maxTypoDistance = c.maxTypoDistance;
		}
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		reindexer::QueryResults res;
		err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTApi, TyposDistanceWithMaxTypos) {
	// Check basic max_typos_distance funtionality with different max_typos values.
	// Letters permutations are not allowed (max_symbol_permutation_distance = 0)
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("блачныйк"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct Case {
		std::string description;
		int maxTypos;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"full_match_0_max_typo", 0, "=облачный~", {"облачный"}},
		{"wrong_letter_0_max_typo", 0, "=аблачный~", {}},
		{"wrong_letter_in_the_middle_0_max_typo", 0, "=облочный~", {}},
		{"2_wrong_letters_0_max_typo_1", 0, "=аблочный~", {}},
		{"2_wrong_letters_0_max_typo_2", 0, "=оплачнык~", {}},
		{"extra_letter_0_max_typo", 0, "=облачкный~", {}},
		{"missing_letter_0_max_typo", 0, "=обланый~", {}},
		{"2_extra_letters_0_max_typo", 0, "=поблачкный~", {}},
		{"2_missing_letters_0_max_typo", 0, "=обланы~", {}},

		{"full_match_1_max_typo", 1, "=облачный~", {"облачный"}},
		{"wrong_letter_1_max_typo", 1, "=аблачный~", {}},
		{"wrong_letter_in_the_middle_1_max_typo", 1, "=облочный~", {}},
		{"2_wrong_letters_1_max_typo_1", 1, "=аблочный~", {}},
		{"2_wrong_letters_1_max_typo_2", 1, "=оплачнык~", {}},
		{"extra_letter_1_max_typo", 1, "=облачкный~", {"облачный"}},
		{"missing_letter_1_max_typo", 1, "=обланый~", {"облачный"}},
		{"2_extra_letters_1_max_typo", 1, "=поблачкный~", {}},
		{"2_missing_letters_1_max_typo", 1, "=обланы~", {}},

		{"full_match_2_max_typo", 2, "=облачный~", {"облачный"}},
		{"wrong_letter_2_max_typo", 2, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_2_max_typo", 2, "=облочный~", {"облачный"}},
		{"2_wrong_letters_2_max_typo_1", 2, "=аблочный~", {}},
		{"2_wrong_letters_2_max_typo_2", 2, "=оплачнык~", {}},
		{"extra_letter_2_max_typo", 2, "=облачкный~", {"облачный"}},
		{"missing_letter_2_max_typo", 2, "=обланый~", {"облачный"}},
		{"2_extra_letters_2_max_typo", 2, "=поблачкный~", {}},
		{"2_missing_letters_2_max_typo", 2, "=обланы~", {}},

		{"full_match_3_max_typo", 3, "=облачный~", {"облачный"}},
		{"wrong_letter_3_max_typo", 3, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_3_max_typo", 3, "=облочный~", {"облачный"}},
		{"2_wrong_letters_3_max_typo_1", 3, "=аблочный~", {}},
		{"2_wrong_letters_3_max_typo_2", 3, "=оплачнык~", {}},
		{"extra_letter_3_max_typo", 3, "=облачкный~", {"облачный"}},
		{"missing_letter_3_max_typo", 3, "=обланый~", {"облачный"}},
		{"2_extra_letters_3_max_typo", 3, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_3_max_typo", 3, "=обланы~", {"облачный"}},
		{"3_extra_letters_3_max_typo", 3, "=поблачкныйк~", {}},
		{"1_wrong_1_extra_letter_3_max_typo", 3, "=облочкный~", {"облачный"}},
		{"1_wrong_1_missing_letter_3_max_typo", 3, "=облоный~", {"облачный"}},
		{"1_letter_permutation_3_max_typo", 3, "=болачный~", {}},
		{"2_letters_permutation_3_max_typo", 3, "=болачынй~", {}},

		{"full_match_4_max_typo", 4, "=облачный~", {"облачный", "отличный"}},
		{"wrong_letter_4_max_typo", 4, "=аблачный~", {"облачный"}},
		{"wrong_letter_in_the_middle_4_max_typo", 4, "=облочный~", {"облачный", "отличный"}},
		{"2_wrong_letters_4_max_typo_1", 4, "=аблочный~", {"облачный"}},
		{"2_wrong_letters_4_max_typo_2", 4, "=оплачнык~", {"облачный"}},
		{"extra_letter_4_max_typo", 4, "=облачкный~", {"облачный"}},
		{"missing_letter_4_max_typo", 4, "=обланый~", {"облачный"}},
		{"2_extra_letters_4_max_typo", 4, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_4_max_typo", 4, "=обланы~", {"облачный"}},
		{"3_extra_letters_4_max_typo", 4, "=поблачкныйк~", {}},
		{"3_missing_letters_4_max_typo", 4, "=обаны~", {}},
		{"1_wrong_1_extra_letter_4_max_typo", 4, "=облочкный~", {"облачный"}},
		{"1_wrong_1_missing_letter_4_max_typo", 4, "=облоный~", {"облачный"}},
		{"1_letter_permutation_4_max_typo", 4, "=болачный~", {"облачный"}},
		{"2_letters_permutation_4_max_typo", 4, "=болачынй~", {}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		EXPECT_EQ(cfg.maxTypoDistance, 0) << "This test expects default max_typo_distance == 0";
		cfg.maxSymbolPermutationDistance = 0;
		cfg.maxTypos = c.maxTypos;
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		reindexer::QueryResults res;
		err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTApi, LettersPermutationDistance) {
	// Check different max_symbol_permutation_distance values with default max_typos and default max_typos_distance (=0)
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct Case {
		std::string description;
		int maxLettPermDist;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"first_letter_1_move_default_config", std::numeric_limits<int>::max(), "=болачный~", {"облачный"}},
		{"first_letter_1_move_and_switch_default_config", std::numeric_limits<int>::max(), "=волачный~", {}},
		{"first_letter_2_move_default_config", std::numeric_limits<int>::max(), "=блоачный~", {}},
		{"first_letter_3_move_default_config", std::numeric_limits<int>::max(), "=блаочный~", {}},
		{"mid_letter_1_move_default_config", std::numeric_limits<int>::max(), "=обалчный~", {"облачный"}},
		{"mid_letter_1_move_and_switch_default_config", std::numeric_limits<int>::max(), "=обакчный~", {}},
		{"mid_letter_2_move_default_config", std::numeric_limits<int>::max(), "=обачлный~", {}},
		{"mid_letter_3_move_default_config", std::numeric_limits<int>::max(), "=обачнлый~", {}},

		{"first_letter_1_move_0_lett_perm", 0, "=болачный~", {}},
		{"first_letter_2_move_0_lett_perm", 0, "=блоачный~", {}},
		{"first_letter_3_move_0_lett_perm", 0, "=блаочный~", {}},
		{"mid_letter_1_move_0_lett_perm", 0, "=обалчный~", {}},
		{"mid_letter_2_move_0_lett_perm", 0, "=обачлный~", {}},
		{"mid_letter_3_move_0_lett_perm", 0, "=обачнлый~", {}},

		{"first_letter_1_move_1_lett_perm", 1, "=болачный~", {"облачный"}},
		{"first_letter_2_move_1_lett_perm", 1, "=блоачный~", {}},
		{"first_letter_3_move_1_lett_perm", 1, "=блаочный~", {}},
		{"mid_letter_1_move_1_lett_perm", 1, "=обалчный~", {"облачный"}},
		{"mid_letter_2_move_1_lett_perm", 1, "=обачлный~", {}},
		{"mid_letter_3_move_1_lett_perm", 1, "=обачнлый~", {}},

		{"first_letter_1_move_2_lett_perm", 2, "=болачный~", {"облачный"}},
		{"first_letter_1_move_and_switch_2_lett_perm", 2, "=бклачный~", {}},
		{"first_letter_2_move_2_lett_perm", 2, "=блоачный~", {"облачный"}},
		{"first_letter_3_move_2_lett_perm", 2, "=блаочный~", {}},
		{"mid_letter_1_move_2_lett_perm", 2, "=обалчный~", {"облачный"}},
		{"mid_letter_1_move_and_switch_2_lett_perm", 2, "=обапчный~", {}},
		{"mid_letter_2_move_2_lett_perm", 2, "=обачлный~", {"облачный"}},
		{"mid_letter_3_move_2_lett_perm", 2, "=обачнлый~", {}},

		{"first_letter_1_move_3_lett_perm", 3, "=болачный~", {"облачный"}},
		{"first_letter_2_move_3_lett_perm", 3, "=блоачный~", {"облачный"}},
		{"first_letter_3_move_3_lett_perm", 3, "=блаочный~", {"облачный"}},
		{"mid_letter_switch_3_lett_perm", 3, "=обалчный~", {"облачный"}},
		{"mid_letter_2_move_3_lett_perm", 3, "=обачлный~", {"облачный"}},
		{"mid_letter_3_move_3_lett_perm", 3, "=обачнлый~", {"облачный"}},

		{"first_letter_1_move_any_lett_perm", -1, "=болачный~", {"облачный"}},
		{"first_letter_1_move_and_switch_any_lett_perm", -1, "=бклачный~", {}},
		{"first_letter_2_move_any_lett_perm", -1, "=блоачный~", {"облачный"}},
		{"first_letter_3_move_any_lett_perm", -1, "=блаочный~", {"облачный"}},
		{"mid_letter_1_move_any_lett_perm", -1, "=обалчный~", {"облачный"}},
		{"mid_letter_1_move_and_switch_any_lett_perm", -1, "=обапчный~", {}},
		{"mid_letter_2_move_any_lett_perm", -1, "=обачлный~", {"облачный"}},
		{"mid_letter_3_move_any_lett_perm", -1, "=обачнлый~", {"облачный"}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		if (c.maxLettPermDist != std::numeric_limits<int>::max()) {
			cfg.maxSymbolPermutationDistance = c.maxLettPermDist;
		}
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		reindexer::QueryResults res;
		err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTApi, LettersPermutationDistanceWithMaxTypos) {
	// Check basic max_symbol_permutation_distance funtionality with different max_typos values.
	// max_typo_distance is 0
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("блачныйк"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct Case {
		std::string description;
		int maxTypos;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"full_match_0_max_typo", 0, "=облачный~", {"облачный"}},
		{"wrong_letter_0_max_typo", 0, "=аблачный~", {}},
		{"extra_letter_0_max_typo", 0, "=облачкный~", {}},
		{"missing_letter_0_max_typo", 0, "=обланый~", {}},
		{"2_extra_letters_0_max_typo", 0, "=поблачкный~", {}},
		{"2_missing_letters_0_max_typo", 0, "=обланы~", {}},
		{"1_letter_permutation_0_max_typo", 0, "=болачный~", {}},
		{"1_letter_permutation_and_switch_0_max_typo", 0, "=долачный~", {}},
		{"1_far_letter_permutation_0_max_typo", 0, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_0_max_typo", 0, "=балчный~", {}},
		{"1_permutation_and_2_missing_letters_0_max_typo", 0, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_0_max_typo", 0, "=болачныйк~", {}},
		{"1_permutation_and_2_extra_letters_0_max_typo", 0, "=болачныйкк~", {}},
		{"2_letters_permutation_0_max_typo", 0, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_0_max_typo", 0, "=болачынйк~", {}},
		{"1_permutation_and_1_wrong_letter_0_max_typo_1", 0, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_0_max_typo_2", 0, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_0_max_typo", 0, "=блоочный~", {}},

		{"full_match_1_max_typo", 1, "=облачный~", {"облачный"}},
		{"wrong_letter_1_max_typo", 1, "=аблачный~", {}},
		{"extra_letter_1_max_typo", 1, "=облачкный~", {"облачный"}},
		{"missing_letter_1_max_typo", 1, "=обланый~", {"облачный"}},
		{"2_extra_letters_1_max_typo", 1, "=поблачкный~", {}},
		{"2_missing_letters_1_max_typo", 1, "=обланы~", {}},
		{"1_letter_permutation_1_max_typo", 1, "=болачный~", {}},
		{"1_letter_permutation_and_switch_1_max_typo", 1, "=долачный~", {}},
		{"1_far_letter_permutation_1_max_typo", 1, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_1_max_typo", 1, "=балчный~", {}},
		{"1_permutation_and_2_missing_letters_1_max_typo", 1, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_1_max_typo", 1, "=болачныйк~", {"блачныйк"}},
		{"1_permutation_and_2_extra_letters_1_max_typo", 1, "=болачныйкк~", {}},
		{"2_letters_permutation_1_max_typo", 1, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_1_max_typo", 1, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_1_max_typo_1", 1, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_1_max_typo_2", 1, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_1_max_typo", 1, "=блоочный~", {}},

		{"full_match_2_max_typo", 2, "=облачный~", {"облачный"}},
		{"wrong_letter_2_max_typo", 2, "=аблачный~", {"облачный"}},
		{"extra_letter_2_max_typo", 2, "=облачкный~", {"облачный"}},
		{"missing_letter_2_max_typo", 2, "=обланый~", {"облачный"}},
		{"2_extra_letters_2_max_typo", 2, "=поблачкный~", {}},
		{"2_missing_letters_2_max_typo", 2, "=обланы~", {}},
		{"1_letter_permutation_2_max_typo", 2, "=болачный~", {"облачный"}},
		{"1_letter_permutation_and_switch_2_max_typo", 2, "=долачный~", {}},
		{"1_far_letter_permutation_2_max_typo", 2, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_2_max_typo", 2, "=балчный~", {}},
		{"1_permutation_and_2_missing_letters_2_max_typo", 2, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_2_max_typo", 2, "=болачныйк~", {"блачныйк"}},
		{"1_permutation_and_2_extra_letters_2_max_typo", 2, "=болачныйкк~", {}},
		{"2_letters_permutation_2_max_typo", 2, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_2_max_typo", 2, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_2_max_typo_1", 2, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_2_max_typo_2", 2, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_2_max_typo", 2, "=блоочный~", {}},

		{"full_match_3_max_typo", 3, "=облачный~", {"облачный"}},
		{"wrong_letter_3_max_typo", 3, "=аблачный~", {"облачный"}},
		{"extra_letter_3_max_typo", 3, "=облачкный~", {"облачный"}},
		{"missing_letter_3_max_typo", 3, "=обланый~", {"облачный"}},
		{"2_extra_letters_3_max_typo", 3, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_3_max_typo", 3, "=обланы~", {"облачный"}},
		{"1_letter_permutation_3_max_typo", 3, "=болачный~", {"облачный"}},
		{"1_letter_permutation_and_switch_3_max_typo", 3, "=долачный~", {}},
		{"1_far_letter_permutation_3_max_typo", 3, "=блоачный~", {}},
		{"1_permutation_and_1_missing_letter_3_max_typo", 3, "=балчный~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_missing_letters_3_max_typo", 3, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_3_max_typo", 3, "=болачныйт~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_extra_letters_3_max_typo", 3, "=болачныйтт~", {}},
		{"2_letters_permutation_3_max_typo", 3, "=болачынй~", {}},
		{"2_permutations_and_1_extra_letter_3_max_typo", 3, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_3_max_typo_1", 3, "=болочный~", {}},
		{"1_permutation_and_1_wrong_letter_3_max_typo_2", 3, "=облончый~", {}},
		{"1_far_permutation_and_1_wrong_letter_3_max_typo", 3, "=блоочный~", {}},

		{"full_match_4_max_typo", 4, "=облачный~", {"облачный", "отличный"}},
		{"wrong_letter_4_max_typo", 4, "=аблачный~", {"облачный"}},
		{"extra_letter_4_max_typo", 4, "=облачкный~", {"облачный"}},
		{"missing_letter_4_max_typo", 4, "=обланый~", {"облачный"}},
		{"2_extra_letters_4_max_typo", 4, "=поблачкный~", {"облачный"}},
		{"2_missing_letters_4_max_typo", 4, "=обланы~", {"облачный"}},
		{"1_letter_permutation_4_max_typo", 4, "=болачный~", {"облачный"}},
		{"1_letter_permutation_and_switch_4_max_typo", 4, "=долачный~", {"облачный"}},
		{"1_far_letter_permutation_4_max_typo_1", 4, "=блоачный~", {"облачный"}},  // Will be handled as double permutation
		{"1_far_letter_permutation_4_max_typo_2", 4, "=блаочный~", {}},
		{"1_permutation_and_1_missing_letter_4_max_typo", 4, "=балчный~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_missing_letters_4_max_typo", 4, "=балчны~", {}},
		{"1_permutation_and_1_extra_letter_4_max_typo", 4, "=болачныйт~", {"облачный", "блачныйк"}},
		{"1_permutation_and_2_extra_letters_4_max_typo", 4, "=болачныйтт~", {}},
		{"2_letters_permutation_4_max_typo", 4, "=болачынй~", {"облачный"}},
		{"2_permutations_and_1_extra_letter_4_max_typo", 4, "=болачынйт~", {}},
		{"1_permutation_and_1_wrong_letter_4_max_typo_1", 4, "=болочный~", {"облачный"}},
		{"1_permutation_and_1_wrong_letter_4_max_typo_2", 4, "=облончый~", {"облачный"}},
		{"1_far_permutation_and_1_wrong_letter_4_max_typo", 4, "=блоочный~", {}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		EXPECT_EQ(cfg.maxSymbolPermutationDistance, 1) << "This test expects default max_symbol_permutation_distance == 1";
		cfg.maxTypoDistance = 0;
		cfg.maxTypos = c.maxTypos;
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		reindexer::QueryResults res;
		err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

TEST_P(FTApi, TyposMissingAndExtraLetters) {
	// Check different max_typos, max_extra_letters and max_missing_letters combinations.
	// max_typos must always override max_extra_letters and max_missing_letters.
	// max_missing_letters and max_extra_letters must restrict corresponding letters' counts
	Init(GetDefaultConfig());
	Add("облачный"sv);
	Add("табачный"sv);
	Add("отличный"sv);
	Add("солнечный"sv);

	struct Case {
		std::string description;
		int maxTypos;
		int maxExtraLetters;
		int maxMissingLetter;
		std::string word;
		std::set<std::string> expectedResults;
	};
	const std::vector<Case> cases = {
		{"full_match_0_max_typos_0_max_extras_0_max_missing", 0, 0, 0, "=облачный~", {"облачный"}},
		{"1_missing_0_max_typos_1_max_extras_1_max_missing", 0, 1, 1, "=облчный~", {}},
		{"1_extra_0_max_typos_1_max_extras_1_max_missing", 0, 1, 1, "=облкачный~", {}},

		{"full_match_1_max_typos_0_max_extras_0_max_missing", 0, 0, 0, "=облачный~", {"облачный"}},
		{"1_missing_1_max_typos_0_max_extras_1_max_missing", 1, 0, 1, "=облчный~", {"облачный"}},
		{"1_missing_1_max_typos_1_max_extras_0_max_missing", 1, 1, 0, "=облчный~", {}},
		{"1_missing_1_max_typos_1_max_extras_1_max_missing", 1, 1, 1, "=облчный~", {"облачный"}},
		{"2_missing_1_max_typos_0_max_extras_2_max_missing", 1, 0, 2, "=облчны~", {}},
		{"1_extra_1_max_typos_0_max_extras_1_max_missing", 1, 0, 1, "=облкачный~", {}},
		{"1_extra_1_max_typos_1_max_extras_0_max_missing", 1, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_1_max_typos_1_max_extras_1_max_missing", 1, 1, 1, "=облкачный~", {"облачный"}},
		{"2_extra_1_max_typos_2_max_extras_0_max_missing", 1, 2, 0, "=облкачныйп~", {}},

		{"full_match_2_max_typos_1_max_extras_1_max_missing", 2, 1, 1, "=облачный~", {"облачный"}},
		{"1_missing_2_max_typos_0_max_extras_1_max_missing", 2, 0, 1, "=облчный~", {"облачный"}},
		{"1_missing_2_max_typos_1_max_extras_0_max_missing", 2, 1, 0, "=облчный~", {}},
		{"1_missing_2_max_typos_1_max_extras_1_max_missing", 2, 1, 1, "=облчный~", {"облачный"}},
		{"2_missing_2_max_typos_0_max_extras_2_max_missing", 2, 0, 2, "=облчны~", {}},
		{"1_missing_2_max_typos_0_max_extras_any_max_missing", 2, 0, -1, "=облчный~", {"облачный"}},
		{"2_missing_2_max_typos_0_max_extras_any_max_missing", 2, 0, -1, "=облчны~", {}},
		{"1_extra_2_max_typos_0_max_extras_1_max_missing", 2, 0, 1, "=облкачный~", {}},
		{"1_extra_2_max_typos_1_max_extras_0_max_missing", 2, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_2_max_typos_1_max_extras_1_max_missing", 2, 1, 1, "=облкачный~", {"облачный"}},
		{"2_extra_2_max_typos_2_max_extras_0_max_missing", 2, 2, 0, "=облкачныйп~", {}},
		{"1_extra_2_max_typos_any_max_extras_0_max_missing", 2, -1, 0, "=облкачный~", {"облачный"}},
		{"2_extra_2_max_typos_any_max_extras_0_max_missing", 2, -1, 0, "=облкачныйп~", {}},

		{"full_match_3_max_typos_2_max_extras_2_max_missing", 3, 2, 2, "=облачный~", {"облачный"}},
		{"1_missing_3_max_typos_0_max_extras_1_max_missing", 3, 0, 1, "=облчный~", {"облачный", "отличный"}},
		{"1_missing_3_max_typos_1_max_extras_0_max_missing", 3, 1, 0, "=облчный~", {}},
		{"1_missing_3_max_typos_1_max_extras_1_max_missing", 3, 1, 1, "=облчный~", {"облачный", "отличный"}},
		{"2_missing_3_max_typos_0_max_extras_1_max_missing", 3, 0, 1, "=облчны~", {}},
		{"2_missing_3_max_typos_0_max_extras_2_max_missing", 3, 0, 2, "=облчны~", {"облачный"}},
		{"2_missing_3_max_typos_0_max_extras_any_max_missing", 3, 0, -1, "=облчны~", {"облачный"}},
		{"3_missing_3_max_typos_0_max_extras_any_max_missing", 3, 0, -1, "=облчн~", {}},
		{"1_extra_3_max_typos_0_max_extras_1_max_missing", 3, 0, 1, "=облкачный~", {}},
		{"1_extra_3_max_typos_1_max_extras_0_max_missing", 3, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_3_max_typos_1_max_extras_1_max_missing", 3, 1, 1, "=облкачный~", {"облачный"}},
		{"2_extra_3_max_typos_1_max_extras_0_max_missing", 3, 1, 0, "=облкачныйп~", {}},
		{"2_extra_3_max_typos_2_max_extras_0_max_missing", 3, 2, 0, "=облкачныйп~", {"облачный"}},
		{"2_extra_3_max_typos_any_max_extras_0_max_missing", 3, -1, 0, "=облкачныйп~", {"облачный"}},
		{"3_extra_3_max_typos_any_max_extras_0_max_missing", 3, -1, 0, "=оболкачныйп~", {}},

		{"full_match_4_max_typos_2_max_extras_2_max_missing", 4, 2, 2, "=облачный~", {"облачный", "отличный"}},
		{"1_missing_4_max_typos_0_max_extras_1_max_missing", 4, 0, 1, "=облчный~", {"облачный", "отличный"}},
		{"1_missing_4_max_typos_1_max_extras_0_max_missing", 4, 1, 0, "=облчный~", {}},
		{"1_missing_4_max_typos_1_max_extras_1_max_missing", 4, 1, 1, "=облчный~", {"облачный", "отличный"}},
		{"2_missing_4_max_typos_0_max_extras_1_max_missing", 4, 0, 1, "=облчны~", {}},
		{"2_missing_4_max_typos_0_max_extras_2_max_missing", 4, 0, 2, "=облчны~", {"облачный"}},
		{"2_missing_4_max_typos_0_max_extras_any_max_missing", 4, 0, -1, "=облчны~", {"облачный"}},
		{"3_missing_4_max_typos_0_max_extras_any_max_missing", 4, 0, -1, "=облчн~", {}},
		{"1_extra_4_max_typos_0_max_extras_1_max_missing", 4, 0, 1, "=облкачный~", {}},
		{"1_extra_4_max_typos_1_max_extras_0_max_missing", 4, 1, 0, "=облкачный~", {"облачный"}},
		{"1_extra_4_max_typos_1_max_extras_1_max_missing", 4, 1, 0, "=облкачный~", {"облачный"}},
		{"2_extra_4_max_typos_1_max_extras_0_max_missing", 4, 1, 0, "=облкачныйп~", {}},
		{"2_extra_4_max_typos_2_max_extras_0_max_missing", 4, 2, 0, "=облкачныйп~", {"облачный"}},
		{"2_extra_4_max_typos_any_max_extras_0_max_missing", 4, -1, 0, "=облкачныйп~", {"облачный"}},
		{"3_extra_4_max_typos_any_max_extras_0_max_missing", 4, -1, 0, "=оболкачныйп~", {}},
	};

	for (auto& c : cases) {
		auto cfg = GetDefaultConfig();
		cfg.maxTypos = c.maxTypos;
		cfg.maxExtraLetters = c.maxExtraLetters;
		cfg.maxMissingLetters = c.maxMissingLetter;
		auto err = SetFTConfig(cfg, "nm1", "ft1", {"ft1"});
		ASSERT_TRUE(err.ok()) << err.what();
		auto q = reindexer::Query("nm1").Where("ft1", CondEq, c.word);
		reindexer::QueryResults res;
		err = rt.reindexer->Select(q, res);
		EXPECT_TRUE(err.ok()) << err.what();
		CheckResultsByField(res, c.expectedResults, "ft1", c.description);
	}
}

INSTANTIATE_TEST_SUITE_P(, FTApi,
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
