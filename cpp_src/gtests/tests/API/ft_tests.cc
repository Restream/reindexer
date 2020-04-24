#include <iostream>
#include <limits>
#include <unordered_map>
#include <unordered_set>
#include "core/ft/ftdsl.h"
#include "debug/allocdebug.h"
#include "ft_api.h"
#include "tools/logger.h"
#include "tools/stringstools.h"

TEST_F(FTApi, CompositeSelect) {
	Init(GetDefaultConfig());
	Add("An entity is something|", "| that in exists entity as itself");
	Add("In law, a legal entity is|", "|an entity that is capable of something bearing legal rights");
	Add("In politics, entity is used as|", "| term for entity territorial divisions of some countries");

	auto res = SimpleCompositeSelect("*entity somethin*");
	std::unordered_set<string> data{"An <b>entity</b> is <b>something</b>|",
									"| that in exists <b>entity</b> as itself",
									"An <b>entity</b> is <b>something</b>|d",
									"| that in exists entity as itself",
									"In law, a legal <b>entity</b> is|",
									"|an <b>entity</b> that is capable of <b>something</b> bearing legal rights",
									"al <b>entity</b> id",
									"|an entity that is capable of something bearing legal rights",
									"In politics, <b>entity</b> is used as|",
									"| term for <b>entity</b> territorial divisions of some countries",
									"s, <b>entity</b> id",
									"| term for entity territorial divisions of some countries"};

	PrintQueryResults("nm1", res);
	for (auto it : res) {
		Item ritem(it.GetItem());
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

TEST_F(FTApi, CompositeSelectWithFields) {
	Init(GetDefaultConfig());
	AddInBothFields("An entity is something|", "| that in exists entity as itself");
	AddInBothFields("In law, a legal entity is|", "|an entity that is capable of something bearing legal rights");
	AddInBothFields("In politics, entity is used as|", "| term for entity territorial divisions of some countries");

	for (const char* field : {"ft1", "ft2"}) {
		auto res = CompositeSelectField(field, "*entity somethin*");
		std::unordered_set<string> data{"An <b>entity</b> is <b>something</b>|",
										"An <b>entity</b> is <b>something</b>|d",
										"| that in exists <b>entity</b> as itself",
										"In law, a legal <b>entity</b> is|",
										"|an <b>entity</b> that is capable of <b>something</b> bearing legal rights",
										"an <b>entity</b> tdof <b>something</b> bd",
										"al <b>entity</b> id",
										"In politics, <b>entity</b> is used as|",
										"| term for <b>entity</b> territorial divisions of some countries",
										"ts <b>entity</b> ad",
										"s, <b>entity</b> id",
										"or <b>entity</b> td"};

		PrintQueryResults("nm1", res);
		for (auto it : res) {
			Item ritem(it.GetItem());
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

TEST_F(FTApi, CompositeRankWithSynonyms) {
	auto cfg = GetDefaultConfig();
	cfg.synonyms = {{{"word"}, {"слово"}}};
	Init(cfg);
	Add("word", "слово");
	Add("world", "world");

	// rank of synonym is higher
	Query qr2 = Query("nm1").Where("ft3", CondEq, "@ft1^0.5, ft2^2 word~").Sort("rank()", true);
	QueryResults res2;
	auto err = rt.reindexer->Select(qr2, res2);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(res2.Count() == 2);

	auto it = res2.begin();
	string val = it.GetItem()["ft1"].As<string>();
	EXPECT_EQ(val, "word");

	++it;
	val = it.GetItem()["ft1"].As<string>();
	EXPECT_EQ(val, "world");
}

TEST_F(FTApi, SelectWithEscaping) {
	reindexer::FtFastConfig ftCfg = GetDefaultConfig();
	ftCfg.extraWordSymbols = "+-\\";
	Init(ftCfg);
	Add("Go to -hell+hell+hell!!");

	auto res = SimpleSelect("\\-hell\\+hell\\+hell");
	EXPECT_TRUE(res.Count() == 1);

	for (auto it : res) {
		Item ritem(it.GetItem());
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == "Go to !-hell+hell+hell!!!");
	}
}

TEST_F(FTApi, SelectWithPlus) {
	Init(GetDefaultConfig());

	Add("added three words");
	Add("added something else");

	auto res = SimpleSelect("+added");
	EXPECT_TRUE(res.Count() == 2);

	const char* results[] = {"!added! something else", "!added! three words"};
	for (size_t i = 0; i < res.Count(); ++i) {
		Item ritem = res[i].GetItem();
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == results[i]);
	}
}

TEST_F(FTApi, SelectWithMinus) {
	Init(GetDefaultConfig());

	Add("including me, excluding you");
	Add("including all of them");

	auto res = SimpleSelect("+including -excluding");
	CheckResults(res, {{"!including! all of them", ""}});

	res = SimpleSelect("including -excluding");
	CheckResults(res, {{"!including! all of them", ""}});

	res = SimpleSelect("-excluding +including");
	CheckResults(res, {{"!including! all of them", ""}});

	res = SimpleSelect("-excluding including");
	CheckResults(res, {{"!including! all of them", ""}});
}

TEST_F(FTApi, SelectWithFieldsList) {
	Init(GetDefaultConfig());

	Add("nm1", "Never watch their games", "Because nothing can be worse than Spartak Moscow");
	Add("nm1", "Spartak Moscow is the worst team right now", "Yes, for sure");

	auto res = SimpleSelect("@ft1 Spartak Moscow");
	EXPECT_TRUE(res.Count() == 1);

	for (size_t i = 0; i < res.Count(); ++i) {
		Item ritem = res[i].GetItem();
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == "!Spartak Moscow! is the worst team right now");
	}
}

TEST_F(FTApi, SelectWithRelevanceBoost) {
	Init(GetDefaultConfig());

	Add("She was a very bad girl");
	Add("All the naughty kids go to hell, not to heaven");
	Add("I've never seen a man as cruel as him");

	auto res = SimpleSelect("@ft1 girl^2 kids cruel^3");
	EXPECT_TRUE(res.Count() == 3);

	const char* results[] = {"I've never seen a man as !cruel! as him", "She was a very bad !girl!",
							 "All the naughty !kids! go to hell, not to heaven"};
	for (size_t i = 0; i < res.Count(); ++i) {
		Item ritem = res[i].GetItem();
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == results[i]);
	}
}

TEST_F(FTApi, SelectWithDistance) {
	Init(GetDefaultConfig());

	Add("Her nose was very very long");
	Add("Her nose was exceptionally long");
	Add("Her nose was long");

	auto res = SimpleSelect("'nose long'~3");
	const char* results[] = {"Her !nose! was !long!", "Her !nose! was exceptionally !long!"};
	EXPECT_TRUE(res.Count() == 2) << res.Count();

	for (size_t i = 0; i < res.Count(); ++i) {
		Item ritem = res[i].GetItem();
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == results[i]);
	}

	auto res2 = SimpleSelect("'nose long'~2");
	EXPECT_TRUE(res2.Count() == 1) << res.Count();

	for (size_t i = 0; i < res2.Count(); ++i) {
		Item ritem = res2[i].GetItem();
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == "Her !nose! was !long!");
	}
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

TEST_F(FTApi, FTDslParserRelevancyBoostTest) {
	FTDSLQueryParams params;
	params.fields = {{"name", 0}, {"title", 1}};
	reindexer::FtDSLQuery ftdsl(params.fields, params.stopWords, params.extraWordSymbols);
	ftdsl.parse("@name^1.5,title^0.5 rush");
	EXPECT_TRUE(ftdsl.size() == 1);
	EXPECT_TRUE(ftdsl[0].pattern == L"rush");
	EXPECT_TRUE(AreFloatingValuesEqual(ftdsl[0].opts.fieldsBoost[0], 1.5f));
}

TEST_F(FTApi, FTDslParserRelevancyBoostTest2) {
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
	Add("оценка 5 майкл джордан 23", "");

	auto res = SimpleSelect("пять +двадцать +три");
	EXPECT_TRUE(res.Count() == 1);

	const string result = "оценка !5! майкл джордан !23!";

	for (auto it : res) {
		Item ritem(it.GetItem());
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(result == val);
	}
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

	data.insert(Add("An entity is something that exists as itself"));
	data.insert(Add("In law, a legal entity is an entity that is capable of bearing legal rights"));
	data.insert(Add("In politics, entity is used as term for territorial divisions of some countries"));
	data.insert(Add("Юридическое лицо — организация, которая имеет обособленное имущество"));
	data.insert(Add("Aftermath - the consequences or aftereffects of a significant unpleasant event"));
	data.insert(Add("Food prices soared in the aftermath of the drought"));
	data.insert(Add("In the aftermath of the war ..."));

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
					Item ritem(it.GetItem());
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
						Item ritem(it.GetItem());
					}
					abort();
				}
			}
		}
	}
}

TEST_F(FTApi, SelectSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"лыв", "лав"}, {"love"}}, {{"лар", "hate"}, {"rex", "looove"}}};
	Init(ftCfg);

	Add("nm1", "test", "love rex");
	Add("nm1", "test", "no looove");
	Add("nm1", "test", "no match");

	auto qr = SimpleSelect("лыв");
	EXPECT_EQ(qr.Count(), 1);
	auto item = qr[0].GetItem();
	EXPECT_EQ(item["ft2"].As<string>(), "!love! rex");
	qr = SimpleSelect("hate");
	EXPECT_EQ(qr.Count(), 2);
	std::vector<std::string> res{qr[0].GetItem()["ft2"].As<string>(), qr[1].GetItem()["ft2"].As<string>()};
	std::sort(res.begin(), res.end());
	EXPECT_EQ(res[0], "love !rex!");
	EXPECT_EQ(res[1], "no !looove!");
}

TEST_F(FTApi, SelectMultiwordSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"whole world", "UN", "United Nations"},
					   {"UN", "ООН", "целый мир", "планета", "генеральная ассамблея организации объединенных наций"}},
					  {{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1", "whole world", "test");
	Add("nm1", "world whole", "test");
	Add("nm1", "whole", "world");
	Add("nm1", "world", "test");
	Add("nm1", "whole", "test");
	Add("nm1", "целый мир", "test");
	Add("nm1", "целый", "мир");
	Add("nm1", "целый", "test");
	Add("nm1", "мир", "test");
	Add("nm1", "планета", "test");
	Add("nm1", "генеральная ассамблея организации объединенных наций", "test");
	Add("nm1", "ассамблея генеральная наций объединенных организации", "test");
	Add("nm1", "генеральная прегенеральная ассамблея", "организации объединенных свободных наций");
	Add("nm1", "генеральная прегенеральная ", "организации объединенных свободных наций");
	Add("nm1", "UN", "UN");

	Add("nm1", "word", "test");
	Add("nm1", "test", "word");
	Add("nm1", "word", "слово");
	Add("nm1", "word", "одно");
	Add("nm1", "слово", "test");
	Add("nm1", "слово всего лишь одно", "test");
	Add("nm1", "одно", "test");
	Add("nm1", "слово", "одно");
	Add("nm1", "слово одно", "test");
	Add("nm1", "одно слово", "word");

	auto qr = SimpleSelect("world");
	CheckResults(qr, {{"whole !world!", "test"}, {"!world! whole", "test"}, {"whole", "!world!"}, {"!world!", "test"}});

	qr = SimpleSelect("whole world");
	CheckResults(qr, {{"!whole world!", "test"},
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

	qr = SimpleSelect("UN");
	CheckResults(qr, {{"!целый мир!", "test"},
					  {"!целый!", "!мир!"},
					  {"!планета!", "test"},
					  {"!генеральная ассамблея организации объединенных наций!", "test"},
					  {"!ассамблея генеральная наций объединенных организации!", "test"},
					  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
					  {"!UN!", "!UN!"}});

	qr = SimpleSelect("United Nations");
	CheckResults(qr, {{"!целый мир!", "test"},
					  {"!целый!", "!мир!"},
					  {"!планета!", "test"},
					  {"!генеральная ассамблея организации объединенных наций!", "test"},
					  {"!ассамблея генеральная наций объединенных организации!", "test"},
					  {"!генеральная! прегенеральная !ассамблея!", "!организации объединенных! свободных !наций!"},
					  {"!UN!", "!UN!"}});

	qr = SimpleSelect("целый мир");
	CheckResults(qr, {{"!целый мир!", "test"}, {"!целый!", "test"}, {"!мир!", "test"}, {"!целый!", "!мир!"}});

	qr = SimpleSelect("ООН");
	CheckResults(qr, {});

	qr = SimpleSelect("word");
	CheckResults(qr, {{"!word!", "test"},
					  {"test", "!word!"},
					  {"!word!", "!слово!"},
					  {"!word!", "!одно!"},
					  {"!слово! всего лишь !одно!", "test"},
					  {"!слово!", "!одно!"},
					  {"!слово одно!", "test"},
					  {"!одно слово!", "!word!"}});
}

TEST_F(FTApi, SelectWithMinusWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word", "several lexems"}, {"слово", "сколькото лексем"}}};
	Init(ftCfg);

	Add("nm1", "word", "test");
	Add("nm1", "several lexems", "test");
	Add("nm1", "слово", "test");
	Add("nm1", "сколькото лексем", "test");

	auto qr = SimpleSelect("test word");
	CheckResults(qr, {{"!word!", "!test!"}, {"several lexems", "!test!"}, {"!слово!", "!test!"}, {"!сколькото лексем!", "!test!"}});
	// Don't use synonyms
	qr = SimpleSelect("test -word");
	CheckResults(qr, {{"several lexems", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
	qr = SimpleSelect("test several lexems");
	CheckResults(qr, {{"word", "!test!"}, {"!several lexems!", "!test!"}, {"!слово!", "!test!"}, {"!сколькото лексем!", "!test!"}});
	// Don't use synonyms
	qr = SimpleSelect("test several -lexems");
	CheckResults(qr, {{"word", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
	// Don't use synonyms
	qr = SimpleSelect("test -several lexems");
	CheckResults(qr, {{"word", "!test!"}, {"слово", "!test!"}, {"сколькото лексем", "!test!"}});
}

TEST_F(FTApi, ChangeSynonymsCfg) {
	auto ftCfg = GetDefaultConfig();
	Init(ftCfg);

	Add("nm1", "UN", "test");
	Add("nm1", "United Nations", "test");
	Add("nm1", "ООН", "test");
	Add("nm1", "организация объединенных наций", "test");

	Add("nm1", "word", "test");
	Add("nm1", "several lexems", "test");
	Add("nm1", "слово", "test");
	Add("nm1", "сколькото лексем", "test");

	auto qr = SimpleSelect("UN");
	CheckResults(qr, {{"!UN!", "test"}});
	qr = SimpleSelect("United Nations");
	CheckResults(qr, {{"!United Nations!", "test"}});
	qr = SimpleSelect("word");
	CheckResults(qr, {{"!word!", "test"}});
	qr = SimpleSelect("several lexems");
	CheckResults(qr, {{"!several lexems!", "test"}});

	// Add synonyms
	ftCfg.synonyms = {{{"UN", "United Nations"}, {"ООН", "организация объединенных наций"}}};
	SetFTConfig(ftCfg, "nm1", "ft3");

	qr = SimpleSelect("UN");
	CheckResults(qr, {{"!UN!", "test"}, {"!ООН!", "test"}, {"!организация объединенных наций!", "test"}});
	qr = SimpleSelect("United Nations");
	CheckResults(qr, {{"!United Nations!", "test"}, {"!ООН!", "test"}, {"!организация объединенных наций!", "test"}});
	qr = SimpleSelect("word");
	CheckResults(qr, {{"!word!", "test"}});
	qr = SimpleSelect("several lexems");
	CheckResults(qr, {{"!several lexems!", "test"}});

	// Change synonyms
	ftCfg.synonyms = {{{"word", "several lexems"}, {"слово", "сколькото лексем"}}};
	SetFTConfig(ftCfg, "nm1", "ft3");

	qr = SimpleSelect("UN");
	CheckResults(qr, {{"!UN!", "test"}});
	qr = SimpleSelect("United Nations");
	CheckResults(qr, {{"!United Nations!", "test"}});
	qr = SimpleSelect("word");
	CheckResults(qr, {{"!word!", "test"}, {"!слово!", "test"}, {"!сколькото лексем!", "test"}});
	qr = SimpleSelect("several lexems");
	CheckResults(qr, {{"!several lexems!", "test"}, {"!слово!", "test"}, {"!сколькото лексем!", "test"}});

	// Remove synonyms
	ftCfg.synonyms.clear();
	SetFTConfig(ftCfg, "nm1", "ft3");

	qr = SimpleSelect("UN");
	CheckResults(qr, {{"!UN!", "test"}});
	qr = SimpleSelect("United Nations");
	CheckResults(qr, {{"!United Nations!", "test"}});
	qr = SimpleSelect("word");
	CheckResults(qr, {{"!word!", "test"}});
	qr = SimpleSelect("several lexems");
	CheckResults(qr, {{"!several lexems!", "test"}});
}

TEST_F(FTApi, SelectWithRelevanceBoostWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}, {{"United Nations"}, {"ООН"}}};
	Init(ftCfg);

	Add("nm1", "одно слово", "");
	Add("nm1", "", "ООН");

	auto res = SimpleSelect("word^2 United^0.5 Nations");
	ASSERT_EQ(res.Count(), 2);

	Item ritem = res[0].GetItem();
	string val = ritem["ft1"].As<string>();
	EXPECT_EQ(val, "!одно слово!");

	ritem = res[1].GetItem();
	val = ritem["ft2"].As<string>();
	EXPECT_EQ(val, "!ООН!");

	res = SimpleSelect("word^0.5 United^2 Nations^0.5");
	ASSERT_EQ(res.Count(), 2);

	ritem = res[0].GetItem();
	val = ritem["ft2"].As<string>();
	EXPECT_EQ(val, "!ООН!");

	ritem = res[1].GetItem();
	val = ritem["ft1"].As<string>();
	EXPECT_EQ(val, "!одно слово!");
}

TEST_F(FTApi, SelectWithFieldsBoostWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1", "одно слово", "");
	Add("nm1", "одно", "слово");
	Add("nm1", "", "одно слово");

	auto res = SimpleSelect("@ft1^2, ft2^0.5 word");
	ASSERT_EQ(res.Count(), 3);

	Item ritem = res[0].GetItem();
	string val = ritem["ft1"].As<string>();
	EXPECT_EQ(val, "!одно слово!");

	ritem = res[1].GetItem();
	val = ritem["ft1"].As<string>();
	EXPECT_EQ(val, "!одно!");

	ritem = res[2].GetItem();
	val = ritem["ft2"].As<string>();
	EXPECT_EQ(val, "!одно слово!");

	res = SimpleSelect("@ft1^0.5, ft2^2 word");
	ASSERT_EQ(res.Count(), 3);

	ritem = res[0].GetItem();
	val = ritem["ft2"].As<string>();
	EXPECT_EQ(val, "!одно слово!");

	ritem = res[1].GetItem();
	val = ritem["ft1"].As<string>();
	EXPECT_EQ(val, "!одно!");

	ritem = res[2].GetItem();
	val = ritem["ft1"].As<string>();
	EXPECT_EQ(val, "!одно слово!");
}

TEST_F(FTApi, SelectWithFieldsListWithSynonyms) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.synonyms = {{{"word"}, {"одно слово"}}};
	Init(ftCfg);

	Add("nm1", "одно слово", "");
	Add("nm1", "одно", "слово");
	Add("nm1", "", "одно слово");

	auto qr = SimpleSelect("word");
	CheckResults(qr, {{"!одно слово!", ""}, {"!одно!", "!слово!"}, {"", "!одно слово!"}});

	qr = SimpleSelect("@ft1 word");
	CheckResults(qr, {{"!одно слово!", ""}});

	qr = SimpleSelect("@ft2 word");
	CheckResults(qr, {{"", "!одно слово!"}});
}

TEST_F(FTApi, RankWithPosition) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.positionWeight = 1.0;
	Init(ftCfg);

	Add("nm1", "one two three word", "");
	Add("nm1", "one two three four five six word", "");
	Add("nm1", "one two three four word", "");
	Add("nm1", "one word", "");
	Add("nm1", "one two three four five word", "");
	Add("nm1", "word", "");
	Add("nm1", "one two word", "");

	auto qr = SimpleSelect("word");
	const char* expected[]{"!word!",
						   "one !word!",
						   "one two !word!",
						   "one two three !word!",
						   "one two three four !word!",
						   "one two three four five !word!",
						   "one two three four five six !word!"};

	ASSERT_TRUE(qr.Count() == (sizeof(expected) / sizeof(const char*)));

	const char** expIt = expected;
	for (auto resIt = qr.begin(); resIt != qr.end(); ++resIt, ++expIt) {
		Item item(resIt.GetItem());
		EXPECT_EQ(item["ft1"].As<string>(), *expIt);
	}
}

TEST_F(FTApi, SelectFullMatch) {
	auto ftCfg = GetDefaultConfig();
	ftCfg.fullMatchBoost = 0.9;
	Init(ftCfg);

	Add("nm1", "test", "love");
	Add("nm1", "test", "love second");

	auto qr = SimpleSelect("love");
	EXPECT_EQ(qr.Count(), 2);
	auto item = qr[0].GetItem();
	EXPECT_EQ(item["ft2"].As<string>(), "!love! second");

	ftCfg.fullMatchBoost = 1.1;
	SetFTConfig(ftCfg, "nm1", "ft3");
	qr = SimpleSelect("love");
	EXPECT_EQ(qr.Count(), 2);
	item = qr[0].GetItem();
	EXPECT_EQ(item["ft2"].As<string>(), "!love!");
}
