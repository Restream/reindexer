#include <iostream>
#include <unordered_map>
#include <unordered_set>
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
			EXPECT_TRUE(it != data.end());
			data.erase(it);
		}
	}
	EXPECT_TRUE(data.empty());
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
	EXPECT_TRUE(res.Count() == 1);

	for (size_t i = 0; i < res.Count(); ++i) {
		Item ritem = res[i].GetItem();
		string val = ritem["ft1"].As<string>();
		EXPECT_TRUE(val == "!including! all of them");
	}
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
						std::cout << ritem["ft1"].As<string>() << std::endl;
					}
					abort();
				}
			}
		}
	}
}
