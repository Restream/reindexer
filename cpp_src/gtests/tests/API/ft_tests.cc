#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include "debug/allocdebug.h"
#include "ft_api.h"
#include "tools/logger.h"
#include "tools/stringstools.h"
using std::unordered_set;
using std::unordered_map;

TEST_F(FTApi, CompositeSelect) {
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

TEST_F(FTApi, NumberToWordsSelect) {
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
	unordered_map<string, int> data;

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

	for (auto it : res) {
		Item ritem(it.GetItem());
		std::cout << ritem["ft1"].As<string>() << std::endl;
	}
}

TEST_F(FTApi, Stress) {
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
	vector<string> data;
	set<size_t> check;
	set<string> checks;
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
