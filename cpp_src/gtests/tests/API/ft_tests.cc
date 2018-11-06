#include <iostream>
#include <unordered_map>
#include <unordered_set>
#include "debug/allocdebug.h"
#include "ft_api.h"
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
		string val = ritem["ft1"].As<string>();
		// printf("%s\n", val.c_str());
		std::cout << val << std::endl;
	}
}

TEST_F(FTApi, Stress) {
	vector<string> data;
	vector<string> phrase;

	for (size_t i = 0; i < 12500; ++i) {
		data.push_back(RandString());
	}

	for (size_t i = 0; i < 50000; ++i) {
		phrase.push_back(data[rand() % data.size()] + "  " + data[rand() % data.size()] + " " + data[rand() % data.size()]);
	}

	for (size_t i = 0; i < phrase.size(); i++) {
		Add(phrase[i], phrase[rand() % phrase.size()]);
		if (i % 2500 == 0) {
			SimpleCompositeSelect(data[rand() % data.size()]);
			auto res = SimpleCompositeSelect(data[rand() % data.size()] + "~");
			SimpleCompositeSelect(data[rand() % data.size()] + "*");
		}
	}
}
