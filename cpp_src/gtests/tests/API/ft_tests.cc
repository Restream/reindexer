#include <iostream>
#include <unordered_set>
#include "ft_api.h"
#include "tools/stringstools.h"
using std::unordered_set;
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

	for (size_t i = 0; i < res.size(); ++i) {
		Item ritem(res.GetItem(i));
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
	auto err = reindexer->ConfigureIndex(
		"nm1", "ft3",
		R"xxx({"enable_translit": true,"enable_numbers_search": true,"enable_kb_layout": true,"merge_limit": 20000,"log_level": 1})xxx");
	EXPECT_TRUE(err.ok());

	Add("оценка 5 майкл джордан 23", "");

	auto res = SimpleSelect("пять +двадцать +три");
	EXPECT_TRUE(res.size() == 1);

	const string result = "оценка !5! майкл джордан !23!";

	for (size_t i = 0; i < res.size(); ++i) {
		Item ritem(res.GetItem(i));
		string val = ritem["ft1"].As<string>();
		std::cout << val << std::endl;
		EXPECT_TRUE(result == val);
	}
}
