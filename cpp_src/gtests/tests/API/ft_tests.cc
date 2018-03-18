#include <unordered_set>
#include "ft_api.h"
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
