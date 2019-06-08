#include "reindexer_api.h"
#include "tools/logger.h"

TEST_F(ReindexerApi, GetValueByJsonPath) {
	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "string", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	struct Data {
		std::string id;
		long intField;
		std::string stringField;
		std::array<int, 3> intArray;
		long firstInner;
		long secondInner;
		long thirdInner;
	};
	const char simpleJsonPattern[] =
		R"xxx({"id": "%s", "monument" : [1,2,3], "inner": {"intField": %ld, "stringField": "%s", "inner2": {"intArray": [%d, %d, %d], "inner3": [{"first":%ld},{"second":%ld},{"third":%ld}]}}})xxx";

	for (int i = 0; i < 100; ++i) {
		Item item = rt.reindexer->NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		Data data = {"pk" + std::to_string(i), i + 1, "str" + std::to_string(i + 2), {{i + 3, i + 4, i + 5}}, i + 6, i + 7, i + 8};
		char json[1024];
		sprintf(json, simpleJsonPattern, data.id.c_str(), data.intField, data.stringField.c_str(), data.intArray[0], data.intArray[1],
				data.intArray[2], data.firstInner, data.secondInner, data.thirdInner);

		err = item.FromJSON(json);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();

		VariantArray intField = item["inner.intField"];
		EXPECT_TRUE(intField.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(intField[0]) == data.intField);

		VariantArray stringField = item["inner.stringField"];
		EXPECT_TRUE(stringField.size() == 1);
		EXPECT_TRUE(stringField[0].As<string>().compare(data.stringField) == 0);

		VariantArray intArray = item["inner.inner2.intArray"];
		EXPECT_TRUE(intArray.size() == 3);
		for (size_t j = 0; j < intArray.size(); ++j) {
			EXPECT_TRUE(static_cast<int64_t>(intArray[j]) == data.intArray[j]);
		}

		VariantArray firstInner = item["inner.inner2.inner3.first"];
		EXPECT_TRUE(firstInner.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(firstInner[0]) == data.firstInner);

		VariantArray secondInner = item["inner.inner2.inner3.second"];
		EXPECT_TRUE(secondInner.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(secondInner[0]) == data.secondInner);

		VariantArray thirdInner = item["inner.inner2.inner3.third"];
		EXPECT_TRUE(thirdInner.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(thirdInner[0]) == data.thirdInner);
	}
}

TEST_F(ReindexerApi, SelectByJsonPath) {
	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "string", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	const char jsonPattern[] = R"xxx({"id": "%s", "nested": {"string": "%s", "int": %d, "intarray" : [1,2,3]}})xxx";

	std::vector<int64_t> properIntValues;
	for (int i = 0; i < 15; ++i) {
		Item item = rt.reindexer->NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		char json[512];
		auto pk = "pk" + std::to_string(i);
		string dumpField = "str_" + pk;
		sprintf(json, jsonPattern, pk.c_str(), dumpField.c_str(), i);

		if (i >= 5) properIntValues.push_back(i);

		err = item.FromJSON(json);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	QueryResults qr1;
	Variant strValueToFind("str_pk1");
	Query query1 = Query(default_namespace).Where("nested.string", CondEq, strValueToFind);
	err = rt.reindexer->Select(query1, qr1);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr1.Count() == 1);
	Item theOnlyItem = qr1[0].GetItem();
	VariantArray krefs = theOnlyItem["nested.string"];
	EXPECT_TRUE(krefs.size() == 1);
	EXPECT_TRUE(krefs[0].As<string>() == strValueToFind.As<string>());

	QueryResults qr2;
	Variant intValueToFind(static_cast<int64_t>(5));
	Query query2 = Query(default_namespace).Where("nested.int", CondGe, intValueToFind);
	err = rt.reindexer->Select(query2, qr2);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr2.Count() == 10);

	EXPECT_TRUE(properIntValues.size() == qr2.Count());
	for (size_t i = 0; i < properIntValues.size(); ++i) {
		Item item = qr2[i].GetItem();
		VariantArray krefs = item["nested.int"];
		EXPECT_TRUE(krefs.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(krefs[0]) == properIntValues[i]);
	}

	QueryResults qr3;
	Variant arrayItemToFind(static_cast<int64_t>(2));
	Query query3 = Query(default_namespace).Where("nested.intarray", CondGe, arrayItemToFind);
	err = rt.reindexer->Select(query3, qr3);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr3.Count() == 15);
}

TEST_F(ReindexerApi, CompositeFTSelectByJsonPath) {
	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "string", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"locale", "hash", "string", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	const char jsonPattern[] = R"xxx({"id": "%s", "locale" : "%s", "nested": {"name": "%s", "count": %ld}})xxx";

	for (int i = 0; i < 100000; ++i) {
		Item item = rt.reindexer->NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		char json[1024];
		auto index = std::to_string(i);
		auto pk = "key" + index;
		string name = "name" + index;
		string locale = i % 2 ? "en" : "ru";
		long count = i;
		sprintf(json, jsonPattern, pk.c_str(), locale.c_str(), name.c_str(), count);

		err = item.FromJSON(json);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();

		err = rt.reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();
	}

	err = rt.reindexer->AddIndex(default_namespace, {"composite_ft", {"nested.name", "id", "locale"}, "text", "composite", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	QueryResults qr;
	Query query = Query(default_namespace).Where("composite_ft", CondEq, "name2");
	err = rt.reindexer->Select(query, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() == 1);

	for (auto it : qr) {
		Item ritem(it.GetItem());
		auto json = ritem.GetJSON();
		EXPECT_TRUE(json == R"xxx({"id":"key2","locale":"ru","nested":{"name":"name2","count":2}})xxx");
	}
}
