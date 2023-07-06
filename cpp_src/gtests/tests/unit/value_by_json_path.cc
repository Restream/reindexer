#include "core/cjson/jsonbuilder.h"
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
		snprintf(json, sizeof(json) - 1, simpleJsonPattern, data.id.c_str(), data.intField, data.stringField.c_str(), data.intArray[0],
				 data.intArray[1], data.intArray[2], data.firstInner, data.secondInner, data.thirdInner);

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
		EXPECT_TRUE(stringField[0].As<std::string>().compare(data.stringField) == 0);

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
		std::string dumpField = "str_" + pk;
		snprintf(json, sizeof(json) - 1, jsonPattern, pk.c_str(), dumpField.c_str(), i);

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
	Query query1{Query(default_namespace).Where("nested.string", CondEq, strValueToFind)};
	err = rt.reindexer->Select(query1, qr1);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr1.Count() == 1);
	Item theOnlyItem = qr1[0].GetItem(false);
	VariantArray krefs = theOnlyItem["nested.string"];
	EXPECT_TRUE(krefs.size() == 1);
	EXPECT_TRUE(krefs[0].As<std::string>() == strValueToFind.As<std::string>());

	QueryResults qr2;
	Variant intValueToFind(static_cast<int64_t>(5));
	Query query2{Query(default_namespace).Where("nested.int", CondGe, intValueToFind)};
	err = rt.reindexer->Select(query2, qr2);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr2.Count() == 10);

	EXPECT_TRUE(properIntValues.size() == qr2.Count());
	for (size_t i = 0; i < properIntValues.size(); ++i) {
		Item item = qr2[i].GetItem(false);
		VariantArray krefs = item["nested.int"];
		EXPECT_TRUE(krefs.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(krefs[0]) == properIntValues[i]);
	}

	QueryResults qr3;
	Variant arrayItemToFind(static_cast<int64_t>(2));
	Query query3{Query(default_namespace).Where("nested.intarray", CondGe, arrayItemToFind)};
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

	const char jsonPattern[] = R"xxx({"id": "key%d", "locale" : "%s", "nested": {"name": "name%d", "count": %ld}})xxx";

	for (int i = 0; i < 20'000; ++i) {
		Item item = rt.reindexer->NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		char json[1024];
		long count = i;
		snprintf(json, sizeof(json) - 1, jsonPattern, i, i % 2 ? "en" : "ru", i, count);

		err = item.Unsafe(true).FromJSON(json);
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
	Query query{Query(default_namespace).Where("composite_ft", CondEq, "name2")};
	err = rt.reindexer->Select(query, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() == 1);

	for (auto it : qr) {
		Item ritem(it.GetItem(false));
		auto json = ritem.GetJSON();
		EXPECT_TRUE(json == R"xxx({"id":"key2","locale":"ru","nested":{"name":"name2","count":2}})xxx");
	}
}

TEST_F(ReindexerApi, NumericSearchForNonIndexedField) {
	// Define namespace structure
	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder item1Builder(wrser, reindexer::ObjType::TypeObject);

	// Insert one item with integer 'mac_address' value
	Item item1 = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item1.Status().ok()) << item1.Status().what();
	item1Builder.Put("id", int(1));
	item1Builder.Put("mac_address", int64_t(2147483648));
	item1Builder.End();
	err = item1.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->Upsert(default_namespace, item1);
	ASSERT_TRUE(err.ok()) << err.what();

	wrser.Reset();

	// Insert another item with string 'mac_address' value
	reindexer::JsonBuilder item2Builder(wrser, reindexer::ObjType::TypeObject);
	Item item2 = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();
	item2Builder.Put("id", int(2));
	item2Builder.Put("mac_address", Variant(std::string("2147483648")));
	item2Builder.End();
	err = item2.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->Upsert(default_namespace, item2);
	ASSERT_TRUE(err.ok()) << err.what();

	// Make sure while seeking for a string we only get a string value as a result
	{
		QueryResults qr;
		err = rt.reindexer->Select("select * from test_namespace where mac_address = '2147483648'", qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 1) << qr.Count();
		Item item = qr[0].GetItem(false);
		Variant id = item["id"];
		ASSERT_TRUE(static_cast<int>(id) == 2);
		Variant value = item["mac_address"];
		ASSERT_TRUE(value.Type().Is<reindexer::KeyValueType::String>()) << value.Type().Name();
	}

	// Make sure while seeking for a number we only get an integer value as a result
	{
		QueryResults qr;
		err = rt.reindexer->Select(Query(default_namespace).Where("mac_address", CondEq, Variant(int64_t(2147483648))), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(qr.Count() == 1) << qr.Count();
		Item item = qr[0].GetItem(false);
		Variant id = item["id"];
		ASSERT_TRUE(static_cast<int>(id) == 1);
		Variant value = item["mac_address"];
		ASSERT_TRUE(value.Type().Is<reindexer::KeyValueType::Int64>());
	}
}

TEST_F(ReindexerApi, InsertWithSeveralJsonPaths) {
	// Define namespace structure with an indexed field that has 3 json paths
	Error err = rt.reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->AddIndex(default_namespace, {"name", {"name", "text", "description"}, "hash", "string", IndexOpts(), 0});
	ASSERT_TRUE(err.ok()) << err.what();

	// Build an item, which includes (and sets) all the json-paths for the field 'name'
	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder jsonBuilder(wrser, reindexer::ObjType::TypeObject);
	jsonBuilder.Put("id", int(1));
	jsonBuilder.Put("name", "first");
	jsonBuilder.Put("text", "second");
	jsonBuilder.Put("description", "third");
	jsonBuilder.End();

	// Insert the item
	Item item = rt.reindexer->NewItem(default_namespace);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->Insert(default_namespace, item);

	// Make sure it returned an error of type 'errParams'
	ASSERT_TRUE(!err.ok() && err.code() == errParams);
}
