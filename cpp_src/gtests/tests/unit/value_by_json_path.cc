#include <ranges>
#include "core/cjson/jsonbuilder.h"
#include "reindexer_api.h"

TEST_F(ReindexerApi, GetValueByJsonPath) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "string", IndexOpts().PK()});

	struct [[nodiscard]] Data {
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
		Item item(rt.NewItem(default_namespace));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		Data data = {"pk" + std::to_string(i), i + 1, "str" + std::to_string(i + 2), {{i + 3, i + 4, i + 5}}, i + 6, i + 7, i + 8};
		char json[1024];
		snprintf(json, sizeof(json) - 1, simpleJsonPattern, data.id.c_str(), data.intField, data.stringField.c_str(), data.intArray[0],
				 data.intArray[1], data.intArray[2], data.firstInner, data.secondInner, data.thirdInner);

		auto err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();

		rt.Upsert(default_namespace, item);

		VariantArray intField = item["inner.intField"];
		ASSERT_EQ(intField.size(), 1);
		EXPECT_EQ(static_cast<int64_t>(intField[0]), data.intField);

		VariantArray stringField = item["inner.stringField"];
		ASSERT_EQ(stringField.size(), 1);
		EXPECT_EQ(stringField[0].As<std::string>().compare(data.stringField), 0);

		VariantArray intArray = item["inner.inner2.intArray"];
		ASSERT_EQ(intArray.size(), data.intArray.size());
		for (size_t j = 0; j < intArray.size(); ++j) {
			EXPECT_EQ(static_cast<int64_t>(intArray[j]), data.intArray[j]);
		}

		VariantArray firstInner = item["inner.inner2.inner3.first"];
		ASSERT_EQ(firstInner.size(), 1);
		EXPECT_EQ(static_cast<int64_t>(firstInner[0]), data.firstInner);

		VariantArray secondInner = item["inner.inner2.inner3.second"];
		ASSERT_EQ(secondInner.size(), 1);
		EXPECT_EQ(static_cast<int64_t>(secondInner[0]), data.secondInner);

		VariantArray thirdInner = item["inner.inner2.inner3.third"];
		ASSERT_EQ(thirdInner.size(), 1);
		EXPECT_EQ(static_cast<int64_t>(thirdInner[0]), data.thirdInner);
	}
}

TEST_F(ReindexerApi, SelectByJsonPath) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "string", IndexOpts().PK()});

	const char jsonPattern[] = R"xxx({"id": "%s", "nested": {"string": "%s", "int": %d, "intarray" : [1,2,3]}})xxx";

	std::vector<int64_t> properIntValues;
	for (int i = 0; i < 15; ++i) {
		Item item(rt.NewItem(default_namespace));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();

		char json[512];
		auto pk = "pk" + std::to_string(i);
		std::string dumpField = "str_" + pk;
		snprintf(json, sizeof(json) - 1, jsonPattern, pk.c_str(), dumpField.c_str(), i);

		if (i >= 5) {
			properIntValues.push_back(i);
		}

		auto err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();

		rt.Upsert(default_namespace, item);
	}

	Variant strValueToFind("str_pk1");
	Query query1{Query(default_namespace).Where("nested.string", CondEq, strValueToFind)};
	auto qr1 = rt.Select(query1);
	ASSERT_EQ(qr1.Count(), 1);
	Item theOnlyItem = qr1.begin().GetItem(false);
	VariantArray krefs = theOnlyItem["nested.string"];
	ASSERT_EQ(krefs.size(), 1);
	EXPECT_EQ(krefs[0].As<std::string>(), strValueToFind.As<std::string>());

	Variant intValueToFind(static_cast<int64_t>(5));
	Query query2{Query(default_namespace).Where("nested.int", CondGe, intValueToFind)};
	auto qr2 = rt.Select(query2);
	ASSERT_EQ(qr2.Count(), properIntValues.size());

	ASSERT_EQ(properIntValues.size(), qr2.Count());
	size_t i = 0;
	for (auto& it : qr2) {
		Item item = it.GetItem(false);
		VariantArray krefs = item["nested.int"];
		ASSERT_EQ(krefs.size(), 1);
		EXPECT_EQ(static_cast<int64_t>(krefs[0]), properIntValues[i++]);
	}

	Variant arrayItemToFind(static_cast<int64_t>(2));
	Query query3{Query(default_namespace).Where("nested.intarray", CondGe, arrayItemToFind)};
	auto qr3 = rt.Select(query3);
	EXPECT_EQ(qr3.Count(), 15);
}

TEST_F(ReindexerApi, CompositeFTSelectByJsonPath) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "string", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"locale", "hash", "string", IndexOpts()});

	const char jsonPattern[] = R"xxx({"id": "key%d", "locale" : "%s", "nested": {"name": "name%d", "count": %ld}})xxx";

	for (int i = 0; i < 20'000; ++i) {
		char json[1024];
		long count = i;
		snprintf(json, sizeof(json) - 1, jsonPattern, i, i % 2 ? "en" : "ru", i, count);

		rt.UpsertJSON(default_namespace, json);
	}

	rt.AddIndex(default_namespace, {"composite_ft", {"nested.name", "id", "locale"}, "text", "composite", IndexOpts()});

	Query query{Query(default_namespace).Where("composite_ft", CondEq, "name2")};
	auto qr = rt.Select(query);
	EXPECT_EQ(qr.Count(), 1);

	for (auto it : qr) {
		Item ritem(it.GetItem(false));
		auto json = ritem.GetJSON();
		EXPECT_EQ(json, R"xxx({"id":"key2","locale":"ru","nested":{"name":"name2","count":2}})xxx");
	}
}

TEST_F(ReindexerApi, CompositeFTSelectByJsonPathWithNulls) {
	constexpr size_t kItems = 1000;
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "string", IndexOpts().PK()});
	rt.AddIndex(default_namespace, {"locale", "hash", "string", IndexOpts()});

	std::unordered_set<std::string> allIds;
	std::unordered_set<std::string> nullIds;
	std::unordered_set<std::string> nonNullIds;
	for (size_t i = 0; i < kItems; ++i) {
		std::string json;
		std::string idStr = fmt::format("key{}", i);
		const std::string_view locale = i % 2 ? "en" : "ru";
		if (rand() % 100) {
			json = fmt::format(R"j({{"id": "{}", "locale" : "{}", "nested": {{"name": "name{}", "count": {}}}}})j", idStr, locale, i, i);
			nonNullIds.emplace(std::move(idStr));
		} else if (rand() % 2) {
			json = fmt::format(R"j({{"id": "{}", "locale" : "{}", "nested": {{"name": null, "count": {}}}}})j", idStr, locale, i);
			nullIds.emplace(std::move(idStr));
		} else {
			json = fmt::format(R"j({{"id": "{}", "locale" : "{}", "nested": {{"count": {}}}}})j", idStr, locale, i);
			nullIds.emplace(std::move(idStr));
		}
		rt.UpsertJSON(default_namespace, json);
	}
	allIds.insert(nullIds.cbegin(), nullIds.cend());
	allIds.insert(nonNullIds.cbegin(), nonNullIds.cend());

	rt.AddIndex(default_namespace, {"composite_ft", {"nested.name", "id", "locale"}, "text", "composite", IndexOpts()});

	auto checkItems = [](const reindexer::QueryResults& qr, const std::unordered_set<std::string>& expected, std::string_view checkName) {
		SCOPED_TRACE(checkName);
		EXPECT_EQ(qr.Count(), expected.size());
		for (auto& it : qr) {
			auto item = it.GetItem(false);
			auto key = item["id"].As<std::string>();
			EXPECT_TRUE(expected.contains(key)) << "unexpected key: " << key;
		}
	};

	// Select everything by key
	auto qr = rt.Select(Query(default_namespace).Where("composite_ft", CondEq, "=key*"));
	checkItems(qr, allIds, "all ids");
	// Select non-null by name
	qr = rt.Select(Query(default_namespace).Where("composite_ft", CondEq, "=name*"));
	checkItems(qr, nonNullIds, "null ids");
	// Remove all nulls
	const auto removed = rt.Delete(Query(default_namespace).Where("nested.name", CondEmpty, reindexer::Variant()));
	ASSERT_EQ(removed, nullIds.size());
	// Select everything by key (there are no nulls anymore)
	qr = rt.Select(Query(default_namespace).Where("composite_ft", CondEq, "=key*"));
	checkItems(qr, nonNullIds, "non-null ids 1");
	// Select non-null by name (same as 'by key')
	qr = rt.Select(Query(default_namespace).Where("composite_ft", CondEq, "=name*"));
	checkItems(qr, nonNullIds, "non-null ids 2");
}

TEST_F(ReindexerApi, NumericSearchForNonIndexedField) {
	// Define namespace structure
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	reindexer::WrSerializer wrser;
	reindexer::JsonBuilder item1Builder(wrser, reindexer::ObjType::TypeObject);

	// Insert one item with integer 'mac_address' value
	Item item1(rt.NewItem(default_namespace));
	ASSERT_TRUE(item1.Status().ok()) << item1.Status().what();
	item1Builder.Put("id", int(1));
	item1Builder.Put("mac_address", int64_t(2147483648));
	item1Builder.End();
	auto err = item1.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	rt.Upsert(default_namespace, item1);

	wrser.Reset();

	// Insert another item with string 'mac_address' value
	reindexer::JsonBuilder item2Builder(wrser, reindexer::ObjType::TypeObject);
	Item item2(rt.NewItem(default_namespace));
	ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();
	item2Builder.Put("id", int(2));
	item2Builder.Put("mac_address", Variant(std::string("2147483648")));
	item2Builder.End();
	err = item2.FromJSON(wrser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	rt.Upsert(default_namespace, item2);

	auto qr = rt.ExecSQL("select * from test_namespace where mac_address = '2147483648'");
	EXPECT_EQ(qr.Count(), 2);
	qr = rt.Select(Query(default_namespace).Where("mac_address", CondEq, Variant(int64_t(2147483648))));
	EXPECT_EQ(qr.Count(), 2);
}

TEST_F(ReindexerApi, SetFieldWithDotsTest) {
	static const std::vector idxNamesAndNestedPaths{std::pair{"nested1", "obj1.id"},
													std::pair{"nested2", "obj1.id1"},

													std::pair{"nested3", "obj1.obj2.id1"},
													std::pair{"nested4", "obj1.obj2.id2"},

													std::pair{"nested5", "obj1.obj2.obj3.id1"},
													std::pair{"nested6", "obj1.obj2.obj3.id3"},

													std::pair{"nested7", "obj1.x.obj2.obj3.id1"},
													std::pair{"nested8", "obj1.x.obj2.obj3.id3"}};

	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	auto addNestedIdx = [this](const auto& idxName, auto jsonPath) {
		rt.AddIndex(default_namespace, {idxName, reindexer::JsonPaths{jsonPath}, "-", "int", IndexOpts()});
	};

	for (const auto& [name, path] : idxNamesAndNestedPaths) {
		addNestedIdx(name, path);
	}

	const std::string_view expected =
		R"json({"id":0,"obj1":{"id":1,"id1":2,"obj2":{"id1":3,"id2":4,"obj3":{"id1":5,"id3":6,"non_idx_field2":"non_idx_field2","non_idx_field3":"non_idx_field3"}},"x":{"obj2":{"obj3":{"id1":7,"id3":8,"non_idx_field1":"non_idx_field1"}}}}})json";

	auto test = [this, expected](const auto& fieldNames) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = 0;

		for (size_t i = 0; i < idxNamesAndNestedPaths.size(); ++i) {
			item[fieldNames[i]] = int(i + 1);
		}

		item["obj1.x.obj2.obj3.non_idx_field1"] = "non_idx_field1";
		item["obj1.obj2.obj3.non_idx_field2"] = "non_idx_field2";
		item["obj1.obj2.obj3.non_idx_field3"] = "non_idx_field3";

		ASSERT_EQ(item.GetJSON(), expected);

		rt.Insert(default_namespace, item);

		auto qr = rt.Select(Query(default_namespace));
		ASSERT_EQ(qr.Count(), 1);

		auto it = qr.begin();
		ASSERT_EQ(expected, *qr.begin().GetJSON());

		rt.TruncateNamespace(default_namespace);
	};

	test(std::views::elements<0>(idxNamesAndNestedPaths));
	test(std::views::elements<1>(idxNamesAndNestedPaths));
}

TEST_F(ReindexerApi, SetSparseFieldThroughItemImplTest) {
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	rt.AddIndex(default_namespace, {"idx1", reindexer::JsonPaths{"f"}, "hash", "string", IndexOpts().Sparse()});
	rt.AddIndex(default_namespace, {"idx2", reindexer::JsonPaths{"f1.f2.f3.f"}, "hash", "string", IndexOpts().Sparse()});

	auto test = [&](std::string_view name, std::string_view expected) {
		Item item(rt.NewItem(default_namespace));
		item["id"] = 0;
		item[name] = "some_value";

		ASSERT_EQ(item.GetJSON(), expected);

		rt.Insert(default_namespace, item);

		auto qr = rt.Select(Query(default_namespace));
		ASSERT_EQ(qr.Count(), 1);

		auto it = qr.begin();
		ASSERT_EQ(expected, *qr.begin().GetJSON());
		rt.TruncateNamespace(default_namespace);
	};

	std::string_view expected = R"json({"id":0,"f":"some_value"})json";
	test("idx1", expected);
	test("f", expected);

	expected = R"json({"id":0,"f1":{"f2":{"f3":{"f":"some_value"}}}})json";
	test("idx2", expected);
	test("f1.f2.f3.f", expected);
}

TEST_F(ReindexerApi, ExceptionMultyJsonPathsThroughItemImplTest) {
	const auto nestedArrPath1 = "f1.f2.f3";
	rt.OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	rt.AddIndex(default_namespace, {"id", "hash", "int", IndexOpts().PK()});

	rt.AddIndex(default_namespace,
				{"value", reindexer::JsonPaths{"value1", "value2", nestedArrPath1}, "hash", "string", IndexOpts().Array()});

	Item item(rt.NewItem(default_namespace));
	item["id"] = 0;

	auto test = [&item](std::string_view name) {
		try {
			item[name] = "some_value";
			ASSERT_TRUE(false);
		} catch (const Error& err) {
			ASSERT_EQ(err, Error(errLogic,
								 "It is not allowed to use fields with multiple json paths in the Item::operator[]. Index name = `value`"));
		}
	};

	test("value");
	test("value1");
	test("value2");
	test(nestedArrPath1);
}
