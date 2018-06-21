#include "reindexer_api.h"

TEST_F(ReindexerApi, GetValueByJsonPath) {
	Error err = reindexer->OpenNamespace(default_namespace, StorageOpts().Enabled(false));
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->AddIndex(default_namespace, {"id", "", "hash", "int", IndexOpts().PK()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	struct Data {
		int64_t id;
		int64_t intField;
		string stringField;
		array<int, 3> intArray;
		int64_t firstInner;
		int64_t secondInner;
		int64_t thirdInner;
	};
	const string simpleJsonPattern =
		R"xxx({"id": %d, "monument" : [1,2,3], "inner": {"intField": %d, "stringField": "%s", "inner2": {"intArray": [%d, %d, %d], "inner3": [{"first":%d},{"second":%d},{"third":%d}]}}})xxx";

	for (int i = 0; i < 100; ++i) {
		Item item = reindexer->NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		Data data = {i, i + 1, "str" + std::to_string(i + 2), {{i + 3, i + 4, i + 5}}, i + 6, i + 7, i + 8};
		char json[1024];
		sprintf(json, simpleJsonPattern.c_str(), data.id, data.intField, data.stringField.c_str(), data.intArray[0], data.intArray[1],
				data.intArray[2], data.firstInner, data.secondInner, data.thirdInner);

		item.FromJSON(json);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();

		err = reindexer->Upsert(default_namespace, item);
		EXPECT_TRUE(err.ok()) << err.what();

		err = reindexer->Commit(default_namespace);
		EXPECT_TRUE(err.ok()) << err.what();

		KeyRefs intField = item.GetFieldByJSONPath("inner.intField");
		EXPECT_TRUE(intField.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(intField[0]) == data.intField);

		KeyRefs stringField = item.GetFieldByJSONPath("inner.stringField");
		EXPECT_TRUE(stringField.size() == 1);
		EXPECT_TRUE(static_cast<p_string>(stringField[0]).toString().compare(data.stringField) == 0);

		KeyRefs intArray = item.GetFieldByJSONPath("inner.inner2.intArray");
		EXPECT_TRUE(intArray.size() == 3);
		for (size_t j = 0; j < intArray.size(); ++j) {
			EXPECT_TRUE(static_cast<int64_t>(intArray[j]) == data.intArray[j]);
		}

		KeyRefs firstInner = item.GetFieldByJSONPath("inner.inner2.inner3.first");
		EXPECT_TRUE(firstInner.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(firstInner[0]) == data.firstInner);

		KeyRefs secondInner = item.GetFieldByJSONPath("inner.inner2.inner3.second");
		EXPECT_TRUE(secondInner.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(secondInner[0]) == data.secondInner);

		KeyRefs thirdInner = item.GetFieldByJSONPath("inner.inner2.inner3.third");
		EXPECT_TRUE(thirdInner.size() == 1);
		EXPECT_TRUE(static_cast<int64_t>(thirdInner[0]) == data.thirdInner);
	}
}
