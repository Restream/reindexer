#pragma once

#include <gtest/gtest.h>
#include "reindexer_api.h"
#include "tools/timetools.h"

class NsApi : public ReindexerApi {
protected:
	void DefineDefaultNamespace() {
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{stringField.c_str(), "hash", "string", IndexOpts(), 0},
												   IndexDeclaration{intField.c_str(), "hash", "int", IndexOpts(), 0},
												   IndexDeclaration{sparseField.c_str(), "hash", "int", IndexOpts().Sparse(), 0},
												   IndexDeclaration{indexedArrayField.c_str(), "hash", "int", IndexOpts().Array(), 0},
												   IndexDeclaration{doubleField.c_str(), "tree", "double", IndexOpts(), 0},
												   IndexDeclaration{boolField.c_str(), "-", "bool", IndexOpts(), 0},
												   IndexDeclaration{emptyField.c_str(), "hash", "string", IndexOpts(), 0}});
	}

	void FillDefaultNamespace() {
		for (int i = 0; i < 1000; ++i) {
			Item item = NewItem(default_namespace);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			item[idIdxName] = i;
			item[intField] = i;
			item[boolField] = i % 2 == 0;
			item[doubleField] = static_cast<double>(rand() % 100) + 3.33333;
			item[stringField] = std::to_string(i);
			item[indexedArrayField] = RandIntVector(10, 0, 100);

			Upsert(default_namespace, item);

			Error err = Commit(default_namespace);
			EXPECT_TRUE(err.ok()) << err.what();
		}
	}

	void AddUnindexedData() {
		Error err = rt.reindexer->AddIndex(default_namespace, reindexer::IndexDef("objects.more.array", {"objects.more.array"}, "hash",
																				  "int64", IndexOpts().Array(), 100000000000));
		ASSERT_TRUE(err.ok()) << err.what();

		char sourceJson[1024];
		const char jsonPattern[] =
			R"json({"id": %s, "indexed_array_field": [11,22,33,44,55,66,77,88,99], "objects":[{"more":[{"array":[9,8,7,6,5]},{"array":[4,3,2,1,0]}]}], "":{"empty_obj_field":"not empty"}, "array_field": [1,2,3], "string_array":["first", "second", "third"], "extra" : "%s", "sparse_field": %ld, "nested":{"bonus":%ld, "nested_array":[{"id":1,"name":"first", "prices":[1,2,3]},{"id":2,"name":"second", "prices":[4,5,6]},{"id":3,"name":"third", "nested":{"array":[0,0,0]}, "prices":[7,8,9]}]}, "nested2":{"bonus2":%ld}})json";
		for (size_t i = 1000; i < 2000; ++i) {
			Item item = NewItem(default_namespace);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			string serial = std::to_string(i);
			sprintf(sourceJson, jsonPattern, serial.c_str(), serial.c_str(), i, i * 2, i * 3);

			Error err = item.FromJSON(sourceJson);
			EXPECT_TRUE(err.ok()) << err.what();
			Upsert(default_namespace, item);

			err = Commit(default_namespace);
			EXPECT_TRUE(err.ok()) << err.what();
		}
	}

	void InsertNewTruncateItem(int i) {
		Item item = NewItem(truncate_namespace);
		item[idIdxName] = i;
		item["data"] = rand();
		item["price"] = rand();
		item["serialNumber"] = i * 100;
		item["fileName"] = "File" + std::to_string(i);
		item["ft11"] = RandString();
		item["ft12"] = RandString();
		item["ft21"] = RandString();
		item["ft22"] = RandString();
		item["ft23"] = RandString();
		auto err = rt.reindexer->Insert(truncate_namespace, item);
		ASSERT_TRUE(err.ok()) << err.what();
	}

	void TruncateNamespace(const std::function<Error(const std::string&)>& truncate) {
		Error err = rt.reindexer->OpenNamespace(truncate_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(
			truncate_namespace,
			{IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"date", "", "int64", IndexOpts(), 0},
			 IndexDeclaration{"price", "", "int64", IndexOpts(), 0}, IndexDeclaration{"serialNumber", "", "int64", IndexOpts(), 0},
			 IndexDeclaration{"fileName", "", "string", IndexOpts(), 0}});

		DefineNamespaceDataset(truncate_namespace, {IndexDeclaration{"ft11", "text", "string", IndexOpts(), 0},
													IndexDeclaration{"ft12", "text", "string", IndexOpts(), 0},
													IndexDeclaration{"ft11+ft12=ft13", "text", "composite", IndexOpts(), 0}});

		DefineNamespaceDataset(truncate_namespace, {IndexDeclaration{"ft21", "text", "string", IndexOpts(), 0},
													IndexDeclaration{"ft22", "text", "string", IndexOpts(), 0},
													IndexDeclaration{"ft23", "text", "string", IndexOpts(), 0},
													IndexDeclaration{"ft21+ft22+ft23=ft24", "text", "composite", IndexOpts(), 0}});

		static constexpr int itemsCount = 1000;
		for (int i = 0; i < itemsCount; ++i) InsertNewTruncateItem(i);

		const static Query q{truncate_namespace};
		QueryResults qr1;
		err = rt.reindexer->Select(q, qr1);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(itemsCount, qr1.Count());

		err = truncate(truncate_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults qr2;
		err = rt.reindexer->Select(q, qr2);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(0, qr2.Count());

		InsertNewTruncateItem(1);

		QueryResults qr3;
		err = rt.reindexer->Select(q, qr3);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(1, qr3.Count());
	}

	static void CheckItemsEqual(Item& lhs, Item& rhs) {
		for (auto idx = 1; idx < lhs.NumFields(); idx++) {
			auto field = lhs[idx].Name();
			ASSERT_TRUE(lhs[field].operator Variant() == rhs[field].operator Variant());
		}
	}

	const string truncate_namespace = "truncate_namespace";
	const string idIdxName = "id";
	const string updatedTimeSecFieldName = "updated_time_sec";
	const string updatedTimeMSecFieldName = "updated_time_msec";
	const string updatedTimeUSecFieldName = "updated_time_usec";
	const string updatedTimeNSecFieldName = "updated_time_nsec";
	const string serialFieldName = "serial_field_int";
	const string manualFieldName = "manual_field_int";
	const string intField = "int_field";
	const string doubleField = "double_field";
	const string boolField = "bool_field";
	const string sparseField = "sparse_field";
	const string stringField = "string_field";
	const string indexedArrayField = "indexed_array_field";
	const string emptyField = "empty_field";
	const int idNum = 1;
	const uint8_t upsertTimes = 3;
};
