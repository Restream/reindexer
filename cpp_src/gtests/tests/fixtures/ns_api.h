#pragma once

#include <gtest/gtest.h>
#include "reindexer_api.h"

class [[nodiscard]] NsApi : public ReindexerApi {
protected:
	void DefineDefaultNamespace() {
		rt.OpenNamespace(default_namespace);
		// clang-format off
		DefineNamespaceDataset(default_namespace, {IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0},
												   IndexDeclaration{stringField, "hash", "string", IndexOpts(), 0},
												   IndexDeclaration{intField, "hash", "int", IndexOpts(), 0},
												   IndexDeclaration{sparseField, "hash", "int", IndexOpts().Sparse(), 0},
												   IndexDeclaration{indexedArrayField, "hash", "int", IndexOpts().Array(), 0},
												   IndexDeclaration{doubleField, "tree", "double", IndexOpts(), 0},
												   IndexDeclaration{boolField, "-", "bool", IndexOpts(), 0},
												   IndexDeclaration{emptyField, "hash", "string", IndexOpts(), 0}});
		// clang-format on
	}

	void FillDefaultNamespace(int count = 1000) {
		for (int i = 0; i < count; ++i) {
			Item item = NewItem(default_namespace);

			item[idIdxName] = i;
			item[intField] = i;
			item[boolField] = i % 2 == 0;
			item[doubleField] = static_cast<double>(rand() % 100) + 3.33333;
			item[stringField] = std::to_string(i);
			item[indexedArrayField] = RandIntVector(10, 0, 100);
			item[sparseField] = i * 3;

			Upsert(default_namespace, item);
		}
	}

	void AddUnindexedData() {
		rt.AddIndex(default_namespace,
					reindexer::IndexDef("objects.more.array", {"objects.more.array"}, "hash", "int64", IndexOpts().Array(), 100000000000));
		constexpr auto jsonPattern =
			R"json({{"id": {},
			"int_field":1,
			"int_field2":88,
			"indexed_array_field": [11,22,33,44,55,66,77,88,99],
			"objects":[{{"more":[{{"array":[9,8,7,6,5]}},{{"array":[4,3,2,1,0]}}]}}],
			"":{{"empty_obj_field":"not empty"}},
			"array_field": [1,2,3],
			"string_array":["first", "second", "third"],
			"bool_array":[true, false],
			"bool_array2":[false, true],
			"extra" : "{}",
			"sparse_field": {},
			"nested":{{
				"bonus":{},
				"nested_array":[
					{{"id":1,"name":"first", "prices":[1,2,3]}},
					{{"id":2,"name":"second", "prices":[4,5,6]}},
					{{"id":3,"name":"third", "nested":{{"array":[0,0,0]}}, "prices":[7,8,9]}}
				]
			}},
			"nested2":{{"bonus2":{}}}}})json";
		for (size_t i = 1000; i < 2000; ++i) {
			std::string serial = std::to_string(i);
			auto json = fmt::format(jsonPattern, serial, serial, i, i * 2, i * 3);
			rt.UpsertJSON(default_namespace, json);
		}
	}

	void AddHeterogeneousNestedData() {
		constexpr auto jsonPattern =
			R"json({{
			"id": {},
			"int_field":1,
			"indexed_array_field": [11,22,33,44,55,66,77,88,99],
			"objects":[{{"array":[{{"field":[9,8,7,6,5]}},{{"field":11}},{{"field":[4,3,2,1,0]}},{{"field":[99]}}]}}],
			"":{{"empty_obj_field":"not empty"}},
			"array_field": [1,2,3],
			"string_array":["first", "second", "third"],
			"extra" : "{}",
			"sparse_field": {},
			"nested":{{"bonus":{}, "nested_array":[{{"id":1,"name":"first", "prices":[1,2,3]}},{{"id":2,"name":"second","prices":[4,5,6]}},{{"id":3,"name":"third", "nested":{{"array":[0,0,0]}}, "prices":[7,8,9]}}]}}, "nested2":{{"bonus2":{}}}
			}})json";

		for (size_t i = 1000; i < 2000; ++i) {
			std::string serial = std::to_string(i);
			auto json = fmt::format(jsonPattern, serial, serial, i, i * 2, i * 3);

			rt.UpsertJSON(default_namespace, json);
		}
	}

	void CreateEmptyArraysNamespace(std::string_view nsName) {
		rt.OpenNamespace(nsName);
		DefineNamespaceDataset(nsName, {IndexDeclaration{idIdxName.c_str(), "hash", "int", IndexOpts().PK(), 0},
										IndexDeclaration{indexedArrayField.c_str(), "hash", "int", IndexOpts().Array(), 0}});
		for (size_t i = 100; i < 200; ++i) {
			rt.UpsertJSON(nsName, fmt::format(R"json({{"id": {}, "indexed_array_field": [], "non_indexed_array_field": []}})json", i));
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
		rt.Insert(truncate_namespace, item);
	}

	void TruncateNamespace(const std::function<Error(const std::string&)>& truncate) {
		rt.OpenNamespace(truncate_namespace);
		DefineNamespaceDataset(
			truncate_namespace,
			{IndexDeclaration{idIdxName, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{"date", "", "int64", IndexOpts(), 0},
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
		for (int i = 0; i < itemsCount; ++i) {
			InsertNewTruncateItem(i);
		}

		const static Query q{truncate_namespace};
		QueryResults qr1 = rt.Select(q);
		ASSERT_EQ(itemsCount, qr1.Count());

		auto err = truncate(truncate_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		QueryResults qr2 = rt.Select(q);
		ASSERT_EQ(0, qr2.Count());

		InsertNewTruncateItem(1);

		QueryResults qr3 = rt.Select(q);
		ASSERT_EQ(1, qr3.Count());
	}

	static void CheckItemsEqual(Item& lhs, Item& rhs) {
		for (auto idx = 1; idx < lhs.NumFields(); idx++) {
			auto field = lhs[idx].Name();
			ASSERT_TRUE(lhs[field].operator Variant() == rhs[field].operator Variant());
		}
	}

	const std::string truncate_namespace = "truncate_namespace";
	const std::string idIdxName = "id";
	const std::string updatedTimeSecFieldName = "updated_time_sec";
	const std::string updatedTimeMSecFieldName = "updated_time_msec";
	const std::string updatedTimeUSecFieldName = "updated_time_usec";
	const std::string updatedTimeNSecFieldName = "updated_time_nsec";
	const std::string serialFieldName = "serial_field_int";
	const std::string manualFieldName = "manual_field_int";
	const std::string intField = "int_field";
	const std::string doubleField = "double_field";
	const std::string boolField = "bool_field";
	const std::string sparseField = "sparse_field";
	const std::string stringField = "string_field";
	const std::string indexedArrayField = "indexed_array_field";
	const std::string emptyField = "empty_field";
	const int idNum = 1;
	const uint8_t upsertTimes = 3;
};
