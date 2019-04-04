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
												   IndexDeclaration{boolField.c_str(), "-", "bool", IndexOpts(), 0}});
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
		char sourceJson[1024];
		const char jsonPattern[] =
			R"json({"id": %s, "indexed_array_field": [0,0], "":{"empty_obj_field":"not empty"}, "array_field": [1,2,3], "extra" : "%s", "sparse_field": %ld, "nested":{"bonus":%ld}, "nested2":{"bonus2":%ld}})json";
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
	const int idNum = 1;
	const uint8_t upsertTimes = 3;
};
