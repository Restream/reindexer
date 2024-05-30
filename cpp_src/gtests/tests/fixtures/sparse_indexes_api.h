#pragma once

#include "reindexer_api.h"

class SparseIndexesApi : public ReindexerApi {
protected:
	void SetUp() override {
		Error err = rt.reindexer->OpenNamespace(default_namespace);
		ASSERT_TRUE(err.ok()) << err.what();

		DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldId, "hash", "string", IndexOpts().PK(), 0},
												   IndexDeclaration{kFieldName, "tree", "string", IndexOpts().Sparse(), 0},
												   IndexDeclaration{kFieldSerialNumber, "hash", "int64", IndexOpts().Sparse(), 0}});

		FillNsFromJson();
	}

	void FillNsFromJson() {
		for (int64_t i = 0; i < 100; ++i) {
			Item item = NewItem(default_namespace);
			EXPECT_TRUE(item.Status().ok()) << item.Status().what();

			FillItem(item, i);

			Upsert(default_namespace, item);
			Commit(default_namespace);
		}
	}

	void FillItem(Item& item, int64_t i) {
		item[kFieldId] = CreateID(i);
		item[kFieldName] = CreateName(i);
		item[kFieldSerialNumber] = i;
	}
	std::string CreateID(int64_t i) { return "key" + std::to_string(i); }
	std::string CreateName(int64_t i) { return "name" + std::to_string(i); }

	void CheckSelectAll() {
		QueryResults qr;
		Error err = rt.reindexer->Select(Query(default_namespace), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 100);

		for (size_t i = 0; i < qr.Count(); ++i) {
			Item ritem(qr[i].GetItem(false));
			std::string expectedName = "name" + std::to_string(i);
			Variant nameVal = ritem[kFieldName];
			EXPECT_TRUE(nameVal.Type().Is<reindexer::KeyValueType::String>());
			EXPECT_TRUE(nameVal.As<std::string>() == expectedName);

			int64_t expectedSerialNumber = static_cast<int64_t>(i);
			Variant serialNumberVal = ritem[kFieldSerialNumber];
			EXPECT_TRUE(serialNumberVal.Type().Is<reindexer::KeyValueType::Int64>());
			EXPECT_EQ(static_cast<int64_t>(serialNumberVal), expectedSerialNumber);
		}
	}

	void CheckSelectByTreeIndex() {
		QueryResults qr;
		Error err = rt.reindexer->Select(Query(default_namespace).Where(kFieldName, CondEq, Variant("name5")), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 1);
		Item ritem(qr[0].GetItem(false));
		Variant nameVal = ritem[kFieldName];
		EXPECT_TRUE(nameVal.Type().Is<reindexer::KeyValueType::String>());
		EXPECT_EQ(nameVal.As<std::string>(), "name5");

		QueryResults qr2;
		const std::string toCompare("name2");
		err = rt.reindexer->Select(Query(default_namespace).Where(kFieldName, CondLt, Variant(toCompare)), qr2);
		ASSERT_TRUE(err.ok()) << err.what();

		for (auto it : qr2) {
			Item ritem(it.GetItem(false));
			Variant nameVal = ritem[kFieldName];
			EXPECT_TRUE(nameVal.Type().Is<reindexer::KeyValueType::String>());
			EXPECT_LT(nameVal.As<std::string>().compare(toCompare), 0);
		}
	}

	void CheckSelectByHashIndex() {
		QueryResults qr;
		Error err = rt.reindexer->Select(Query(default_namespace).Where(kFieldSerialNumber, CondLt, Variant(static_cast<int64_t>(50))), qr);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 50);
		for (int64_t i = 0; i < static_cast<int64_t>(qr.Count()); ++i) {
			Item ritem(qr[i].GetItem(false));
			Variant serialNumberVal = ritem[kFieldSerialNumber];
			EXPECT_TRUE(serialNumberVal.Type().Is<reindexer::KeyValueType::Int64>());
			EXPECT_EQ(static_cast<int64_t>(serialNumberVal), i);
		}

		QueryResults qr2;
		int64_t expectedValue(static_cast<int64_t>(77));
		err = rt.reindexer->Select(Query(default_namespace).Where(kFieldSerialNumber, CondEq, Variant(expectedValue)), qr2);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr2.Count(), 1);

		Item ritem(qr2[0].GetItem(false));
		Variant serialNumberVal = ritem[kFieldSerialNumber];
		EXPECT_TRUE(serialNumberVal.Type().Is<reindexer::KeyValueType::Int64>());
		EXPECT_EQ(static_cast<int64_t>(serialNumberVal), expectedValue);
	}

	const char* kFieldId = "id";
	const char* kFieldName = "name";
	const char* kFieldSerialNumber = "serialNumber";
};
