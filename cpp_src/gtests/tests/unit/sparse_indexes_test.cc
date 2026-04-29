#include "gmock/gmock.h"
#include "sparse_indexes_api.h"

TEST_F(SparseIndexesApi, SparseIndexSelectAll) { CheckSelectAll(); }
TEST_F(SparseIndexesApi, SelectByTreeSparseIndex) { CheckSelectByTreeIndex(); }
TEST_F(SparseIndexesApi, SelectByHashSparseIndex) { CheckSelectByHashIndex(); }

TEST_F(SparseIndexesApi, SparseIndexConsistencyWithRuntimeIndexes) {
	const char* rtIndexName = "rt1";
	rt.AddIndex(default_namespace, {rtIndexName, "tree", "int64", IndexOpts().Sparse()});
	CheckSelectAll();

	constexpr int64_t kCount = 10;
	const Query q = Query(default_namespace).Where(rtIndexName, CondLt, Variant(kCount));
	auto qr = rt.Select(q);
	ASSERT_EQ(qr.Count(), 0);
	for (int64_t i = 0; i < kCount; ++i) {
		Item item = NewItem(default_namespace);
		FillItem(item, i + 5);
		item[rtIndexName] = i;

		Upsert(default_namespace, item);
	}

	qr = rt.Select(q);
	ASSERT_EQ(qr.Count(), kCount);

	rt.DropIndex(default_namespace, rtIndexName);

	qr = rt.Select(q);
	ASSERT_EQ(qr.Count(), kCount);

	CheckSelectAll();
	CheckSelectByHashIndex();
	CheckSelectByTreeIndex();
}

TEST_F(SparseIndexesApi, RejectObjectOnInsert) {
	const auto test = [this](std::string_view json) {
		Item item = NewItem(default_namespace);
		const auto parseErr = item.FromJSON(json);
		ASSERT_FALSE(parseErr.ok());
		ASSERT_EQ(parseErr.code(), errLogic);
		ASSERT_THAT(parseErr.what(),
					testing::MatchesRegex("Error parsing json field \'(name|serialNumber)\' - unable to use object in this context"));
	};

	test(R"json({"id":"broken_tree","name":{"field":"some text"}})json");
	test(R"json({"id":"broken_hash","serialNumber":{"a":1,"b":1}})json");
}

TEST_F(SparseIndexesApi, RejectObjectOnUpdate) {
	QueryResults qr;
	auto err = rt.reindexer->Update(
		Query(default_namespace).Where(kFieldId, CondEq, Variant("key1")).SetObject(kFieldName, Variant{std::string{R"json({"x":1})json"}}),
		qr);
	ASSERT_FALSE(err.ok());
	ASSERT_TRUE(err.code() == errLogic || err.code() == errParams) << err.what();

	err = rt.reindexer->Update(Query(default_namespace)
								   .Where(kFieldId, CondEq, Variant("key2"))
								   .SetObject(kFieldSerialNumber, Variant{std::string{R"json({"x":2})json"}}),
							   qr);
	ASSERT_FALSE(err.ok());
	ASSERT_TRUE(err.code() == errLogic || err.code() == errParams) << err.what();
}

TEST_F(SparseIndexesApi, ObjectInsertIntoSparseTreeNotBreakBgOptimization) {
	Item brokenItem = NewItem(default_namespace);
	auto parseErr = brokenItem.FromJSON(R"json({"id":"broken_tree_regression","name":{"field":"some text"}})json");
	ASSERT_FALSE(parseErr.ok());
	ASSERT_EQ(parseErr.code(), errLogic);
	ASSERT_EQ(parseErr.whatStr(), "Error parsing json field 'name' - unable to use object in this context");

	Item validItem = NewItem(default_namespace);
	FillItem(validItem, 1000);
	validItem[kFieldId] = "valid_after_reject";
	validItem[kFieldName] = "name_after_reject";
	validItem[kFieldSerialNumber] = int64_t{1000};
	Upsert(default_namespace, validItem);

	rt.AwaitIndexOptimization(default_namespace);

	auto qr = rt.Select(Query(default_namespace).Where(kFieldId, CondEq, Variant("valid_after_reject")));
	ASSERT_EQ(qr.Count(), 1);

	qr = rt.Select(Query(default_namespace).Where(kFieldName, CondEq, Variant("name_after_reject")));
	ASSERT_EQ(qr.Count(), 1);

	qr = rt.Select(Query(default_namespace).Where(kFieldId, CondEq, Variant("broken_tree_regression")));
	ASSERT_EQ(qr.Count(), 0);
}
