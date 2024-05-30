#include "sparse_indexes_api.h"

TEST_F(SparseIndexesApi, SparseIndexSelectAll) { CheckSelectAll(); }
TEST_F(SparseIndexesApi, SelectByTreeSparseIndex) { CheckSelectByTreeIndex(); }
TEST_F(SparseIndexesApi, SelectByHashSparseIndex) { CheckSelectByHashIndex(); }

TEST_F(SparseIndexesApi, SparseIndexConsistencyWithRuntimeIndexes) {
	const char* rtIndexName = "rt1";
	Error err = rt.reindexer->AddIndex(default_namespace, {rtIndexName, "tree", "int64", IndexOpts().Sparse()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	CheckSelectAll();

	constexpr int64_t kCount = 10;
	const Query q = Query(default_namespace).Where(rtIndexName, CondLt, Variant(kCount));
	QueryResults qr;
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 0);
	for (int64_t i = 0; i < kCount; ++i) {
		Item item = NewItem(default_namespace);
		EXPECT_TRUE(item.Status().ok()) << item.Status().what();
		FillItem(item, i + 5);
		item[rtIndexName] = i;

		Upsert(default_namespace, item);
	}
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), kCount);

	reindexer::IndexDef idef(rtIndexName);
	err = rt.reindexer->DropIndex(default_namespace, idef);
	ASSERT_TRUE(err.ok()) << err.what();

	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), kCount);

	CheckSelectAll();
	CheckSelectByHashIndex();
	CheckSelectByTreeIndex();
}
