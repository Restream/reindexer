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
