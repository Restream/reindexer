#include "sparse_indexes_api.h"

TEST_F(SparseIndexesApi, SparseIndexSelectAll) { CheckSelectAll(); }
TEST_F(SparseIndexesApi, SelectByTreeSparseIndex) { CheckSelectByTreeIndex(); }
TEST_F(SparseIndexesApi, SelectByHashSparseIndex) { CheckSelectByHashIndex(); }

TEST_F(SparseIndexesApi, SparseIndexConsistencyWithRuntimeIndexes) {
	const char* rtIndexName = "rt1";
	Error err = rt.reindexer->AddIndex(default_namespace, {rtIndexName, "hash", "int64", IndexOpts()});
	EXPECT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(default_namespace);
	EXPECT_TRUE(err.ok()) << err.what();

	CheckSelectAll();

	// Not possible to test until appropriate implementation of Upsert()
	// for Sparse indexes: need SetByJsonValue() to set such fields.
	//    for (int64_t i = 0; i < 10; ++i) {
	//        Item item = NewItem(default_namespace);
	//        EXPECT_TRUE(item.Status().ok()) << item.Status().what();
	//        item[rtIndexName] = i;

	//        Upsert(default_namespace, item);
	//        Commit(default_namespace);
	//    }

	//    QueryResults qr;
	//    err = rt.reindexer->Select(Query(default_namespace).Where(rtIndexName, CondLt, Variant(static_cast<int64_t>(10))), qr);
	//    EXPECT_TRUE(err.ok()) << err.what();
	//    EXPECT_TRUE(qr.size() == 10);

	reindexer::IndexDef idef(rtIndexName);
	err = rt.reindexer->DropIndex(default_namespace, idef);
	EXPECT_TRUE(err.ok()) << err.what();

	CheckSelectAll();
	CheckSelectByHashIndex();
	CheckSelectByTreeIndex();
}
