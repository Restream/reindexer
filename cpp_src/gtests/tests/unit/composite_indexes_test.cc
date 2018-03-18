#include "composite_indexes_api.h"

TEST_F(CompositeIndexesApi, CompositeIndexesAddTest) {
	fillNamespace(0, 100);
	addIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePages}), CompositeIndexHash, IndexOpts());
	fillNamespace(101, 200);
	addIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePrice}), CompositeIndexBTree, IndexOpts());
	fillNamespace(201, 300);
	addIndex(getCompositeIndexName({kFieldNameBookid, kFieldNameBookid2}), CompositeIndexHash, IndexOpts().PK());
	fillNamespace(301, 400);
}

TEST_F(CompositeIndexesApi, CompositeIndexesDropTest) {
	fillNamespace(0, 100);
	addIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePages}), CompositeIndexHash, IndexOpts());
	fillNamespace(101, 200);
	addIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePrice}), CompositeIndexBTree, IndexOpts());
	fillNamespace(201, 300);
	addIndex(getCompositeIndexName({kFieldNameBookid, kFieldNameBookid2}), CompositeIndexHash, IndexOpts().PK());
	fillNamespace(301, 400);

	dropIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePrice}));
	fillNamespace(401, 500);
	dropIndex(getCompositeIndexName({kFieldNameBookid, kFieldNameBookid2}));
	fillNamespace(501, 600);
	dropIndex(getCompositeIndexName({kFieldNameTitle, kFieldNamePages}));
	fillNamespace(601, 700);
}

TEST_F(CompositeIndexesApi, CompositeIndexesSelectTest) {
	int priceValue = 77777, pagesValue = 88888;
	const char* titleValue = "test book1 title";
	const char* nameValue = "test book1 name";

	fillNamespace(0, 100);

	string compositeIndexName(getCompositeIndexName({kFieldNamePrice, kFieldNamePages}));
	addIndex(compositeIndexName, CompositeIndexHash, IndexOpts());

	addOneRow(300, 3000, titleValue, pagesValue, priceValue, nameValue);
	fillNamespace(101, 200);

	QueryResults qr;
	Error err = reindexer->Select(
		Query(default_namespace).WhereComposite(compositeIndexName.c_str(), CondEq, {{KeyValue(priceValue), KeyValue(pagesValue)}}), qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.size() == 1);

	Item pricePageRow = qr.GetItem(0);
	KeyRef selectedPrice = pricePageRow[kFieldNamePrice];
	KeyRef selectedPages = pricePageRow[kFieldNamePages];
	EXPECT_EQ(static_cast<int>(selectedPrice), priceValue);
	EXPECT_EQ(static_cast<int>(selectedPages), pagesValue);

	QueryResults qr1;
	err = reindexer->Select(
		Query(default_namespace).WhereComposite(compositeIndexName.c_str(), CondLt, {{KeyValue(priceValue), KeyValue(pagesValue)}}), qr1);
	EXPECT_TRUE(err.ok()) << err.what();

	QueryResults qr2;
	err = reindexer->Select(
		Query(default_namespace).WhereComposite(compositeIndexName.c_str(), CondGt, {{KeyValue(priceValue), KeyValue(pagesValue)}}), qr2);
	EXPECT_TRUE(err.ok()) << err.what();

	QueryResults qr3;
	err = reindexer->Select(
		Query(default_namespace).WhereComposite(compositeIndexName.c_str(), CondGt, {{KeyValue(priceValue), KeyValue(pagesValue)}}), qr3);
	EXPECT_TRUE(err.ok()) << err.what();

	fillNamespace(301, 400);

	QueryResults qr4;
	err = reindexer->Select(Query(default_namespace)
								.WhereComposite(compositeIndexName.c_str(), CondRange,
												{{KeyValue(1), KeyValue(1)}, {KeyValue(priceValue), KeyValue(pagesValue)}}),
							qr4);
	EXPECT_TRUE(err.ok()) << err.what();

	dropIndex(compositeIndexName);
	fillNamespace(401, 500);

	string compositeIndexName2(getCompositeIndexName({kFieldNameTitle, kFieldNameName}));
	addIndex(compositeIndexName2, CompositeIndexBTree, IndexOpts());

	QueryResults qr5;
	err = reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compositeIndexName2.c_str(), CondEq, {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}),
		qr5);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr5.size() == 1);

	Item titleNameRow = qr.GetItem(0);
	KeyRef selectedTitle = titleNameRow[kFieldNameTitle];
	KeyRef selectedName = titleNameRow[kFieldNameName];
	EXPECT_TRUE(static_cast<reindexer::key_string>(selectedTitle)->compare(string(titleValue)) == 0);
	EXPECT_TRUE(static_cast<reindexer::key_string>(selectedName)->compare(string(nameValue)) == 0);

	QueryResults qr6;
	err = reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compositeIndexName2.c_str(), CondGe, {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}),
		qr6);
	EXPECT_TRUE(err.ok()) << err.what();

	QueryResults qr7;
	err = reindexer->Select(
		Query(default_namespace)
			.WhereComposite(compositeIndexName2.c_str(), CondLt, {{KeyValue(string(titleValue)), KeyValue(string(nameValue))}}),
		qr7);
	EXPECT_TRUE(err.ok()) << err.what();

	dropIndex(compositeIndexName2);
	fillNamespace(201, 300);

	QueryResults qr8;
	err = reindexer->Select(Query(default_namespace), qr8);
	EXPECT_TRUE(err.ok()) << err.what();
}
