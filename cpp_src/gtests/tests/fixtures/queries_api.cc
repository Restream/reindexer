#include "queries_api.h"

void QueriesApi::CheckMergeQueriesWithLimit() {
	Query q = Query{default_namespace}.Merge(Query{joinNs}.Limit(1));
	QueryResults qr;
	Error err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Limit and offset in inner merge query is not allowed");

	q = Query{default_namespace}.Merge(Query{joinNs}.Offset(1));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Limit and offset in inner merge query is not allowed");

	q = Query{default_namespace}.Merge(Query{joinNs}.Sort(kFieldNameId, false));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in inner merge query is not allowed");  // TODO #1449

	q = Query{default_namespace}.Merge(Query{joinNs}).Sort(kFieldNameId, false);
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in merge query is not implemented yet");	 // TODO #1449

	q = Query{default_namespace}.Where(kFieldNameDescription, CondEq, RandString()).Merge(Query{joinNs});
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "In merge query without sorting all subqueries should be fulltext or not fulltext at the same time");

	q = Query{default_namespace}.Merge(Query{joinNs}.Where(kFieldNameDescription, CondEq, RandString()));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "In merge query without sorting all subqueries should be fulltext or not fulltext at the same time");

	q = Query{default_namespace}.Where(kFieldNameDescription, CondEq, RandString()).Merge(Query{joinNs}).Sort(kFieldNameId, false);
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in merge query is not implemented yet");	 // TODO #1449

	q = Query{default_namespace}.Merge(Query{joinNs}.Where(kFieldNameDescription, CondEq, RandString())).Sort(kFieldNameId, false);
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	EXPECT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Sorting in merge query is not implemented yet");	 // TODO #1449

	qr.Clear();
	q = Query{default_namespace}.Merge(Query{joinNs}).Limit(10);
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 10);
	EXPECT_EQ(qr.getMergedNSCount(), 2);

	qr.Clear();
	q = Query{default_namespace}.Merge(Query{joinNs}).Offset(10);
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.getMergedNSCount(), 2);

	q = Query{default_namespace}
			.Where(kFieldNameDescription, CondEq, RandString())
			.Merge(Query{joinNs}.Where(kFieldNameDescription, CondEq, RandString()));
	qr.Clear();
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.getMergedNSCount(), 2);
}
