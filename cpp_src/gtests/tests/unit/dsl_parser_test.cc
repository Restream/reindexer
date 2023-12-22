#include "join_selects_api.h"

static void checkQueryDslParse(const reindexer::Query& q) {
	const std::string dsl = q.GetJSON();
	Query parsedQuery;
	Error err = parsedQuery.FromJSON(dsl);
	ASSERT_TRUE(err.ok()) << err.what() << "\nDSL:\n" << dsl;
	ASSERT_EQ(q, parsedQuery) << "DSL:\n" << dsl << "\nOriginal query:\n" << q.GetSQL() << "\nParsed query:\n" << parsedQuery.GetSQL();
}

TEST_F(JoinSelectsApi, JoinsDSLTest) {
	Query queryGenres(genres_namespace);
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 500)};
	queryBooks.OrInnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres));
	queryBooks.LeftJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors));
	checkQueryDslParse(queryBooks);
}

TEST_F(JoinSelectsApi, EqualPositionDSLTest) {
	Query query = Query(default_namespace);
	query.Where("f1", CondEq, 1).Where("f2", CondEq, 2).Or().Where("f3", CondEq, 2);
	query.AddEqualPosition({"f1", "f2"});
	query.AddEqualPosition({"f1", "f3"});
	query.OpenBracket().Where("f4", CondEq, 4).Where("f5", CondLt, 10);
	query.AddEqualPosition({"f4", "f5"});
	query.CloseBracket();
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, MergedQueriesDSLTest) {
	Query mainBooksQuery{Query(books_namespace, 0, 10).Where(price, CondGe, 500)};
	Query firstMergedQuery{Query(books_namespace, 10, 100).Where(pages, CondLe, 250)};
	Query secondMergedQuery{Query(books_namespace, 100, 50).Where(bookid, CondGe, 100)};

	mainBooksQuery.Merge(std::move(firstMergedQuery));
	mainBooksQuery.Merge(std::move(secondMergedQuery));
	checkQueryDslParse(mainBooksQuery);
}

TEST_F(JoinSelectsApi, AggregateFunctonsDSLTest) {
	Query query{Query(books_namespace, 10, 100).Where(pages, CondGe, 150)};
	query.aggregations_.push_back({AggAvg, {price}});
	query.aggregations_.push_back({AggSum, {pages}});
	query.aggregations_.push_back({AggFacet, {title, pages}, {{{title, true}}}, 100, 10});
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, SelectFilterDSLTest) {
	Query query{Query(books_namespace, 10, 100).Where(pages, CondGe, 150).Select({price, pages, title})};
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, SelectFilterInJoinDSLTest) {
	Query queryBooks = Query(books_namespace, 0, 10).Select({price, title});
	{
		Query queryAuthors = Query(authors_namespace).Select({authorid, age});

		queryBooks.LeftJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors));
	}
	checkQueryDslParse(queryBooks);
}

TEST_F(JoinSelectsApi, ReqTotalDSLTest) {
	Query query{Query(books_namespace, 10, 100, ModeNoTotal).Where(pages, CondGe, 150)};
	checkQueryDslParse(query);

	query.CachedTotal();
	checkQueryDslParse(query);

	query.ReqTotal();
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, SelectFunctionsDSLTest) {
	Query query{Query(books_namespace, 10, 100).Where(pages, CondGe, 150)};
	query.AddFunction("f1()");
	query.AddFunction("f2()");
	query.AddFunction("f3()");
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, CompositeValuesDSLTest) {
	std::string pagesBookidIndex = pages + std::string("+") + bookid;
	Query query{Query(books_namespace).WhereComposite(pagesBookidIndex.c_str(), CondGe, {{Variant(500), Variant(10)}})};
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, GeneralDSLTest) {
	Query queryGenres(genres_namespace);
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 500)};
	Query innerJoinQuery = queryBooks.InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors));

	Query testDslQuery = innerJoinQuery.OrInnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres));
	testDslQuery.Merge(std::move(queryBooks));
	testDslQuery.Merge(std::move(innerJoinQuery));
	testDslQuery.Select({genreid, bookid, authorid_fk});
	testDslQuery.AddFunction("f1()");
	testDslQuery.AddFunction("f2()");
	testDslQuery.aggregations_.push_back({AggDistinct, {bookid}});

	checkQueryDslParse(testDslQuery);
}
