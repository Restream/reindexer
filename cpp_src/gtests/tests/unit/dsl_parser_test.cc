#include "core/query/impl.h"
#include "gtests/helpers.h"
#include "join_selects_api.h"

namespace reindexer_tests {

using reindexer::impl::Impl;

static void checkQueryDslParse(const reindexer::Query& q) {
	const std::string dsl = Impl{q}->GetJSON();
	Query parsedQuery = Query::FromJSON(dsl);
	ASSERT_EQ(q, parsedQuery) << "DSL:\n" << dsl << "\nOriginal query:\n" << *q.GetSQL() << "\nParsed query:\n" << *parsedQuery.GetSQL();
}

TEST_F(JoinSelectsApi, JoinsDSLTest) {
	Query queryGenres(genres_namespace);
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 500)};
	queryBooks.Or().InnerJoin(std::move(queryGenres), genreId_fk, CondEq, genreid);
	queryBooks.LeftJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid);
	checkQueryDslParse(queryBooks);
}

TEST_F(JoinSelectsApi, EqualPositionDSLTest) {
	Query query = Query(default_namespace);
	query.Where("f1", CondEq, 1).Where("f2", CondEq, 2).Or().Where("f3", CondEq, 2);
	query.EqualPositions({"f1", "f2"});
	query.EqualPositions({"f1", "f3"});
	query.OpenBracket().Where("f4", CondEq, 4).Where("f5", CondLt, 10);
	query.EqualPositions({"f4", "f5"});
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
	query.Aggregate(AggAvg, {price});
	query.Aggregate(AggSum, {pages});
	query.Aggregate(AggFacet, {title, pages}, {{{title, true}}}, 100, 10);
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, SelectFilterDSLTest) {
	auto query = Query(books_namespace, 10, 100).Where(pages, CondGe, 150).Select(price).Select(pages).Select(title);
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, SelectFilterInJoinDSLTest) {
	Query queryBooks = Query(books_namespace, 0, 10).Select(price).Select(title);
	{
		Query queryAuthors = Query(authors_namespace).Select(authorid).Select(age);

		queryBooks.LeftJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid);
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
	Query query{Query(books_namespace).WhereComposite(pagesBookidIndex, CondGe, {{Variant(500), Variant(10)}})};
	checkQueryDslParse(query);
}

TEST_F(JoinSelectsApi, GeneralDSLTest) {
	Query queryGenres(genres_namespace);
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 500)};
	Query innerJoinQuery = queryBooks.InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid);

	Query testDslQuery = innerJoinQuery.Or().InnerJoin(std::move(queryGenres), genreId_fk, CondEq, genreid);
	testDslQuery.Merge(std::move(queryBooks));
	testDslQuery.Merge(std::move(innerJoinQuery));
	testDslQuery.Select(genreid).Select(bookid).Select(authorid_fk);
	testDslQuery.AddFunction("f1()");
	testDslQuery.AddFunction("f2()");
	testDslQuery.Aggregate(AggDistinct, {bookid});

	checkQueryDslParse(testDslQuery);
}

TEST_F(JoinSelectsApi, DSL_SQLConvertionTest) {
	auto json = R"json({
		"namespace":"ns1",
		"type":"select",
		"select_filter":[
			"*",
			"vectors()"
		],
		"filters":[
			{
				"op":"NOT",
				"join_query":{
					"namespace":"ns2",
					"select_filter":[
						"*",
						"vectors()"
					],
					"type":"INNER",
					"on":[
						{
							"op":"NOT",
							"left_field":"lfield",
							"cond":"SET",
							"right_field":"rfield"
						}
					]
				}
			}
		],
		"sort":{
			"field":"ns2.respons",
			"desc":false
		},
		"limit":12
	})json";

	const auto testQueryFromDSL = QueryImpl::FromJSON(json);
	const auto sql = testQueryFromDSL.GetSQL();
	const Query testQueryFromSQL = Query::FromSQL(sql);
	ASSERT_EQ(sql, testQueryFromSQL.GetSQL()) << "SQL: " << sql;
	ASSERT_EQ("SELECT *, vectors() FROM ns1 WHERE NOT INNER JOIN ns2 ON NOT ns1.lfield IN ns2.rfield ORDER BY 'ns2.respons' LIMIT 12", sql);
}

}  // namespace reindexer_tests
