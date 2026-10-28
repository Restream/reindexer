#include <thread>
#include <unordered_set>
#include "core/nsselecter/joins/item_context.h"
#include "core/nsselecter/joins/iterators.h"
#include "core/nsselecter/joins/preselect.h"
#include "core/query/query_impl.h"
#include "core/queryresults/localqueryresults.h"
#include "join_on_conditions_api.h"
#include "test_helpers.h"

namespace reindexer_tests {

using reindexer::IndexOpts;
using reindexer::LocalQueryResults;

TEST_F(JoinSelectsApi, JoinsAsWhereConditionsTest) {
	Query queryGenres{Query(genres_namespace).Not().Where(genreid, CondEq, 1)};
	Query queryAuthors{Query(authors_namespace).Where(authorid, CondGe, 10).Where(authorid, CondLe, 25)};
	Query queryAuthors2{Query(authors_namespace).Where(authorid, CondGe, 300).Where(authorid, CondLe, 400)};
	// clang-format off
	Query queryBooks{Query(books_namespace).Limit(50)
						 .OpenBracket()
							 .Where(price, CondGe, 9540)
							 .Where(price, CondLe, 9550)
						 .CloseBracket()
						 .Or().OpenBracket()
							 .Where(price, CondGe, 1000)
							 .Where(price, CondLe, 2000)
							 .InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid)
							 .Or().InnerJoin(std::move(queryGenres), genreId_fk, CondEq, genreid )
						 .CloseBracket()
						 .Or().OpenBracket()
							 .Where(pages, CondEq, 0)
						 .CloseBracket()
						 .Or().InnerJoin(std::move(queryAuthors2), authorid_fk, CondEq, authorid )};
	// clang-format on

	QueryWatcher watcher{queryBooks};
	auto qr = rt.Select(queryBooks);
	EXPECT_LE(qr.Count(), 50);
	CheckJoinsInComplexWhereCondition(qr);
}

TEST_F(JoinSelectsApi, JoinsLockWithCache_364) {
	Query queryGenres{Query(genres_namespace).Where(genreid, CondEq, 1)};
	Query queryBooks{Query(books_namespace).Limit(50).InnerJoin(std::move(queryGenres), genreId_fk, CondEq, genreid)};
	QueryWatcher watcher{queryBooks};
	TurnOnJoinCache(genres_namespace);

	for (int i = 0; i < 10; ++i) {
		SCOPED_TRACE(std::to_string(i));
		std::ignore = rt.Select(queryBooks);
	}
}

TEST_F(JoinSelectsApi, ArithmeticInJoinPreselect) {
	auto joinedQuery = Query(authors_namespace)
						   .Where(reindexer::expressions::ArithmeticExpression(std::string(authorid) + "+1"), CondGe,
								  reindexer::VariantArray{Variant{11}});
	auto query = Query(books_namespace).InnerJoin(std::move(joinedQuery), authorid_fk, CondEq, authorid);

	EXPECT_GT(rt.Select(query).Count(), 0);
}

TEST_F(JoinSelectsApi, ArithmeticInJoinPreselectWithCache) {
	TurnOnJoinCache(authors_namespace);

	const auto makeQuery = [this] {
		return Query(books_namespace)
			.InnerJoin(Query(authors_namespace)
						   .Where(reindexer::expressions::ArithmeticExpression(std::string(authorid) + "+1"), CondGe,
								  reindexer::VariantArray{Variant{11}}),
					   authorid_fk, CondEq, authorid);
	};

	const auto expectedCount = rt.Select(makeQuery()).Count();
	ASSERT_GT(expectedCount, 0);
	for (int i = 0; i < 10; ++i) {
		SCOPED_TRACE(std::to_string(i));
		EXPECT_EQ(rt.Select(makeQuery()).Count(), expectedCount);
	}
}

TEST_F(JoinSelectsApi, ArithmeticNowBypassesJoinCache) {
	for (const auto* mode : {"on", "aggressive"}) {
		SCOPED_TRACE(mode);
		TurnOnJoinCache(authors_namespace, mode);
		AwaitIndexOptimization(authors_namespace);

		const Query query = Query(books_namespace)
								.Offset(0)
								.Limit(100)
								.InnerJoin(Query(authors_namespace)
											   .Where(reindexer::expressions::ArithmeticExpression("now(nsec)"), CondGt,
													  reindexer::VariantArray{Variant{0}}),
										   authorid_fk, CondEq, authorid);
		const auto before = getMemStat(*rt.reindexer, authors_namespace)["join_cache.items_count"].As<int64_t>();

		for (int i = 0; i < 5; ++i) {
			ASSERT_GT(rt.Select(query).Count(), 0);
		}
		const auto after = getMemStat(*rt.reindexer, authors_namespace)["join_cache.items_count"].As<int64_t>();
		EXPECT_EQ(after, before);
	}
}

TEST_F(JoinSelectsApi, JoinsAsWhereConditionsTest2) {
	std::string sql =
		"SELECT * FROM books_namespace WHERE "
		"(price >= 9540 AND price <= 9550) "
		"OR (price >= 1000 AND price <= 2000 INNER JOIN (SELECT * FROM authors_namespace WHERE authorid >= 10 AND authorid <= 25)ON "
		"authors_namespace.authorid = books_namespace.authorid_fk OR INNER JOIN (SELECT * FROM genres_namespace WHERE NOT genreid = 1) ON "
		"genres_namespace.genreid = books_namespace.genreid_fk) "
		"OR (pages = 0) "
		"OR INNER JOIN (SELECT *FROM authors_namespace WHERE authorid >= 300 AND authorid <= 400) ON authors_namespace.authorid = "
		"books_namespace.authorid_fk LIMIT 50";

	Query query = Query::FromSQL(sql);
	QueryWatcher watcher{query};
	auto qr = rt.Select(query);
	EXPECT_LE(qr.Count(), 50);
	CheckJoinsInComplexWhereCondition(qr);
}

TEST_F(JoinSelectsApi, JoinsAsWhereNotConditionsTest) {
	std::string sql =
		"SELECT * FROM books_namespace WHERE NOT INNER JOIN (SELECT * FROM authors_namespace) ON authors_namespace.authorid = "
		"books_namespace.authorid_fk";

	auto query = Query::FromSQL(sql);
	QueryWatcher watcher{query};
	auto qr = rt.Select(query);
	EXPECT_EQ(qr.Count(), 0);
}

TEST_F(JoinSelectsApi, JoinsAsWhereNotSQLConditionsTest) {
	std::string sql =
		"SELECT * FROM books_namespace WHERE NOT INNER JOIN (SELECT * FROM authors_namespace) ON authors_namespace.authorid = "
		"books_namespace.authorid_fk";

	auto query = Query::FromSQL(sql);
	auto q = Query(books_namespace).Not().InnerJoin(Query(authors_namespace), "authorid_fk", CondEq, "authorid");
	EXPECT_EQ(query, q);
}

TEST_F(JoinSelectsApi, JoinsNotConditionsNegativeTest) {
	try {
		std::string sql =
			"SELECT * FROM books_namespace NOT INNER JOIN (SELECT * FROM authors_namespace) ON authors_namespace.authorid = "
			"books_namespace.authorid_fk";
		auto query = Query::FromSQL(sql);
	} catch (const Error& err) {
		EXPECT_STREQ(err.what(), "Unexpected 'not' in query, line: 1 column: 30 33");
	}
}

TEST_F(JoinSelectsApi, JoinsNotConditionsBracketsNegativeTest) {
	try {
		std::string sql =
			"SELECT * FROM books_namespace (NOT INNER JOIN (SELECT * FROM authors_namespace) ON authors_namespace.authorid = "
			"books_namespace.authorid_fk)";
		auto query = Query::FromSQL(sql);
	} catch (const Error& err) {
		EXPECT_STREQ(err.what(), "Unexpected '(' in query, line: 1 column: 30 31");
	}
}

TEST_F(JoinSelectsApi, SqlParsingTest) {
	constexpr std::string_view sql =
		"select * from books_namespace where (pages > 0 and inner join (select * from authors_namespace limit 10) on "
		"authors_namespace.authorid = "
		"books_namespace.authorid_fk and price > 1000 or inner join (select * from genres_namespace limit 10) on "
		"genres_namespace.genreid = books_namespace.genreid_fk and pages < 10000 and inner join (select * from authors_namespace WHERE "
		"(authorid >= 10 AND authorid <= 20) limit 100) on "
		"authors_namespace.authorid = books_namespace.authorid_fk) or pages == 3 limit 20";

	auto srcQuery = Query::FromSQL(sql);
	QueryWatcher watcher{srcQuery};

	reindexer::WrSerializer wrser;
	Impl(srcQuery).GetSQL(wrser);

	auto dstQuery = Query::FromSQL(wrser.Slice());
	ASSERT_EQ(srcQuery, dstQuery);

	wrser.Reset();
	BindingCapabilities caps{kBindingCapabilityQrIdleTimeouts | kBindingCapabilityResultsWithShardIDs | kBindingCapabilityIncarnationTags |
							 kBindingCapabilityComplexRank | kBindingCapabilityQueryFormatV2};
	Impl(srcQuery).Serialize(wrser, Normal, caps.GetQueryFormat());
	reindexer::Serializer ser(wrser.Buf(), wrser.Len());
	Query deserializedQuery1 = QueryImpl::Deserialize(ser, caps.GetQueryFormat());
	ASSERT_EQ(srcQuery, deserializedQuery1) << "Original query:\n"
											<< srcQuery.GetSQL() << "\nDeserialized query:\n"
											<< deserializedQuery1.GetSQL();

	const auto json = srcQuery.GetJSON();
	auto deserializedQuery2 = Query::FromJSON(json);
	ASSERT_EQ(srcQuery, deserializedQuery2) << "Original query:\n"
											<< srcQuery.GetSQL() << "\nDeserialized query:\n"
											<< deserializedQuery2.GetSQL();
}

TEST_F(JoinSelectsApi, InnerJoinTest) {
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace).Limit(10).Where(price, CondGe, 600)};
	Query joinQuery = queryBooks.InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid);
	QueryWatcher watcher{joinQuery};

	auto joinQueryRes = rt.Select(joinQuery);

	auto err = VerifyResJSON(joinQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	auto pureSelectRes = rt.Select(queryBooks);

	QueryResultRows joinSelectRows;
	QueryResultRows pureSelectRows;

	for (auto it : pureSelectRes) {
		Item booksItem(it.GetItem(false));
		Variant authorIdKeyRef = booksItem[authorid_fk];

		Query authorsQuery{Query(authors_namespace).Where(authorid, CondEq, authorIdKeyRef)};
		auto authorsSelectRes = rt.Select(authorsQuery);

		int bookId = booksItem[bookid].Get<int>();
		QueryResultRow& pureSelectRow = pureSelectRows[bookId];

		FillQueryResultFromItem(booksItem, pureSelectRow);
		for (auto jit : authorsSelectRes) {
			Item authorsItem(jit.GetItem(false));
			FillQueryResultFromItem(authorsItem, pureSelectRow);
		}
	}

	FillQueryResultRows(joinQueryRes, joinSelectRows);
	EXPECT_EQ(CompareQueriesResults(pureSelectRows, joinSelectRows), true);
}

TEST_F(JoinSelectsApi, InnerJoinWithNestedJoinTest) {
	auto queryLocations = Query{location_namespace}
							  .Limit(100)
							  .Not()
							  .Where(code, CondEq, 13)
							  .InnerJoin(Query{countries_namespace}, countryid_fk, CondEq, countryid);

	auto queryAuthors = Query{authors_namespace}
							.Limit(100)
							.InnerJoin(Query{queryLocations}, locationid_fk, CondEq, locationid)
							.InnerJoin(Query{location_namespace}.Limit(100).Where(code, CondGe, 1), locationid_fk, CondEq, locationid);

	auto queryBooks = Query(books_namespace)
						  .Limit(50)
						  .Where(price, CondGe, 2)
						  .InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid)
						  .Merge(Query{authors_namespace})
						  .Merge(Query{books_namespace})
						  .Merge(Query{location_namespace})
						  .Merge(Query{location_namespace}.InnerJoin(Query{countries_namespace}, countryid_fk, CondEq, countryid))
						  .Merge(Query{countries_namespace});
	QueryWatcher watcher{queryBooks};

	const int mergedSize{static_cast<int>(Impl(queryBooks).MergeQueries().size())};
	reindexer::joins::QueryJoinsTable joinsInfo{Impl(queryBooks)};
	const int booksNsId{0};
	const int authorsNsId{joinsInfo.GetJoinedNsId(booksNsId, 0)};
	const int locations1NsId{joinsInfo.GetJoinedNsId(authorsNsId, 0)};
	const int locations2NsId{joinsInfo.GetJoinedNsId(authorsNsId, 1)};
	const int countryNsId{joinsInfo.GetJoinedNsId(locations1NsId, 0)};
	ASSERT_TRUE(authorsNsId == 1 + mergedSize);
	ASSERT_TRUE(locations1NsId == 2 + mergedSize);
	ASSERT_TRUE(countryNsId == 3 + mergedSize);
	ASSERT_TRUE(locations2NsId == 4 + mergedSize);

	auto joinQr{rt.Select(queryBooks)};
	auto err{VerifyResJSON(joinQr)};
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(joinQr.Count() > 0);

	for (auto bookIt : joinQr) {
		auto bookJson{bookIt.GetJSON()};
		gason::JsonParser parser;
		// std::cout << bookJson.value() << std::endl;
		ASSERT_NO_THROW(parser.Parse(reindexer::giftStr(bookJson.value())));

		Item bookItem(bookIt.GetItem(false));
		Variant authorId{bookItem[authorid_fk]};

		ASSERT_GE(Variant{bookItem[price]}.As<int>(), 2);
		auto booksQr{rt.Select(Query{books_namespace}.Limit(50).Where(price, CondGe, 2).Where(bookid, CondEq, Variant(bookItem[bookid])))};
		ASSERT_TRUE(booksQr.Count() > 0);

		auto joinedAuthorsCtx{bookIt.GetJoinedContext()};
		auto& joinedAuthorsIt{joinedAuthorsCtx.iterator};
		ASSERT_TRUE(joinedAuthorsIt.GetFieldsCount() == 1);

		for (auto authorsField = joinedAuthorsIt.Begin(); authorsField != joinedAuthorsIt.End(); ++authorsField) {
			ASSERT_TRUE(authorsField.ItemsCount() == 1);

			auto authorItem{authorsField.GetItem(0, joinQr.GetPayloadType(authorsNsId), joinQr.GetTagsMatcher(authorsNsId))};
			ASSERT_TRUE(Variant(authorItem.GetField(authorItem.FieldIndex(authorid))) == authorId);

			Variant locationId{authorItem.GetField(authorItem.FieldIndex(locationid_fk))};

			auto authorsJoinQr{authorsField.ToQueryResults(joinedAuthorsCtx)};
			ASSERT_TRUE(authorsJoinQr.Count() == 1);

			for (auto& authorResultIt : authorsJoinQr) {
				ASSERT_TRUE(authorResultIt.GetItemRef().Nsid() == authorsNsId);

				auto joinedLocationsCtx{authorResultIt.GetJoinedContext()};
				auto& joinedLocationsIt{joinedLocationsCtx.iterator};
				ASSERT_TRUE(joinedLocationsIt.GetFieldsCount() == 2);

				int joinedLocationQueryIndex{0};
				for (auto locationsField = joinedLocationsIt.Begin(); locationsField != joinedLocationsIt.End();
					 ++locationsField, ++joinedLocationQueryIndex) {
					ASSERT_TRUE(locationsField.ItemsCount() == 1);

					const int locationsNsId{(joinedLocationQueryIndex == 0) ? locations1NsId : locations2NsId};

					auto locationItem{
						locationsField.GetItem(0, joinQr.GetPayloadType(locationsNsId), joinQr.GetTagsMatcher(locationsNsId))};
					ASSERT_TRUE(Variant(locationItem.GetField(locationItem.FieldIndex(locationid))) == locationId);

					auto locationsJoinQr{locationsField.ToQueryResults(joinedLocationsCtx)};
					ASSERT_EQ(locationsJoinQr.Count(), 1);

					for (auto& locationResultIt : locationsJoinQr) {
						ASSERT_TRUE(locationResultIt.GetItemRef().Nsid() == locationsNsId);

						auto locationItem{locationResultIt.GetItem()};
						Variant countryId{locationItem[countryid_fk]};

						auto joinedCountriesCtx{locationResultIt.GetJoinedContext()};
						auto& joinedCountriesIt{joinedCountriesCtx.iterator};
						ASSERT_EQ(joinedCountriesIt.GetFieldsCount(), (locationsNsId == locations1NsId) ? 1 : 0);

						for (auto countriesField = joinedCountriesIt.Begin(); countriesField != joinedCountriesIt.End(); ++countriesField) {
							ASSERT_TRUE(countriesField.ItemsCount() == 1);

							auto countryItem{
								countriesField.GetItem(0, joinQr.GetPayloadType(countryNsId), joinQr.GetTagsMatcher(countryNsId))};

							ASSERT_TRUE(Variant(countryItem.GetField(countryItem.FieldIndex(countryid))) == countryId);

							auto countriesJoinQr{countriesField.ToQueryResults(joinedCountriesCtx)};
							ASSERT_TRUE(countriesJoinQr.Count() == 1);
							for (const auto& countriesResultIt : countriesJoinQr) {
								ASSERT_TRUE(countriesResultIt.GetItemRef().Nsid() == countryNsId);
								auto joinedCtx{countriesResultIt.GetJoinedContext()};
								auto& joinedIt{joinedCtx.iterator};
								ASSERT_TRUE(joinedIt.GetFieldsCount() == 0);
								const auto joinedItemsCount{joinedIt.GetItemsCount()};
								ASSERT_TRUE(joinedItemsCount == 0);
							}
						}
					}
				}
			}
		}

		// Verify against original queries
		auto authorsOriginalQr{rt.Select(Query(authors_namespace).Where(authorid, CondEq, authorId))};
		ASSERT_TRUE(authorsOriginalQr.Count() == 1);
		for (auto authorOriginalIt : authorsOriginalQr) {
			auto authorOriginalItem{authorOriginalIt.GetItem(false)};
			ASSERT_TRUE(Variant(authorOriginalItem[authorid]) == authorId);

			Variant locationIdFromAuthor{authorOriginalItem[locationid_fk]};
			auto locationsOriginalQr{
				rt.Select(Query(location_namespace).Where(locationid, CondEq, locationIdFromAuthor).Not().Where(code, CondEq, 13))};
			ASSERT_TRUE(locationsOriginalQr.Count() == 1);

			for (auto locationOriginalIt : locationsOriginalQr) {
				auto locationOriginalItem{locationOriginalIt.GetItem(false)};
				ASSERT_TRUE(Variant(locationOriginalItem[locationid]) == locationIdFromAuthor);

				Variant countryId{locationOriginalItem[countryid_fk]};
				auto countriesOriginalQr{rt.Select(Query(countries_namespace).Where(countryid, CondEq, countryId))};
				ASSERT_TRUE(countriesOriginalQr.Count() == 1);
			}
		}
	}
}

TEST_F(JoinSelectsApi, InnerJoinWithSubqueryInJoinedQueryTest) {
	constexpr int kAuthorId = 990001;
	constexpr int kBookId = 990002;
	constexpr int kAge = 73;

	{
		Item authorItem = NewItem(authors_namespace);
		authorItem[authorid] = kAuthorId;
		authorItem[name] = "Joined Subquery Author";
		authorItem[age] = kAge;
		authorItem[locationid_fk] = 0;
		Upsert(authors_namespace, authorItem);
	}
	{
		Item bookItem = NewItem(books_namespace);
		bookItem[bookid] = kBookId;
		bookItem[title] = "Joined Subquery Book";
		bookItem[pages] = 321;
		bookItem[price] = 1234;
		bookItem[genreId_fk] = 4;
		bookItem[authorid_fk] = kAuthorId;
		Upsert(books_namespace, bookItem);
	}

	Query authorsSubQuery = Query(authors_namespace).Select(authorid).Where(age, CondEq, kAge);
	Query authorsQuery = Query(authors_namespace).Where(authorid, CondSet, std::move(authorsSubQuery));
	Query joinQuery =
		Query(books_namespace).Where(bookid, CondEq, kBookId).InnerJoin(std::move(authorsQuery), authorid_fk, CondEq, authorid);
	QueryWatcher watcher{joinQuery};

	auto qr = rt.Select(joinQuery);
	auto err = VerifyResJSON(qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	auto rowIt = qr.begin();
	Item bookItem = rowIt.GetItem(false);
	ASSERT_EQ(bookItem[authorid_fk].As<int>(), kAuthorId);

	auto joinedCtx = rowIt.GetJoinedContext();
	auto& joinedIt = joinedCtx.iterator;
	ASSERT_EQ(joinedIt.GetFieldsCount(), 1);
	auto authorsField = joinedIt.Begin();
	ASSERT_EQ(authorsField.ItemsCount(), 1);

	auto authorItem = authorsField.ToQueryResults(joinedCtx)[0].GetItem();
	ASSERT_EQ(authorItem[authorid].As<int>(), kAuthorId);
	ASSERT_EQ(authorItem[age].As<int>(), kAge);
}

TEST_F(JoinSelectsApi, LeftJoinWithSubqueryInJoinedQueryTest) {
	constexpr int kAuthorId = 990101;
	constexpr int kBookId = 990102;
	constexpr int kPrice = 2345;

	{
		Item authorItem = NewItem(authors_namespace);
		authorItem[authorid] = kAuthorId;
		authorItem[name] = "Left Joined Subquery Author";
		authorItem[age] = 71;
		authorItem[locationid_fk] = 0;
		Upsert(authors_namespace, authorItem);
	}
	{
		Item bookItem = NewItem(books_namespace);
		bookItem[bookid] = kBookId;
		bookItem[title] = "Left Joined Subquery Book";
		bookItem[pages] = 123;
		bookItem[price] = kPrice;
		bookItem[genreId_fk] = 4;
		bookItem[authorid_fk] = kAuthorId;
		Upsert(books_namespace, bookItem);
	}

	Query booksSubQuery = Query(books_namespace).Select(bookid).Where(price, CondEq, kPrice);
	Query booksQuery = Query(books_namespace).Where(bookid, CondSet, std::move(booksSubQuery));
	Query joinQuery =
		Query(authors_namespace).Where(authorid, CondEq, kAuthorId).LeftJoin(std::move(booksQuery), authorid, CondEq, authorid_fk);
	QueryWatcher watcher{joinQuery};

	auto qr = rt.Select(joinQuery);
	auto err = VerifyResJSON(qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	auto rowIt = qr.begin();
	Item authorItem = rowIt.GetItem(false);
	ASSERT_EQ(authorItem[authorid].As<int>(), kAuthorId);

	auto joinedCtx = rowIt.GetJoinedContext();
	auto& joinedIt = joinedCtx.iterator;
	ASSERT_EQ(joinedIt.GetFieldsCount(), 1);
	auto booksField = joinedIt.Begin();
	ASSERT_EQ(booksField.ItemsCount(), 1);

	auto bookItem = booksField.ToQueryResults(joinedCtx)[0].GetItem();
	ASSERT_EQ(bookItem[bookid].As<int>(), kBookId);
	ASSERT_EQ(bookItem[price].As<int>(), kPrice);
}

TEST_F(JoinSelectsApi, InnerJoinWithSubqueriesInNestedJoinedQueriesTest) {
	constexpr int kCountryId = 991001;
	constexpr int kCountryCode = 991002;
	constexpr int kLocationId = 991003;
	constexpr int kLocationCode = 991004;
	constexpr int kAuthorId = 991005;
	constexpr int kBookId = 991006;

	{
		Item countryItem = NewItem(countries_namespace);
		countryItem[countryid] = kCountryId;
		countryItem[countryName] = "Subquery Country";
		countryItem[countryCode] = kCountryCode;
		Upsert(countries_namespace, countryItem);
	}
	{
		Item locationItem = NewItem(location_namespace);
		locationItem[locationid] = kLocationId;
		locationItem[countryid_fk] = kCountryId;
		locationItem[code] = kLocationCode;
		locationItem[city] = "Subquery City";
		Upsert(location_namespace, locationItem);
	}
	{
		Item authorItem = NewItem(authors_namespace);
		authorItem[authorid] = kAuthorId;
		authorItem[name] = "Nested Joined Subquery Author";
		authorItem[age] = 67;
		authorItem[locationid_fk] = kLocationId;
		Upsert(authors_namespace, authorItem);
	}
	{
		Item bookItem = NewItem(books_namespace);
		bookItem[bookid] = kBookId;
		bookItem[title] = "Nested Joined Subquery Book";
		bookItem[pages] = 654;
		bookItem[price] = 4321;
		bookItem[genreId_fk] = 4;
		bookItem[authorid_fk] = kAuthorId;
		Upsert(books_namespace, bookItem);
	}

	Query countrySubQuery = Query(countries_namespace).Select(countryid).Where(countryCode, CondEq, kCountryCode);
	Query countriesQuery = Query(countries_namespace).Where(countryid, CondSet, std::move(countrySubQuery));

	Query locationSubQuery = Query(location_namespace).Select(locationid).Where(code, CondEq, kLocationCode);
	Query locationsQuery = Query(location_namespace)
							   .Where(locationid, CondSet, std::move(locationSubQuery))
							   .InnerJoin(std::move(countriesQuery), countryid_fk, CondEq, countryid);

	Query authorSubQuery = Query(authors_namespace).Select(authorid).Where(name, CondEq, "Nested Joined Subquery Author");
	Query authorsQuery = Query(authors_namespace)
							 .Where(authorid, CondSet, std::move(authorSubQuery))
							 .InnerJoin(std::move(locationsQuery), locationid_fk, CondEq, locationid);

	Query joinQuery =
		Query(books_namespace).Where(bookid, CondEq, kBookId).InnerJoin(std::move(authorsQuery), authorid_fk, CondEq, authorid);
	QueryWatcher watcher{joinQuery};

	auto qr = rt.Select(joinQuery);
	auto err = VerifyResJSON(qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	reindexer::joins::QueryJoinsTable joinsInfo{Impl(joinQuery)};
	const int authorsNsId = joinsInfo.GetJoinedNsId(0, 0);
	const int locationsNsId = joinsInfo.GetJoinedNsId(authorsNsId, 0);
	const int countriesNsId = joinsInfo.GetJoinedNsId(locationsNsId, 0);

	auto bookIt = qr.begin();
	auto authorsCtx = bookIt.GetJoinedContext();
	auto& authorsIt = authorsCtx.iterator;
	ASSERT_EQ(authorsIt.GetFieldsCount(), 1);
	auto authorsField = authorsIt.Begin();
	ASSERT_EQ(authorsField.ItemsCount(), 1);

	auto authorItem = authorsField.GetItem(0, qr.GetPayloadType(authorsNsId), qr.GetTagsMatcher(authorsNsId));
	ASSERT_EQ(authorItem.GetField(authorItem.FieldIndex(authorid)).As<int>(), kAuthorId);

	auto authorQr = authorsField.ToQueryResults(authorsCtx);
	ASSERT_EQ(authorQr.Count(), 1);
	auto locationsCtx = authorQr.begin().GetJoinedContext();
	auto& locationsIt = locationsCtx.iterator;
	ASSERT_EQ(locationsIt.GetFieldsCount(), 1);
	auto locationsField = locationsIt.Begin();
	ASSERT_EQ(locationsField.ItemsCount(), 1);

	auto locationItem = locationsField.GetItem(0, qr.GetPayloadType(locationsNsId), qr.GetTagsMatcher(locationsNsId));
	ASSERT_EQ(locationItem.GetField(locationItem.FieldIndex(locationid)).As<int>(), kLocationId);
	ASSERT_EQ(locationItem.GetField(locationItem.FieldIndex(code)).As<int>(), kLocationCode);

	auto locationQr = locationsField.ToQueryResults(locationsCtx);
	ASSERT_EQ(locationQr.Count(), 1);
	auto countriesCtx = locationQr.begin().GetJoinedContext();
	auto& countriesIt = countriesCtx.iterator;
	ASSERT_EQ(countriesIt.GetFieldsCount(), 1);
	auto countriesField = countriesIt.Begin();
	ASSERT_EQ(countriesField.ItemsCount(), 1);

	auto countryItem = countriesField.GetItem(0, qr.GetPayloadType(countriesNsId), qr.GetTagsMatcher(countriesNsId));
	ASSERT_EQ(countryItem.GetField(countryItem.FieldIndex(countryid)).As<int>(), kCountryId);
	ASSERT_EQ(countryItem.GetField(countryItem.FieldIndex(countryCode)).As<int>(), kCountryCode);
}

TEST_F(JoinSelectsApi, TestNestedJoinsSQL) {
	{
		constexpr auto sql = R"(
			SELECT * FROM books_namespace
				INNER JOIN (
					SELECT * FROM authors_namespace
					INNER JOIN (
						SELECT * FROM location_namespace
						INNER JOIN countries_namespace
							ON location_namespace.countryid_fk = countries_namespace.countryid
						WHERE location_namespace.code >= 1
					)
						ON authors_namespace.locationid_fk = location_namespace.locationid
					INNER JOIN genres_namespace
						ON authors_namespace.authorid >= genres_namespace.genreid
				)
				ON books_namespace.authorid_fk = authors_namespace.authorid;)";

		Query query{Query::FromSQL(sql)};
		const auto queryImpl = Impl(query);
		ASSERT_EQ(queryImpl.JoinQueries().size(), 1);
		ASSERT_EQ(Impl(queryImpl.JoinQueries()[0]).JoinQueries().size(), 2);
		ASSERT_EQ(Impl(Impl(queryImpl.JoinQueries()[0]).JoinQueries()[0]).NsName(), location_namespace);
		ASSERT_EQ(Impl(Impl(queryImpl.JoinQueries()[0]).JoinQueries()[0]).JoinQueries().size(), 1);
		ASSERT_EQ(Impl(Impl(Impl(queryImpl.JoinQueries()[0]).JoinQueries()[0]).JoinQueries()[0]).NsName(), countries_namespace);
		ASSERT_EQ(Impl(Impl(queryImpl.JoinQueries()[0]).JoinQueries()[1]).NsName(), genres_namespace);

		const Query queryFromSql{Query::FromSQL(query.GetSQL())};
		ASSERT_EQ(query, queryFromSql) << query.GetSQL();
	}
	{
		auto queryLocations = Query{location_namespace}
								  .Limit(100)
								  .Where(code, CondGe, 1)
								  .InnerJoin(Query{countries_namespace}, countryid_fk, CondEq, countryid);

		auto queryAuthors = Query{authors_namespace}
								.Limit(100)
								.InnerJoin(std::move(queryLocations), locationid_fk, CondEq, locationid)
								.InnerJoin(Query{genres_namespace}, authorid, CondGe, genreid);

		auto queryBooks =
			Query{books_namespace}.Limit(50).Where(price, CondGe, 2).InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid);

		const auto querySql{queryBooks.GetSQL()};
		constexpr auto expectedSQL =
			R"(SELECT * FROM books_namespace WHERE price >= 2 AND INNER JOIN (SELECT * FROM authors_namespace WHERE INNER JOIN (SELECT * FROM location_namespace WHERE code >= 1 AND INNER JOIN countries_namespace ON location_namespace.countryid_fk = countries_namespace.countryid LIMIT 100) ON authors_namespace.locationid_fk = location_namespace.locationid AND INNER JOIN genres_namespace ON authors_namespace.authorid >= genres_namespace.genreid LIMIT 100) ON books_namespace.authorid_fk = authors_namespace.authorid LIMIT 50)";
		ASSERT_EQ(expectedSQL, querySql);
		ASSERT_EQ(queryBooks, Query::FromSQL(querySql));
	}
}

// Recursive verification function for nested join chains.
// At each depth level: verifies FK matches this level's PK (id), computes next FK,
// and recurses into the nested join context. Leaf level checks there are no more joins.
static void VerifyNestedJoinChain(reindexer::joins::JoinedItemContext& ctx, int depth, int expectedFk, int kJoinLevels,
								  const std::vector<int>& chainNsIds, const reindexer::QueryResults& joinQr) {
	auto& joinedIt = ctx.iterator;

	if (depth == kJoinLevels) {
		ASSERT_EQ(joinedIt.GetFieldsCount(), 0) << "Expected 0 joined fields at leaf, depth=" << depth;
		const auto joinedItemsCount{joinedIt.GetItemsCount()};
		ASSERT_EQ(joinedItemsCount, 0) << "Expected 0 joined items at leaf, depth=" << depth;
		return;
	}

	ASSERT_EQ(joinedIt.GetFieldsCount(), 1) << "Expected 1 joined field at depth " << depth;

	auto fieldIt = joinedIt.Begin();
	ASSERT_NE(fieldIt, joinedIt.End()) << "No joined fields at depth " << depth;
	ASSERT_EQ(fieldIt.ItemsCount(), 1) << "Expected 1 joined item at depth " << depth;

	auto nestedItem = fieldIt.GetItem(0, joinQr.GetPayloadType(chainNsIds[depth]), joinQr.GetTagsMatcher(chainNsIds[depth]));
	const int pkValue = nestedItem.GetField(nestedItem.FieldIndex("id")).As<int>();
	ASSERT_EQ(expectedFk, pkValue) << "FK-PK mismatch at depth " << depth;

	const int nextFk = (depth < kJoinLevels - 1) ? pkValue : -1;

	for (auto& nestedResult : fieldIt.ToQueryResults(ctx)) {
		if (nestedResult.GetItemRef().Nsid() != chainNsIds[depth]) {
			continue;
		}
		auto nestedCtx{nestedResult.GetJoinedContext()};
		VerifyNestedJoinChain(nestedCtx, depth + 1, nextFk, kJoinLevels, chainNsIds, joinQr);
	}
}

TEST_F(JoinSelectsApi, InnerJoinWithNestedJoinBigDepthTest) {
	constexpr int kChainDepth = 10;
	constexpr int kItemsPerLevel = 50;
	const std::string kBaseNsName = "deep_nested_ns_";

	// Build linked namespaces inline
	std::vector<std::string> nsNames;
	for (int i = 0; i < kChainDepth; ++i) {
		std::string nsName{
			kBaseNsName + RandString() + "_level_" +
			std::to_string(
				std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count() + i)};
		rt.OpenNamespace(nsName);
		std::vector<IndexDeclaration> indexes;
		indexes.reserve(2);
		indexes.emplace_back("id", "hash", "int", IndexOpts().PK(), 0);
		std::string fkIndexName;
		if (i < kChainDepth - 1) {
			fkIndexName = "fk_" + std::to_string(i);
			indexes.emplace_back(fkIndexName, "hash", "int", IndexOpts(), 0);
		}
		DefineNamespaceDataset(nsName, indexes);
		nsNames.emplace_back(std::move(nsName));
	}

	// Fill data: each level has items with id from 0 to kItemsPerLevel-1.
	// FK at level i equals id, so it always joins to the next level's item with the same id.
	for (int level = 0; level < kChainDepth; ++level) {
		for (int i = 0; i < kItemsPerLevel; ++i) {
			Item item{NewItem(nsNames[level])};
			item["id"] = i;
			if (level < kChainDepth - 1) {
				item["fk_" + std::to_string(level)] = i;
			}
			Upsert(nsNames[level], item);
		}
	}

	// Build deep nested join query from leaf to root.
	// Each level wraps the previous one: Query(level[i]).InnerJoin(fk_i, id, ..., Query(level[i+1])...)
	Query leafQuery = Query(nsNames.back()).Limit(100);
	Query joinQuery = std::move(leafQuery);
	for (int level = kChainDepth - 2; level >= 0; --level) {
		joinQuery = Query(nsNames[level]).InnerJoin(std::move(joinQuery), "fk_" + std::to_string(level), CondEq, "id");
	}

	// Add merge queries
	joinQuery.Merge(Query(nsNames[0]));
	joinQuery.Merge(Query(nsNames[kChainDepth - 1]));
	joinQuery.Merge(Query(nsNames[kChainDepth / 2]));

	// Verify join chain NsIds through QueryJoinsTable
	const int rootNsId{0};
	const int kJoinLevels{kChainDepth - 1};	 // Number of join levels in the chain
	std::vector<int> chainNsIds(kJoinLevels);

	reindexer::joins::QueryJoinsTable joinsInfo{Impl(joinQuery)};
	int prevNsId{rootNsId};
	for (int depth = 0; depth < kJoinLevels; ++depth) {
		chainNsIds[depth] = joinsInfo.GetJoinedNsId(prevNsId, 0);
		ASSERT_TRUE(chainNsIds[depth] > 0) << "Failed to get nsId at depth " << depth;
		prevNsId = chainNsIds[depth];
	}

	const int mergedSize{static_cast<int>(Impl(joinQuery).MergeQueries().size())};
	for (int i = 0; i < kJoinLevels; ++i) {
		ASSERT_TRUE(chainNsIds[i] == i + 1 + mergedSize) << "nsId mismatch at depth " << i;
	}

	// Execute query
	auto joinQr{rt.Select(joinQuery)};
	auto err{VerifyResJSON(joinQr)};
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(joinQr.Count() > 0);

	for (auto it : joinQr) {
		// Only verify items from the root (main) query - skip merge query results
		if (it.GetItemRef().Nsid() != rootNsId) {
			continue;
		}

		Item item(it.GetItem(false));
		// The FK of root item equals root item's id (we set them equal). Extract as int.
		const int rootId = item["id"].As<int>();
		const int fkValue = item["fk_0"].As<int>();
		ASSERT_EQ(rootId, fkValue) << "Root item's id should match its fk_0";

		auto rootCtx{it.GetJoinedContext()};
		VerifyNestedJoinChain(rootCtx, 0, fkValue, kJoinLevels, chainNsIds, joinQr);
	}
}

TEST_F(JoinSelectsApi, InnerJoinSmallNsPreselectTest) {
	auto prepareData = [this](int32_t itemsCount, int32_t preresult_max_iterations) {
		std::ignore = rt.ExecSQL(fmt::format("delete from authors_namespace"));
		std::ignore = rt.ExecSQL(fmt::format("delete from books_namespace"));
		std::ignore = rt.ExecSQL(fmt::format(
			"update #config set namespaces[*].max_iterations_idset_preresult = {} where type = 'namespaces'", preresult_max_iterations));

		FillAuthorsNamespace(itemsCount);
		FillBooksNamespace(0, itemsCount);
	};

	auto executeAndCheck = [this](unsigned booksLimit, const std::string& method) {
		Query queryAuthors(authors_namespace);
		Query queryBooks{Query(books_namespace).Limit(booksLimit).Where(price, CondLe, 10000)};
		Query joinQuery{queryBooks.Explain().InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid)};
		QueryWatcher watcher{joinQuery};

		auto qr{rt.Select(joinQuery)};

		gason::JsonParser parser;
		auto json = parser.Parse(qr.GetExplainResults());
		auto selectors = json["selectors"];

		size_t index = 0;
		for (const auto& item : selectors) {
			if (++index == 3) {
				EXPECT_EQ(item["field"].As<std::string>(), "inner_join authors_namespace");
				EXPECT_EQ(item["method"].As<std::string>(), method);
			}
		}
		EXPECT_EQ(index, 3);
	};

	// 1. Check cases with optimization
	prepareData(199, 210);
	executeAndCheck(199, "preselected_values");

	prepareData(205, 210);
	executeAndCheck(205, "preselected_rows");

	// 2. Check case without optimization
	prepareData(210, 200);
	executeAndCheck(210, "no_preselect");
}

TEST_F(JoinSelectsApi, LeftJoinTest) {
	Query booksQuery{Query(books_namespace).Where(price, CondGe, 500)};
	auto booksQueryRes = rt.Select(booksQuery);
	QueryResultRows pureSelectRows;
	for (auto it : booksQueryRes) {
		Item item(it.GetItem(false));
		BookId bookId = item[bookid].Get<int>();
		QueryResultRow& resultRow = pureSelectRows[bookId];
		FillQueryResultFromItem(item, resultRow);
	}

	Query joinQuery{Query(authors_namespace).LeftJoin(std::move(booksQuery), authorid, CondEq, authorid_fk)};

	QueryWatcher watcher{joinQuery};
	auto joinQueryRes = rt.Select(joinQuery);
	auto err = VerifyResJSON(joinQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	std::unordered_set<int> presentedAuthorIds;
	std::unordered_map<reindexer::IdType, int> rowidsIndexes;
	int i = 0;
	for (auto rowIt : joinQueryRes.ToLocalQr()) {
		Item item(rowIt.GetItem(false));
		Variant authorIdKeyRef1 = item[authorid];
		const reindexer::ItemRef& rowid = rowIt.GetItemRef();

		auto itemItCtx = rowIt.GetJoinedContext();
		auto& itemIt = itemItCtx.iterator;
		if (itemIt.GetFieldsCount() == 0) {
			continue;
		}

		for (auto joinedFieldIt = itemIt.Begin(); joinedFieldIt != itemIt.End(); ++joinedFieldIt) {
			auto jqr = joinedFieldIt.ToQueryResults(itemItCtx);

			ASSERT_GT(jqr.Count(), 0);
			auto queryResult = jqr.begin();
			Item item2 = queryResult.GetItem();
			ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();

			Variant authorIdKeyRef2 = item2[authorid_fk];
			EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
		}

		presentedAuthorIds.insert(static_cast<int>(authorIdKeyRef1));
		rowidsIndexes.insert({rowid.Id(), i});
		i++;
	}

	for (const auto& rowIt : joinQueryRes.ToLocalQr()) {
		const auto rowid = rowIt.GetItemRef().Id();
		auto itemItCtx = rowIt.GetJoinedContext();
		auto& itemIt = itemItCtx.iterator;
		for (auto joinedFieldIt = itemIt.Begin(); joinedFieldIt != itemIt.End(); ++joinedFieldIt) {
			for (auto queryResult : joinedFieldIt.ToQueryResults(itemItCtx)) {
				Item item = queryResult.GetItem();
				ASSERT_TRUE(item.Status().ok()) << item.Status().what();

				Variant authorIdKeyRef1 = item[authorid_fk];
				int authorId = static_cast<int>(authorIdKeyRef1);

				auto itAuthorid(presentedAuthorIds.find(authorId));
				EXPECT_NE(itAuthorid, presentedAuthorIds.end());

				auto itRowidIndex(rowidsIndexes.find(rowid));
				EXPECT_NE(itRowidIndex, rowidsIndexes.end());

				if (itRowidIndex != rowidsIndexes.end()) {
					Item item2((joinQueryRes.begin() + rowid.ToNumber()).GetItem(false));
					Variant authorIdKeyRef2 = item2[authorid];
					EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
				}
			}
		}
	}
}

TEST_F(JoinSelectsApi, OrInnerJoinTest) {
	Query queryGenres(genres_namespace);
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace).Limit(10).Where(price, CondGe, 500)};
	Query innerJoinQuery = std::move(queryBooks.InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid));
	Query orInnerJoinQuery = std::move(innerJoinQuery.Or().InnerJoin(std::move(queryGenres), genreId_fk, CondEq, genreid));
	QueryWatcher watcher{orInnerJoinQuery};

	const int authorsNsJoinIndex = 0;
	const int genresNsJoinIndex = 1;

	auto queryRes = rt.Select(orInnerJoinQuery);
	auto err = VerifyResJSON(queryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto rowIt : queryRes) {
		Item item(rowIt.GetItem(false));
		auto joinedItemIt = rowIt.GetJoined();

		reindexer::joins::FieldIterator authorIdIt = joinedItemIt.At(authorsNsJoinIndex);
		Variant authorIdKeyRef1 = item[authorid_fk];
		for (int i = 0; i < authorIdIt.ItemsCount(); ++i) {
			reindexer::ItemImpl authorsItem(authorIdIt.GetItem(i, queryRes.GetPayloadType(1), queryRes.GetTagsMatcher(1)));
			Variant authorIdKeyRef2 = authorsItem.GetField(queryRes.GetPayloadType(1).FieldByName(authorid));
			EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
		}

		reindexer::joins::FieldIterator genreIdIt = joinedItemIt.At(genresNsJoinIndex);
		Variant genresIdKeyRef1 = item[genreId_fk];
		for (int i = 0; i < genreIdIt.ItemsCount(); ++i) {
			reindexer::ItemImpl genresItem = genreIdIt.GetItem(i, queryRes.GetPayloadType(2), queryRes.GetTagsMatcher(2));
			Variant genresIdKeyRef2 = genresItem.GetField(queryRes.GetPayloadType(2).FieldByName(genreid));
			EXPECT_EQ(genresIdKeyRef1, genresIdKeyRef2);
		}
	}
}

TEST_F(JoinSelectsApi, JoinTestSorting) {
	for (size_t i = 0; i < 10; ++i) {
		int booksTimeout = 1000, authorsTimeout = 0;
		if (i % 2 == 0) {
			std::swap(booksTimeout, authorsTimeout);
		} else if (i % 3) {
			authorsTimeout = booksTimeout;
		}
		ChangeNsOptimizationTimeout(books_namespace, booksTimeout);
		ChangeNsOptimizationTimeout(authors_namespace, authorsTimeout);
		std::this_thread::sleep_for(std::chrono::milliseconds(150));
		Query booksQuery{
			Query(books_namespace).Offset(11).Limit(1111).Where(pages, CondGe, 100).Where(price, CondGe, 200).Sort(price, SortOrder::Desc)};
		Query joinQuery{Query(authors_namespace)
							.Where(authorid, CondLe, 100)
							.LeftJoin(std::move(booksQuery), authorid, CondEq, authorid_fk)
							.Sort(age, SortOrder::Asc)
							.Limit(10)};

		QueryWatcher watcher{joinQuery};
		auto joinQueryRes = rt.Select(joinQuery);
		Variant prevField;
		for (auto rowIt : joinQueryRes) {
			Item item = rowIt.GetItem(false);
			const auto cmpRes = prevField.Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(item[age]);
			ASSERT_NE(cmpRes & reindexer::ComparationResult::Le, 0);

			Variant key = item[authorid];
			auto itemItCtx = rowIt.GetJoinedContext();
			auto& itemIt = itemItCtx.iterator;
			if (itemIt.GetFieldsCount() == 0) {
				continue;
			}

			auto joinedFieldIt = itemIt.Begin();
			auto jqr = joinedFieldIt.ToQueryResults(itemItCtx);

			std::optional<Variant> prevJoinedValue;
			for (auto queryResult : jqr) {
				reindexer::Item joinItem = queryResult.GetItem();
				ASSERT_TRUE(joinItem.Status().ok()) << joinItem.Status().what();

				Variant fkey = joinItem[authorid_fk];
				auto cmpRes = key.Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(fkey);
				ASSERT_EQ(cmpRes, reindexer::ComparationResult::Eq) << key.As<std::string>() << " " << fkey.As<std::string>();

				Variant recentJoinedValue = joinItem[price];
				ASSERT_GE(recentJoinedValue.As<int>(), 200);

				if (prevJoinedValue.has_value()) {
					cmpRes =
						prevJoinedValue->Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(recentJoinedValue);
					ASSERT_TRUE(cmpRes & reindexer::ComparationResult::Ge)
						<< prevJoinedValue->As<std::string>() << " " << recentJoinedValue.As<std::string>();
				}

				Variant pagesValue = joinItem[pages];
				ASSERT_GE(pagesValue.As<int>(), 100);
				prevJoinedValue = recentJoinedValue;
			}
			prevField = item[age];
		}
	}
}

TEST_F(JoinSelectsApi, TestSortingByJoinedNs) {
	Query joinedQuery1 = Query(books_namespace);
	Query query1{Query(authors_namespace)
					 .LeftJoin(std::move(joinedQuery1), authorid, CondEq, authorid_fk)
					 .Sort(books_namespace + '.' + price, SortOrder::Asc)};

	reindexer::QueryResults joinQueryRes1;
	Error err = rt.reindexer->Select(query1, joinQueryRes1);
	// several book to one author, cannot sort
	ASSERT_FALSE(err.ok());
	EXPECT_STREQ(err.what(), "Not found value joined from ns books_namespace");

	Query joinedQuery2 = Query(authors_namespace);
	Query query2{Query(books_namespace)
					 .InnerJoin(std::move(joinedQuery2), authorid_fk, CondEq, authorid)
					 .Sort(authors_namespace + '.' + age, SortOrder::Asc)};

	QueryWatcher watcher{query2};
	auto joinQueryRes2 = rt.Select(query2);
	Variant prevValue;
	for (auto& rowIt : joinQueryRes2) {
		auto itemItCtx = rowIt.GetJoinedContext();
		auto& itemIt = itemItCtx.iterator;
		const auto joinedItemsCount{itemIt.GetItemsCount()};
		ASSERT_EQ(joinedItemsCount, 1);
		const auto joinedFieldIt = itemIt.Begin();
		auto joinItem = joinedFieldIt.ToQueryResults(itemItCtx)[0].GetItem();
		const Variant recentValue = joinItem[age];

		reindexer::WrSerializer ser;
		const auto cmpRes = prevValue.Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(recentValue);
		ASSERT_NE(cmpRes & reindexer::ComparationResult::Le, 0) << (prevValue.Dump(ser), ser << ' ', recentValue.Dump(ser), ser.Slice());

		prevValue = recentValue;
	}
}

TEST_F(JoinSelectsApi, JoinTestSelectNonIndexedField) {
	Query authorsQuery = Query(authors_namespace);
	auto qr = rt.Select(Query(books_namespace)
							.Where(rating, CondEq, Variant(static_cast<int64_t>(100)))
							.InnerJoin(std::move(authorsQuery), authorid_fk, CondEq, authorid));
	ASSERT_EQ(qr.Count(), 1);

	Item theOnlyItem = qr.begin().GetItem(false);
	reindexer::VariantArray krefs = theOnlyItem[title];
	ASSERT_EQ(krefs.size(), 1);
	ASSERT_EQ(krefs[0].As<std::string>(), "Crime and Punishment");
}

TEST_F(JoinSelectsApi, JoinByNonIndexedField) {
	rt.OpenNamespace(default_namespace);
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});

	std::stringstream json;
	json << "{" << addQuotes(id) << ":" << 1 << "," << addQuotes(authorid_fk) << ":" << DostoevskyAuthorId << "}";
	rt.UpsertJSON(default_namespace, json.str());

	Query authorsQuery = Query(authors_namespace);
	auto qr = rt.Select(Query(default_namespace)
							.Where(authorid_fk, CondEq, Variant(DostoevskyAuthorId))
							.InnerJoin(std::move(authorsQuery), authorid_fk, CondEq, authorid));
	ASSERT_EQ(qr.Count(), 1);

	// And backwards even!
	Query testNsQuery = Query(default_namespace);
	auto qr2 = rt.Select(Query(authors_namespace)
							 .Where(authorid, CondEq, Variant(DostoevskyAuthorId))
							 .InnerJoin(std::move(testNsQuery), authorid, CondEq, authorid_fk));
	ASSERT_EQ(qr2.Count(), 1);
}

TEST_F(JoinSelectsApi, JoinsEasyStressTest) {
	auto selectTh = [this]() {
		Query queryGenres(genres_namespace);
		Query queryAuthors(authors_namespace);
		Query queryBooks{Query(books_namespace).Limit(10).Where(price, CondGe, 600).Sort(bookid, SortOrder::Asc)};
		Query joinQuery1 = std::move(queryBooks.InnerJoin(Query{queryAuthors}, authorid_fk, CondEq, authorid).Sort(pages, SortOrder::Asc));
		Query joinQuery2 = std::move(joinQuery1.LeftJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid));
		Query orInnerJoinQuery = std::move(
			joinQuery2.Or().InnerJoin(std::move(queryGenres), genreId_fk, CondEq, genreid).Sort(price, SortOrder::Desc).Limit(20));
		for (size_t i = 0; i < 10; ++i) {
			auto queryRes = rt.Select(orInnerJoinQuery);
			EXPECT_GT(queryRes.Count(), 0);
		}
	};

	auto removeTh = [this]() { std::ignore = rt.Delete(Query(books_namespace).Limit(10).Where(price, CondGe, 5000)); };

	int32_t since = 0, count = 1000;
	std::vector<std::thread> threads;
#if RX_WITH_STDLIB_DEBUG
	constexpr size_t kItersCount = 8;
#else	// RX_WITH_STDLIB_DEBUG
	constexpr size_t kItersCount = 20;
#endif	// RX_WITH_STDLIB_DEBUG
	for (size_t i = 0; i < kItersCount; ++i) {
		threads.push_back(std::thread(selectTh));
		if (i % 2 == 0) {
			threads.push_back(std::thread(removeTh));
		}
		if (i % 4 == 0) {
			threads.push_back(std::thread([this, since, count]() { FillBooksNamespace(since, count); }));
		}
		since += 1000;
	}
	for (size_t i = 0; i < threads.size(); ++i) {
		threads[i].join();
	}
}

TEST_F(JoinSelectsApi, PreSelectStoreValuesOptimizationStressTest) {
	using reindexer::joins::PreSelect;
	static const std::string rightNs = "rightNs";
	static constexpr const char* data = "data";
	static constexpr int maxDataValue = 10;
	static constexpr int maxRightNsRowCount = maxDataValue * PreSelect::MaxIterationsForValuesOptimization;
	static constexpr int maxLeftNsRowCount = 10000;
	static constexpr size_t leftNsCount = 50;
	static std::vector<std::string> leftNs;
	if (leftNs.empty()) {
		leftNs.reserve(leftNsCount);
		for (size_t i = 0; i < leftNsCount; ++i) {
			leftNs.push_back("leftNs" + std::to_string(i));
		}
	}

	const auto createNs = [this](const std::string& ns) {
		rt.OpenNamespace(ns);
		DefineNamespaceDataset(
			ns, {IndexDeclaration{id, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{data, "hash", "int", IndexOpts(), 0}});
	};
	const auto fill = [this](const std::string& ns, int startId, int endId) {
		for (int i = startId; i < endId; ++i) {
			Item item = NewItem(ns);
			item[id] = i;
			item[data] = rand() % maxDataValue;
			Upsert(ns, item);
		}
	};

	createNs(rightNs);
	fill(rightNs, 0, maxRightNsRowCount);
	std::atomic<bool> start{false};
	std::vector<std::thread> threads;
	threads.reserve(leftNs.size());
	for (size_t i = 0; i < leftNs.size(); ++i) {
		createNs(leftNs[i]);
		fill(leftNs[i], 0, maxLeftNsRowCount);
		threads.emplace_back([this, i, &start]() {
			// about 50% of queries will use the optimization
			Query q{Query(leftNs[i]).InnerJoin(Query(rightNs).Where(data, CondEq, rand() % maxDataValue), data, CondEq, data)};
			while (!start) {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
			}
			std::ignore = rt.Select(q);
		});
	}
	start = true;
	for (auto& th : threads) {
		th.join();
	}
}

static void checkForAllowedJsonTags(const std::vector<std::string>& tags, gason::JsonValue jsonValue) {
	size_t count = 0;
	for (const auto& elem : jsonValue) {
		ASSERT_NE(std::find(tags.begin(), tags.end(), std::string_view(elem.key)), tags.end()) << elem.key;
		++count;
	}
	ASSERT_EQ(count, tags.size());
}

TEST_F(JoinSelectsApi, JoinWithSelectFilter) {
	Query queryAuthors = Query(authors_namespace).Select(name, age);

	Query queryBooks{Query(books_namespace)
						 .Where(pages, CondGe, 100)
						 .InnerJoin(std::move(queryAuthors), authorid_fk, CondEq, authorid)
						 .Select(title, price)};

	auto qr = rt.Select(queryBooks);
	for (auto it : qr) {
		ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		reindexer::WrSerializer wrser;
		auto err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();

		auto itemItCtx = it.GetJoinedContext();
		auto& joinIt = itemItCtx.iterator;
		gason::JsonParser jsonParser;
		gason::JsonNode root = jsonParser.Parse(reindexer::giftStr(wrser.Slice()));
		checkForAllowedJsonTags({title, price, "joined_authors_namespace"}, root.value);

		for (auto fieldIt = joinIt.Begin(); fieldIt != joinIt.End(); ++fieldIt) {
			LocalQueryResults jqr = fieldIt.ToQueryResults(itemItCtx);
			for (auto jit : jqr) {
				ASSERT_TRUE(jit.Status().ok()) << jit.Status().what();
				wrser.Reset();
				err = jit.GetJSON(wrser, false);
				ASSERT_TRUE(err.ok()) << err.what();
				root = jsonParser.Parse(reindexer::giftStr(wrser.Slice()));
				checkForAllowedJsonTags({name, age}, root.value);
			}
		}
	}
}

// Execute a query that is merged with another one:
// both queries should contain join queries,
// joined NS for the 1st query should be the same
// as the main NS of the merged query.
TEST_F(JoinSelectsApi, TestMergeWithJoins) {
	// Build the 1st query with 'authors_namespace' as join.
	const auto queryBooks = Query(books_namespace)
								.InnerJoin(Query(authors_namespace), authorid_fk, CondEq, authorid)
								.Merge(Query(authors_namespace).LeftJoin(Query(location_namespace), locationid_fk, CondEq, locationid));

	// Execute it
	auto qr = rt.Select(queryBooks);
	auto err = VerifyResJSON(qr);
	ASSERT_TRUE(err.ok()) << err.what();

	// Make sure results are correct:
	// values of main and joined namespaces match
	// in both parts of the query.
	size_t rowId = 0;
	for (auto it : qr) {
		Item item = it.GetItem(false);
		auto joinedCtx = it.GetJoinedContext();
		auto& joined = joinedCtx.iterator;
		ASSERT_EQ(joined.GetFieldsCount(), 1);

		bool booksItem = (rowId <= 10000);
		LocalQueryResults jqr = joined.Begin().ToQueryResults(joinedCtx);

		if (booksItem) {
			Variant fkValue = item[authorid_fk];
			for (auto jit : jqr) {
				Item jItem = jit.GetItem(false);
				Variant value = jItem[authorid];
				ASSERT_EQ(value, fkValue);
			}
		} else {
			Variant fkValue = item[locationid_fk];
			for (auto jit : jqr) {
				Item jItem = jit.GetItem(false);
				Variant value = jItem[locationid];
				ASSERT_EQ(value, fkValue);
			}
		}

		++rowId;
	}
}

TEST_F(JoinSelectsApi, TestMergeWithSubqueryInJoinedQuery) {
	constexpr int kAuthorId = 991101;
	constexpr int kBookId = 991102;
	constexpr int kPrice = 991103;

	{
		Item authorItem = NewItem(authors_namespace);
		authorItem[authorid] = kAuthorId;
		authorItem[name] = "Merge Joined Subquery Author";
		authorItem[age] = 69;
		authorItem[locationid_fk] = 0;
		Upsert(authors_namespace, authorItem);
	}
	{
		Item bookItem = NewItem(books_namespace);
		bookItem[bookid] = kBookId;
		bookItem[title] = "Merge Joined Subquery Book";
		bookItem[pages] = 987;
		bookItem[price] = kPrice;
		bookItem[genreId_fk] = 4;
		bookItem[authorid_fk] = kAuthorId;
		Upsert(books_namespace, bookItem);
	}

	Query booksSubQuery = Query(books_namespace).Select(bookid).Where(price, CondEq, kPrice);
	Query booksQuery = Query(books_namespace).Where(bookid, CondSet, std::move(booksSubQuery));
	Query queryAuthors =
		Query(authors_namespace).Where(authorid, CondEq, kAuthorId).InnerJoin(std::move(booksQuery), authorid, CondEq, authorid_fk);

	Query queryBooks = Query(books_namespace).Where(bookid, CondEq, kBookId + 1);
	queryBooks.Merge(std::move(queryAuthors));
	QueryWatcher watcher{queryBooks};

	auto qr = rt.Select(queryBooks);
	auto err = VerifyResJSON(qr);
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	auto rowIt = qr.begin();
	ASSERT_EQ(rowIt.GetItemRef().Nsid(), 1);
	Item authorItem = rowIt.GetItem(false);
	ASSERT_EQ(authorItem[authorid].As<int>(), kAuthorId);

	auto joinedCtx = rowIt.GetJoinedContext();
	auto& joinedIt = joinedCtx.iterator;
	ASSERT_EQ(joinedIt.GetFieldsCount(), 1);
	auto booksField = joinedIt.Begin();
	ASSERT_EQ(booksField.ItemsCount(), 1);

	auto bookItem = booksField.ToQueryResults(joinedCtx)[0].GetItem();
	ASSERT_EQ(bookItem[bookid].As<int>(), kBookId);
	ASSERT_EQ(bookItem[price].As<int>(), kPrice);
}

// Check MERGEs nested into the JOINs (expecting errors)
TEST_F(JoinSelectsApi, TestNestedMergesInJoinsError) {
	constexpr auto sqlPattern =
		R"(select * from books_namespace {} (select * from authors_namespace merge (select * from books_namespace)) on authors_namespace.authorid = books_namespace.authorid_fk)";
	auto joinTypes = {"inner join", "join", "left join"};
	for (auto& join : joinTypes) {
		auto sql = fmt::format(sqlPattern, join);
		ValidateQueryThrow(sql, errParseSQL, "Expected ')', but found 'merge', line: 1 column: .*");
	}
}

// Check MERGEs nested into the MERGEs (expecting errors)
TEST_F(JoinSelectsApi, TestNestedMergesInMergesError) {
	constexpr char sql[] =
		R"(select * from books_namespace merge (select * from authors_namespace  merge (select * from books_namespace)))";
	ValidateQueryError(sql, errParams, "MERGEs nested into the MERGEs are not supported");
}

TEST_F(JoinSelectsApi, CountCachedWithDifferentJoinConditions) {
	// Test checks if cached total values is changing after inner join's condition change

	const std::vector<Query> kBaseQueries = {
		Query(books_namespace).InnerJoin(Query(authors_namespace), authorid_fk, CondEq, authorid).Limit(10),
		Query(books_namespace).InnerJoin(Query(authors_namespace).Where(authorid, CondLe, 100), authorid_fk, CondEq, authorid).Limit(10),
		Query(books_namespace).InnerJoin(Query(authors_namespace).Where(authorid, CondGe, 200), authorid_fk, CondEq, authorid).Limit(10),
		Query(books_namespace).InnerJoin(Query(authors_namespace).Where(authorid, CondLe, 400), authorid_fk, CondEq, authorid).Limit(10),
		Query(books_namespace).InnerJoin(Query(authors_namespace).Where(authorid, CondGe, 400), authorid_fk, CondEq, authorid).Limit(10)};

	SetQueriesCacheHitsCount(1);
	for (auto& bq : kBaseQueries) {
		SCOPED_TRACE(bq.GetSQL());
		const Query cachedTotalNoCondQ = Query(bq).CachedTotal();
		const Query totalCountNoCondQ = Query(bq).ReqTotal();
		auto qrRegular = rt.Select(totalCountNoCondQ);
		// Run all the queries with CountCached twice to check main and cached values
		for (int i = 0; i < 2; ++i) {
			SCOPED_TRACE(std::to_string(i));
			auto qrCached = rt.Select(cachedTotalNoCondQ);
			EXPECT_EQ(qrCached.TotalCount(), qrRegular.TotalCount());
		}
	}
}

TEST_F(JoinSelectsApi, CountCachedWithJoinNsUpdates) {
	const Genre kLastGenre = *genres.rbegin();
	const std::vector<Query> kBaseQueries = {
		Query(books_namespace)
			.InnerJoin(Query(authors_namespace).Where(authorid, CondGe, 100), authorid_fk, CondEq, authorid)
			.Or()
			.InnerJoin(Query(genres_namespace)
						   .Where(genrename, CondSet,
								  {Variant{"non fiction"}, Variant{"poetry"}, Variant{"documentary"}, Variant{kLastGenre.name}}),
					   genreId_fk, CondEq, genreid)
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(Query(authors_namespace).Where(authorid, CondGe, 100), authorid_fk, CondEq, authorid)
			.InnerJoin(Query(genres_namespace)
						   .Where(genrename, CondSet,
								  {Variant{"non fiction"}, Variant{"poetry"}, Variant{"documentary"}, Variant{kLastGenre.name}}),
					   genreId_fk, CondEq, genreid)
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(Query(authors_namespace).Where(authorid, CondGe, 100), authorid_fk, CondEq, authorid)
			.OpenBracket()
			.InnerJoin(Query(genres_namespace).Where(genrename, CondSet, {Variant{"non fiction"}, Variant{kLastGenre.name}}), genreId_fk,
					   CondEq, genreid)
			.Or()
			.InnerJoin(
				Query(genres_namespace).Where(genrename, CondSet, {Variant{"poetry"}, Variant{"documentary"}, Variant{kLastGenre.name}}),
				genreId_fk, CondEq, genreid)
			.CloseBracket()
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(Query(authors_namespace).Where(authorid, CondGe, 100), authorid_fk, CondEq, authorid)
			.OpenBracket()
			.InnerJoin(Query(genres_namespace).Where(genrename, CondSet, {Variant{"non fiction"}, Variant{kLastGenre.name}}), genreId_fk,
					   CondEq, genreid)
			.InnerJoin(Query(genres_namespace), genreId_fk, CondEq, genreid)
			.CloseBracket()
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(Query(authors_namespace).Where(authorid, CondGe, 100), authorid_fk, CondEq, authorid)
			.OpenBracket()
			.InnerJoin(Query(genres_namespace).Where(genrename, CondSet, {Variant{"non fiction"}, Variant{kLastGenre.name}}), genreId_fk,
					   CondEq, genreid)
			.Or()
			.InnerJoin(Query(genres_namespace), genreId_fk, CondEq, genreid)
			.CloseBracket()
			.Limit(10)};

	SetQueriesCacheHitsCount(1);
	for (auto& bq : kBaseQueries) {
		SCOPED_TRACE(bq.GetSQL());
		const Query cachedTotalNoCondQ = Query(bq).CachedTotal();
		const Query totalCountNoCondQ = Query(bq).ReqTotal();
		auto checkQuery = [&](std::string_view step) {
			SCOPED_TRACE(step);
			// With Initial data
			auto qrRegular = rt.Select(totalCountNoCondQ);
			// Run all the queries with CountCached twice to check main and cached values
			for (int i = 0; i < 2; ++i) {
				auto qrCached = rt.Select(cachedTotalNoCondQ);
				EXPECT_EQ(qrCached.TotalCount(), qrRegular.TotalCount()) << "i = " << i;
			}
		};

		// Check query and create cache with initial data
		checkQuery("initial data");

		// Update data on the first joined namespace
		RemoveLastAuthors(250);
		checkQuery("first ns update (remove)");
		FillAuthorsNamespace(250);
		checkQuery("first ns update (add)");

		// Update data on the second joined namespace
		RemoveGenre(kLastGenre.id);
		checkQuery("second ns update (remove)");
		AddGenre(kLastGenre.id, kLastGenre.name);
		checkQuery("second ns update (insert)");
	}
}

TEST_F(JoinOnConditionsApi, TestGeneralConditions) {
	const std::string sqlTemplate =
		R"(select * from books_namespace inner join books_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages {} books_namespace.pages);)";
	for (CondType condition : {CondLt, CondLe, CondGt, CondGe, CondEq}) {
		Query queryBooks = Query::FromSQL(GetSql(sqlTemplate, condition));
		auto qr = rt.Select(queryBooks);
		for (auto it : qr) {
			const auto item = it.GetItem();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			const Variant authorid1 = item[authorid_fk];
			const Variant pages1 = item[pages];
			auto joinedCtx = it.GetJoinedContext();
			auto& joined = joinedCtx.iterator;
			ASSERT_EQ(joined.GetFieldsCount(), 1);
			LocalQueryResults jqr = joined.Begin().ToQueryResults(joinedCtx);
			jqr.addNSContext(qr, 0, reindexer::lsn_t());
			for (auto jit : jqr) {
				auto joinedItem = jit.GetItem();
				ASSERT_TRUE(joinedItem.Status().ok()) << joinedItem.Status().what();
				Variant authorid2 = joinedItem[authorid_fk];
				ASSERT_EQ(authorid1, authorid2);
				Variant pages2 = joinedItem[pages];
				ASSERT_TRUE(CompareVariants(pages1, pages2, condition))
					<< pages1.As<std::string>() << ' ' << reindexer::CondTypeToStr(condition) << ' ' << pages2.As<std::string>();
			}
		}
	}
}

#ifndef REINDEX_WITH_TSAN

TEST_F(JoinOnConditionsApi, TestComparisonConditions) {
	const std::vector<std::pair<std::string, std::string>> sqlTemplates = {
		{R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk {} authors_namespace.authorid);)",
		 R"(select * from books_namespace inner join authors_namespace on (authors_namespace.authorid {} books_namespace.authorid_fk);)"}};
	const std::vector<std::pair<CondType, CondType>> conditions = {{CondLt, CondGt}, {CondLe, CondGe}, {CondGt, CondLt},
																   {CondGe, CondLe}, {CondEq, CondEq}, {CondSet, CondSet}};
	for (size_t i = 0; i < sqlTemplates.size(); ++i) {
		const auto& sqlTemplate = sqlTemplates[i];
		for (const auto& condition : conditions) {
			auto query1 = Query::FromSQL(GetSql(sqlTemplate.first, condition.first));
			auto qr1 = rt.Select(query1);
			auto query2 = Query::FromSQL(GetSql(sqlTemplate.second, condition.second));
			auto qr2 = rt.Select(query2);
			ASSERT_EQ(query1.GetJSON(), query2.GetJSON());
			ASSERT_EQ(qr1.Count(), qr2.Count());
			for (QueryResults::Iterator it1 = qr1.begin(), it2 = qr2.begin(); it1 != qr1.end(); ++it1, ++it2) {
				auto item1 = it1.GetItem();
				ASSERT_TRUE(item1.Status().ok()) << item1.Status().what();
				auto joined1Ctx = it1.GetJoinedContext();
				auto& joined1 = joined1Ctx.iterator;
				ASSERT_EQ(joined1.GetFieldsCount(), 1);
				LocalQueryResults jqr1 = joined1.Begin().ToQueryResults(joined1Ctx);

				auto item2 = it2.GetItem();
				ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();
				auto itemItCtx = it2.GetJoinedContext();
				auto& itemIt = itemItCtx.iterator;
				ASSERT_EQ(itemIt.GetFieldsCount(), 1);

				auto joinedFieldIt = itemIt.Begin();
				auto jqr2 = joinedFieldIt.ToQueryResults(itemItCtx);
				jqr2.addNSContext(qr2, 1, reindexer::lsn_t());

				ASSERT_EQ(jqr1.Count(), jqr2.Count());

				for (auto jit1 = jqr1.begin(), jit2 = jqr2.begin(); jit1 != jqr1.end(); ++jit1, ++jit2) {
					auto joinedItem1 = jit1.GetItem();
					ASSERT_TRUE(joinedItem1.Status().ok()) << joinedItem1.Status().what();
					Variant authorid11 = item1[authorid_fk];
					Variant authorid12 = joinedItem1[authorid];
					ASSERT_TRUE(CompareVariants(authorid11, authorid12, condition.first));

					auto joinedItem2 = jit2.GetItem();
					ASSERT_TRUE(joinedItem2.Status().ok()) << joinedItem2.Status().what();
					Variant authorid21 = item2[authorid_fk];
					Variant authorid22 = joinedItem2[authorid];
					ASSERT_TRUE(CompareVariants(authorid21, authorid22, condition.first));

					ASSERT_EQ(authorid11, authorid21);
					ASSERT_EQ(authorid12, authorid22);
				}
			}
		}
	}
}

#endif

TEST_F(JoinOnConditionsApi, TestLeftJoinOnCondSet) {
	const std::string leftNs = "leftNs";
	const std::string rightNs = "rightNs";
	std::vector<int> leftNsData = {1, 3, 10};
	std::vector<std::vector<int>> rightNsData = {{1, 2, 3}, {3, 4, 5}, {5, 6, 7}};
	CreateCondSetTable(leftNs, rightNs, leftNsData, rightNsData);
	// clang-format off
	const std::vector<std::string_view> results = {
						R"({"id":1,"joined_rightNs":[{"id":10,"set":[1,2,3]}]})",
						R"({"id":3,"joined_rightNs":[{"id":10,"set":[1,2,3]},{"id":11,"set":[3,4,5]}]})",
						R"({"id":10})"
	};
	// clang-format on

	auto execQuery = [&results, this](Query& q) {
		auto qr = rt.Select(q);
		ASSERT_EQ(qr.Count(), results.size());
		int k = 0;
		for (auto it = qr.begin(); it != qr.end(); ++it, ++k) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			reindexer::WrSerializer ser;
			auto err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(ser.Slice(), results[k]);
		}
	};

	{
		auto q = Query(leftNs).Sort("id", SortOrder::Asc);
		reindexer::Query qj(rightNs);
		q.LeftJoin(std::move(qj), "id", CondSet, "set");
		reindexer::WrSerializer ser;
		execQuery(q);
	}

	auto sqlTestCase = [execQuery](const std::string& s) {
		Query q = Query::FromSQL(s);
		execQuery(q);
	};

	sqlTestCase(fmt::format("select * from {} left join {} on {}.id IN {}.set order by id", leftNs, rightNs, leftNs, rightNs));
	sqlTestCase(fmt::format("select * from {} left join {} on {}.set IN {}.id order by id", leftNs, rightNs, rightNs, leftNs));
	sqlTestCase(fmt::format("select * from {} left join {} on {}.id = {}.set order by id", leftNs, rightNs, leftNs, rightNs));
	sqlTestCase(fmt::format("select * from {} left join {} on {}.set = {}.id order by id", leftNs, rightNs, rightNs, leftNs));
}

TEST_F(JoinOnConditionsApi, TestInvalidConditions) {
	const std::vector<std::string> sqls = {
		R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages is null);)",
		R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages range(0, 1000));)",
		R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages in(1, 50, 100, 500, 1000, 1500));)",
	};
	for (const std::string& sql : sqls) {
		EXPECT_THROW(std::ignore = Query::FromSQL(sql), Error);
	}
	QueryResults qr;
	Error err = rt.reindexer->Select(Query(books_namespace).InnerJoin(Query(authors_namespace), authorid_fk, CondAllSet, authorid), qr);
	EXPECT_FALSE(err.ok());
	qr.Clear();
	err = rt.reindexer->Select(Query(books_namespace).InnerJoin(Query(authors_namespace), authorid_fk, CondLike, authorid), qr);
	EXPECT_FALSE(err.ok());
}

void CheckJoinIds(const std::map<int, std::vector<std::set<int>>>& ids, const reindexer::QueryResults& qr) {
	ASSERT_EQ(ids.size(), qr.Count());
	for (auto it : qr) {
		{
			const auto item = it.GetItem();
			const int id = item["id"].Get<int>();
			const auto idIt = ids.find(id);
			ASSERT_NE(idIt, ids.end()) << id;

			auto joinedCtx = it.GetJoinedContext();
			auto& joined = joinedCtx.iterator;
			const auto& joinedIds = idIt->second;
			ASSERT_EQ(joinedIds.size(), joined.GetFieldsCount());
			for (size_t i = 0; i < joinedIds.size(); ++i) {
				const auto& joinedFieldIt = joined.At(i);
				const auto& joinedIdsSet = joinedIds[i];
				ASSERT_EQ(joinedIds[i].size(), joinedFieldIt.ItemsCount());
				for (auto it : joinedFieldIt.ToQueryResults(joinedCtx)) {
					auto item{it.GetItem()};
					const int joinedId{item["id"].Get<int>()};
					EXPECT_NE(joinedIdsSet.find(joinedId), joinedIdsSet.end()) << joinedId;
				}
			}
		}

		{
			reindexer::WrSerializer ser;
			const auto err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			gason::JsonParser parser;
			const auto mainNode = parser.Parse(ser.Slice());
			const int id = mainNode["id"].As<int>();
			const auto idIt = ids.find(id);
			ASSERT_NE(idIt, ids.end()) << id;

			for (size_t i = 0, s = idIt->second.size(); i < s; ++i) {
				auto& joinedIds = idIt->second[i];
				const std::string joinedFieldName = "joined_" + (idIt->second.size() == 1 ? "" : std::to_string(i + 1) + '_') + "join_ns";
				size_t found = 0;
				for (const auto joinedNode : mainNode[joinedFieldName]) {
					const int joinedId = joinedNode["id"].As<int>();
					const auto joinedIdIt = joinedIds.find(joinedId);
					EXPECT_NE(joinedIdIt, joinedIds.end()) << joinedFieldName << ' ' << joinedId;
					found += (joinedIdIt != joinedIds.end());
				}
				EXPECT_EQ(joinedIds.size(), found) << joinedFieldName;
			}
		}
	}
}

TEST_F(JoinSelectsApi, SeveralJoinsByTheSameNs) {
	const std::string_view mainNs = "main_ns";
	const std::string_view joinNs = "join_ns";
	rt.OpenNamespace(mainNs);
	DefineNamespaceDataset(mainNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});

	rt.UpsertJSON(mainNs, R"({"id": 0, "join_id": 2})");
	rt.UpsertJSON(mainNs, R"({"id": 1, "join_id": 3})");

	rt.OpenNamespace(joinNs);
	DefineNamespaceDataset(joinNs, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});

	Item joinItem = NewItem(joinNs);
	joinItem["id"] = 0;
	Upsert(joinNs, joinItem);

	joinItem = NewItem(joinNs);
	joinItem["id"] = 1;
	Upsert(joinNs, joinItem);

	joinItem = NewItem(joinNs);
	joinItem["id"] = 2;
	Upsert(joinNs, joinItem);

	joinItem = NewItem(joinNs);
	joinItem["id"] = 3;
	Upsert(joinNs, joinItem);

	auto qr = rt.Select(Query(mainNs).InnerJoin(Query(joinNs), "id", CondEq, "id"));
	CheckJoinIds({{0, {{0}}}, {1, {{1}}}}, qr);

	qr = rt.Select(Query(mainNs).InnerJoin(Query(joinNs), "id", CondEq, "id").LeftJoin(Query(joinNs), "join_id", CondEq, "id"));
	CheckJoinIds({{0, {{0}, {2}}}, {1, {{1}, {3}}}}, qr);

	qr = rt.Select(Query(mainNs).InnerJoin(Query(joinNs), "id", CondEq, "id").LeftJoin(Query(joinNs), "join_id", CondGe, "id"));
	CheckJoinIds({{0, {{0}, {0, 1, 2}}}, {1, {{1}, {0, 1, 2, 3}}}}, qr);

	qr = rt.Select(Query(mainNs)
					   .InnerJoin(Query(joinNs), "id", CondEq, "id")
					   .LeftJoin(Query(joinNs), "join_id", CondGe, "id")
					   .LeftJoin(Query(joinNs), "join_id", CondEq, "id"));
	CheckJoinIds({{0, {{0}, {0, 1, 2}, {2}}}, {1, {{1}, {0, 1, 2, 3}, {3}}}}, qr);
}

}  // namespace reindexer_tests
