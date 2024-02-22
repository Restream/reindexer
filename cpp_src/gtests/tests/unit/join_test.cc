#include <chrono>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include "core/itemimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "core/type_consts_helpers.h"
#include "join_on_conditions_api.h"
#include "join_selects_api.h"
#include "test_helpers.h"

TEST_F(JoinSelectsApi, JoinsAsWhereConditionsTest) {
	Query queryGenres{Query(genres_namespace).Not().Where(genreid, CondEq, 1)};
	Query queryAuthors{Query(authors_namespace).Where(authorid, CondGe, 10).Where(authorid, CondLe, 25)};
	Query queryAuthors2{Query(authors_namespace).Where(authorid, CondGe, 300).Where(authorid, CondLe, 400)};
	// clang-format off
	Query queryBooks{Query(books_namespace, 0, 50)
						 .OpenBracket()
							 .Where(price, CondGe, 9540)
							 .Where(price, CondLe, 9550)
						 .CloseBracket()
						 .Or().OpenBracket()
							 .Where(price, CondGe, 1000)
							 .Where(price, CondLe, 2000)
							 .InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors))
							 .OrInnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres))
						 .CloseBracket()
						 .Or().OpenBracket()
							 .Where(pages, CondEq, 0)
						 .CloseBracket()
						 .Or().InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors2))};
	// clang-format on

	QueryWatcher watcher{queryBooks};
	reindexer::QueryResults qr;
	Error err = rt.reindexer->Select(queryBooks, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_LE(qr.Count(), 50);
	CheckJoinsInComplexWhereCondition(qr);
}

TEST_F(JoinSelectsApi, JoinsLockWithCache_364) {
	Query queryGenres{Query(genres_namespace).Where(genreid, CondEq, 1)};
	Query queryBooks{Query(books_namespace, 0, 50).InnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres))};
	QueryWatcher watcher{queryBooks};
	TurnOnJoinCache(genres_namespace);

	for (int i = 0; i < 10; ++i) {
		reindexer::QueryResults qr;
		Error err = rt.reindexer->Select(queryBooks, qr);
		ASSERT_TRUE(err.ok()) << err.what();
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
	reindexer::QueryResults qr;
	Error err = rt.reindexer->Select(query, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_LE(qr.Count(), 50);
	CheckJoinsInComplexWhereCondition(qr);
}

TEST_F(JoinSelectsApi, SqlParsingTest) {
	std::string sql =
		"select * from books_namespace where (pages > 0 and inner join (select * from authors_namespace limit 10) on "
		"authors_namespace.authorid = "
		"books_namespace.authorid_fk and price > 1000 or inner join (select * from genres_namespace limit 10) on "
		"genres_namespace.genreid = books_namespace.genreid_fk and pages < 10000 and inner join (select * from authors_namespace WHERE "
		"(authorid >= 10 AND authorid <= 20) limit 100) on "
		"authors_namespace.authorid = books_namespace.authorid_fk) or pages == 3 limit 20";

	Query srcQuery = Query::FromSQL(sql);
	QueryWatcher watcher{srcQuery};

	reindexer::WrSerializer wrser;
	srcQuery.GetSQL(wrser);

	Query dstQuery = Query::FromSQL(wrser.Slice());

	ASSERT_EQ(srcQuery, dstQuery);

	wrser.Reset();
	srcQuery.Serialize(wrser);
	reindexer::Serializer ser(wrser.Buf(), wrser.Len());
	Query deserializedQuery = Query::Deserialize(ser);
	ASSERT_EQ(srcQuery, deserializedQuery) << "Original query:\n"
										   << srcQuery.GetSQL() << "\nDeserialized query:\n"
										   << deserializedQuery.GetSQL();
}

TEST_F(JoinSelectsApi, InnerJoinTest) {
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 600)};
	Query joinQuery = queryBooks.InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors));
	QueryWatcher watcher{joinQuery};

	reindexer::QueryResults joinQueryRes;
	Error err = rt.reindexer->Select(joinQuery, joinQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	err = VerifyResJSON(joinQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::QueryResults pureSelectRes;
	err = rt.reindexer->Select(queryBooks, pureSelectRes);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResultRows joinSelectRows;
	QueryResultRows pureSelectRows;

	if (err.ok()) {
		for (auto it : pureSelectRes) {
			Item booksItem(it.GetItem(false));
			Variant authorIdKeyRef = booksItem[authorid_fk];

			reindexer::QueryResults authorsSelectRes;
			Query authorsQuery{Query(authors_namespace).Where(authorid, CondEq, authorIdKeyRef)};
			err = rt.reindexer->Select(authorsQuery, authorsSelectRes);
			ASSERT_TRUE(err.ok()) << err.what();

			if (err.ok()) {
				int bookId = booksItem[bookid].Get<int>();
				QueryResultRow& pureSelectRow = pureSelectRows[bookId];

				FillQueryResultFromItem(booksItem, pureSelectRow);
				for (auto jit : authorsSelectRes) {
					Item authorsItem(jit.GetItem(false));
					FillQueryResultFromItem(authorsItem, pureSelectRow);
				}
			}
		}

		FillQueryResultRows(joinQueryRes, joinSelectRows);
		EXPECT_EQ(CompareQueriesResults(pureSelectRows, joinSelectRows), true);
	}
}

TEST_F(JoinSelectsApi, LeftJoinTest) {
	Query booksQuery{Query(books_namespace).Where(price, CondGe, 500)};
	reindexer::QueryResults booksQueryRes;
	Error err = rt.reindexer->Select(booksQuery, booksQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	QueryResultRows pureSelectRows;
	if (err.ok()) {
		for (auto it : booksQueryRes) {
			Item item(it.GetItem(false));
			BookId bookId = item[bookid].Get<int>();
			QueryResultRow& resultRow = pureSelectRows[bookId];
			FillQueryResultFromItem(item, resultRow);
		}
	}

	Query joinQuery{Query(authors_namespace).LeftJoin(authorid, authorid_fk, CondEq, std::move(booksQuery))};

	QueryWatcher watcher{joinQuery};
	reindexer::QueryResults joinQueryRes;
	err = rt.reindexer->Select(joinQuery, joinQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	err = VerifyResJSON(joinQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	if (err.ok()) {
		std::unordered_set<int> presentedAuthorIds;
		std::unordered_map<int, int> rowidsIndexes;
		int i = 0;
		for (auto rowIt : joinQueryRes.ToLocalQr()) {
			Item item(rowIt.GetItem(false));
			Variant authorIdKeyRef1 = item[authorid];
			const reindexer::ItemRef& rowid = rowIt.GetItemRef();

			auto itemIt = rowIt.GetJoined();
			if (itemIt.getJoinedItemsCount() == 0) continue;
			for (auto joinedFieldIt = itemIt.begin(); joinedFieldIt != itemIt.end(); ++joinedFieldIt) {
				reindexer::ItemImpl item2(joinedFieldIt.GetItem(0, joinQueryRes.GetPayloadType(1), joinQueryRes.GetTagsMatcher(1)));
				Variant authorIdKeyRef2 = item2.GetField(joinQueryRes.GetPayloadType(1).FieldByName(authorid_fk));
				EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
			}

			presentedAuthorIds.insert(static_cast<int>(authorIdKeyRef1));
			rowidsIndexes.insert({rowid.Id(), i});
			i++;
		}

		for (auto rowIt : joinQueryRes.ToLocalQr()) {
			IdType rowid = rowIt.GetItemRef().Id();
			auto itemIt = rowIt.GetJoined();
			if (itemIt.getJoinedItemsCount() == 0) continue;
			auto joinedFieldIt = itemIt.begin();
			for (int i = 0; i < joinedFieldIt.ItemsCount(); ++i) {
				reindexer::ItemImpl item(joinedFieldIt.GetItem(i, joinQueryRes.GetPayloadType(1), joinQueryRes.GetTagsMatcher(1)));

				Variant authorIdKeyRef1 = item.GetField(joinQueryRes.GetPayloadType(1).FieldByName(authorid_fk));
				int authorId = static_cast<int>(authorIdKeyRef1);

				auto itAutorid(presentedAuthorIds.find(authorId));
				EXPECT_NE(itAutorid, presentedAuthorIds.end());

				auto itRowidIndex(rowidsIndexes.find(rowid));
				EXPECT_NE(itRowidIndex, rowidsIndexes.end());

				if (itRowidIndex != rowidsIndexes.end()) {
					Item item2((joinQueryRes.begin() + rowid).GetItem(false));
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
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 500)};
	Query innerJoinQuery = std::move(queryBooks.InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors)));
	Query orInnerJoinQuery = std::move(innerJoinQuery.OrInnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres)));
	QueryWatcher watcher{orInnerJoinQuery};

	const int authorsNsJoinIndex = 0;
	const int genresNsJoinIndex = 1;

	reindexer::QueryResults queryRes;
	Error err = rt.reindexer->Select(orInnerJoinQuery, queryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	err = VerifyResJSON(queryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	if (err.ok()) {
		for (auto rowIt : queryRes) {
			Item item(rowIt.GetItem(false));
			auto itemIt = rowIt.GetJoined();

			reindexer::joins::JoinedFieldIterator authorIdIt = itemIt.at(authorsNsJoinIndex);
			Variant authorIdKeyRef1 = item[authorid_fk];
			for (int i = 0; i < authorIdIt.ItemsCount(); ++i) {
				reindexer::ItemImpl authorsItem(authorIdIt.GetItem(i, queryRes.GetPayloadType(1), queryRes.GetTagsMatcher(1)));
				Variant authorIdKeyRef2 = authorsItem.GetField(queryRes.GetPayloadType(1).FieldByName(authorid));
				EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
			}

			reindexer::joins::JoinedFieldIterator genreIdIt = itemIt.at(genresNsJoinIndex);
			Variant genresIdKeyRef1 = item[genreId_fk];
			for (int i = 0; i < genreIdIt.ItemsCount(); ++i) {
				reindexer::ItemImpl genresItem = genreIdIt.GetItem(i, queryRes.GetPayloadType(2), queryRes.GetTagsMatcher(2));
				Variant genresIdKeyRef2 = genresItem.GetField(queryRes.GetPayloadType(2).FieldByName(genreid));
				EXPECT_EQ(genresIdKeyRef1, genresIdKeyRef2);
			}
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
		Query booksQuery{Query(books_namespace, 11, 1111).Where(pages, CondGe, 100).Where(price, CondGe, 200).Sort(price, true)};
		Query joinQuery{Query(authors_namespace)
							.Where(authorid, CondLe, 100)
							.LeftJoin(authorid, authorid_fk, CondEq, std::move(booksQuery))
							.Sort(age, false)
							.Limit(10)};

		QueryWatcher watcher{joinQuery};
		reindexer::QueryResults joinQueryRes;
		Error err = rt.reindexer->Select(joinQuery, joinQueryRes);
		ASSERT_TRUE(err.ok()) << err.what();

		Variant prevField;
		for (auto rowIt : joinQueryRes) {
			Item item = rowIt.GetItem(false);
			if (!prevField.Type().Is<reindexer::KeyValueType::Null>()) {
				ASSERT_LE(prevField.Compare(item[age]), 0);
			}

			Variant key = item[authorid];
			auto itemIt = rowIt.GetJoined();
			if (itemIt.getJoinedItemsCount() == 0) continue;
			auto joinedFieldIt = itemIt.begin();

			Variant prevJoinedValue;
			for (int i = 0; i < joinedFieldIt.ItemsCount(); ++i) {
				reindexer::ItemImpl joinItem(joinedFieldIt.GetItem(i, joinQueryRes.GetPayloadType(1), joinQueryRes.GetTagsMatcher(1)));
				Variant fkey = joinItem.GetField(joinQueryRes.GetPayloadType(1).FieldByName(authorid_fk));
				ASSERT_EQ(key.Compare(fkey), 0) << key.As<std::string>() << " " << fkey.As<std::string>();
				Variant recentJoinedValue = joinItem.GetField(joinQueryRes.GetPayloadType(1).FieldByName(price));
				ASSERT_GE(recentJoinedValue.As<int>(), 200);
				if (!prevJoinedValue.Type().Is<reindexer::KeyValueType::Null>()) {
					ASSERT_GE(prevJoinedValue.Compare(recentJoinedValue), 0);
				}
				Variant pagesValue = joinItem.GetField(joinQueryRes.GetPayloadType(1).FieldByName(pages));
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
					 .LeftJoin(authorid, authorid_fk, CondEq, std::move(joinedQuery1))
					 .Sort(books_namespace + '.' + price, false)};

	reindexer::QueryResults joinQueryRes1;
	Error err = rt.reindexer->Select(query1, joinQueryRes1);
	// several book to one author, cannot sort
	ASSERT_FALSE(err.ok());
	EXPECT_EQ(err.what(), "Not found value joined from ns books_namespace");

	Query joinedQuery2 = Query(authors_namespace);
	Query query2{Query(books_namespace)
					 .InnerJoin(authorid_fk, authorid, CondEq, std::move(joinedQuery2))
					 .Sort(authors_namespace + '.' + age, false)};

	QueryWatcher watcher{query2};
	reindexer::QueryResults joinQueryRes2;
	err = rt.reindexer->Select(query2, joinQueryRes2);
	ASSERT_TRUE(err.ok()) << err.what();

	Variant prevValue;
	for (auto rowIt : joinQueryRes2) {
		const auto itemIt = rowIt.GetJoined();
		ASSERT_EQ(itemIt.getJoinedItemsCount(), 1);
		const auto joinedFieldIt = itemIt.begin();
		reindexer::ItemImpl joinItem(joinedFieldIt.GetItem(0, joinQueryRes2.GetPayloadType(1), joinQueryRes2.GetTagsMatcher(1)));
		const Variant recentValue = joinItem.GetField(joinQueryRes2.GetPayloadType(1).FieldByName(age));
		if (!prevValue.Type().Is<reindexer::KeyValueType::Null>()) {
			reindexer::WrSerializer ser;
			ASSERT_LE(prevValue.Compare(recentValue), 0) << (prevValue.Dump(ser), ser << ' ', recentValue.Dump(ser), ser.Slice());
		}
		prevValue = recentValue;
	}
}

TEST_F(JoinSelectsApi, JoinTestSelectNonIndexedField) {
	reindexer::QueryResults qr;
	Query authorsQuery = Query(authors_namespace);
	Error err = rt.reindexer->Select(Query(books_namespace)
										 .Where(rating, CondEq, Variant(static_cast<int64_t>(100)))
										 .InnerJoin(authorid_fk, authorid, CondEq, std::move(authorsQuery)),
									 qr);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	Item theOnlyItem = qr.begin().GetItem(false);
	VariantArray krefs = theOnlyItem[title];
	ASSERT_EQ(krefs.size(), 1);
	ASSERT_EQ(krefs[0].As<std::string>(), "Crime and Punishment");
}

TEST_F(JoinSelectsApi, JoinByNonIndexedField) {
	Error err = rt.reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK(), 0}});

	std::stringstream json;
	json << "{" << addQuotes(id) << ":" << 1 << "," << addQuotes(authorid_fk) << ":" << DostoevskyAuthorId << "}";
	Item lonelyItem = NewItem(default_namespace);
	ASSERT_TRUE(lonelyItem.Status().ok()) << lonelyItem.Status().what();

	err = lonelyItem.FromJSON(json.str());
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Upsert(default_namespace, lonelyItem);
	ASSERT_TRUE(err.ok()) << err.what();

	err = rt.reindexer->Commit(books_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::QueryResults qr;
	Query authorsQuery = Query(authors_namespace);
	err = rt.reindexer->Select(Query(default_namespace)
								   .Where(authorid_fk, CondEq, Variant(DostoevskyAuthorId))
								   .InnerJoin(authorid_fk, authorid, CondEq, std::move(authorsQuery)),
							   qr);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr.Count(), 1);

	// And backwards even!
	reindexer::QueryResults qr2;
	Query testNsQuery = Query(default_namespace);
	err = rt.reindexer->Select(Query(authors_namespace)
								   .Where(authorid, CondEq, Variant(DostoevskyAuthorId))
								   .InnerJoin(authorid, authorid_fk, CondEq, std::move(testNsQuery)),
							   qr2);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_EQ(qr2.Count(), 1);
}

TEST_F(JoinSelectsApi, JoinsEasyStressTest) {
	auto selectTh = [this]() {
		Query queryGenres(genres_namespace);
		Query queryAuthors(authors_namespace);
		Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 600).Sort(bookid, false)};
		Query joinQuery1 = std::move(queryBooks.InnerJoin(authorid_fk, authorid, CondEq, queryAuthors).Sort(pages, false));
		Query joinQuery2 = std::move(joinQuery1.LeftJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors)));
		Query orInnerJoinQuery =
			std::move(joinQuery2.OrInnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres)).Sort(price, true).Limit(20));
		for (size_t i = 0; i < 10; ++i) {
			reindexer::QueryResults queryRes;
			Error err = rt.reindexer->Select(orInnerJoinQuery, queryRes);
			ASSERT_TRUE(err.ok()) << err.what();
			EXPECT_GT(queryRes.Count(), 0);
		}
	};

	auto removeTh = [this]() {
		QueryResults qres;
		Error err = rt.reindexer->Delete(Query(books_namespace, 0, 10).Where(price, CondGe, 5000), qres);
		ASSERT_TRUE(err.ok()) << err.what();
	};

	int32_t since = 0, count = 1000;
	std::vector<std::thread> threads;
	for (size_t i = 0; i < 20; ++i) {
		threads.push_back(std::thread(selectTh));
		if (i % 2 == 0) threads.push_back(std::thread(removeTh));
		if (i % 4 == 0) threads.push_back(std::thread([this, since, count]() { FillBooksNamespace(since, count); }));
		since += 1000;
	}
	for (size_t i = 0; i < threads.size(); ++i) threads[i].join();
}

TEST_F(JoinSelectsApi, JoinPreResultStoreValuesOptimizationStressTest) {
	using reindexer::JoinedSelector;
	static const std::string rightNs = "rightNs";
	static constexpr char const* data = "data";
	static constexpr int maxDataValue = 10;
	static constexpr int maxRightNsRowCount = maxDataValue * JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization();
	static constexpr int maxLeftNsRowCount = 10000;
	static constexpr size_t leftNsCount = 50;
	static std::vector<std::string> leftNs;
	if (leftNs.empty()) {
		leftNs.reserve(leftNsCount);
		for (size_t i = 0; i < leftNsCount; ++i) leftNs.push_back("leftNs" + std::to_string(i));
	}

	const auto createNs = [this](const std::string& ns) {
		Error err = rt.reindexer->OpenNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what();
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
		const auto err = Commit(ns);
		ASSERT_TRUE(err.ok()) << err.what();
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
			Query q{Query(leftNs[i]).InnerJoin(data, data, CondEq, Query(rightNs).Where(data, CondEq, rand() % maxDataValue))};
			QueryResults qres;
			while (!start) std::this_thread::sleep_for(std::chrono::milliseconds(1));
			Error err = rt.reindexer->Select(q, qres);
			ASSERT_TRUE(err.ok()) << err.what();
		});
	}
	start = true;
	for (auto& th : threads) th.join();
}

static void checkForAllowedJsonTags(const std::vector<std::string>& tags, gason::JsonValue jsonValue) {
	size_t count = 0;
	for (const auto& elem : jsonValue) {
		ASSERT_NE(std::find(tags.begin(), tags.end(), std::string_view(elem.key)), tags.end());
		++count;
	}
	ASSERT_EQ(count, tags.size());
}

TEST_F(JoinSelectsApi, JoinWithSelectFilter) {
	Query queryAuthors = Query(authors_namespace).Select({name, age});

	Query queryBooks{Query(books_namespace)
						 .Where(pages, CondGe, 100)
						 .InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors))
						 .Select({title, price})};

	QueryResults qr;
	Error err = rt.reindexer->Select(queryBooks, qr);
	ASSERT_TRUE(err.ok()) << err.what();

	for (auto it : qr) {
		ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		reindexer::WrSerializer wrser;
		err = it.GetJSON(wrser, false);
		ASSERT_TRUE(err.ok()) << err.what();

		reindexer::joins::ItemIterator joinIt = it.GetJoined();
		gason::JsonParser jsonParser;
		gason::JsonNode root = jsonParser.Parse(reindexer::giftStr(wrser.Slice()));
		checkForAllowedJsonTags({title, price, "joined_authors_namespace"}, root.value);

		for (auto fieldIt = joinIt.begin(); fieldIt != joinIt.end(); ++fieldIt) {
			LocalQueryResults jqr = fieldIt.ToQueryResults();
			jqr.addNSContext(qr, 1, reindexer::lsn_t());
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
	Query queryBooks = Query(books_namespace);
	queryBooks.InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace));

	// Build the 2nd query (with join) with 'authors_namespace' as the main NS.
	Query queryAuthors = Query(authors_namespace);
	queryAuthors.LeftJoin(locationid_fk, locationid, CondEq, Query(location_namespace));
	queryBooks.Merge(std::move(queryAuthors));

	// Execute it
	QueryResults qr;
	Error err = rt.reindexer->Select(queryBooks, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	VerifyResJSON(qr);

	// Make sure results are correct:
	// values of main and joined namespaces match
	// in both parts of the query.
	size_t rowId = 0;
	for (auto it : qr) {
		Item item = it.GetItem(false);
		auto joined = it.GetJoined();
		ASSERT_EQ(joined.getJoinedFieldsCount(), 1);

		bool booksItem = (rowId <= 10000);
		LocalQueryResults jqr = joined.begin().ToQueryResults();
		int joinedNs = booksItem ? 2 : 3;
		jqr.addNSContext(qr, joinedNs, reindexer::lsn_t());

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

// Check JOINs nested into the other JOINs (expecting errors)
TEST_F(JoinSelectsApi, TestNestedJoinsError) {
	constexpr char sqlPattern[] =
		R"(select * from books_namespace %s (select * from authors_namespace %s (select * from books_namespace) on authors_namespace.authorid = books_namespace.authorid_fk) on authors_namespace.authorid = books_namespace.authorid_fk)";
	auto joinTypes = {"inner join", "join", "left join"};
	for (auto& firstJoin : joinTypes) {
		for (auto& secondJoin : joinTypes) {
			auto sql = fmt::sprintf(sqlPattern, firstJoin, secondJoin);
			ValidateQueryThrow(sql, errParseSQL, "Expected ')', but found .*, line: 1 column: .*");
		}
	}
}

// Check MERGEs nested into the JOINs (expecting errors)
TEST_F(JoinSelectsApi, TestNestedMergesInJoinsError) {
	constexpr char sqlPattern[] =
		R"(select * from books_namespace %s (select * from authors_namespace  merge (select * from books_namespace)) on authors_namespace.authorid = books_namespace.authorid_fk)";
	auto joinTypes = {"inner join", "join", "left join"};
	for (auto& join : joinTypes) {
		auto sql = fmt::sprintf(sqlPattern, join);
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
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondLe, 100)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 200)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondLe, 400)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 400)).Limit(10)};

	SetQueriesCacheHitsCount(1);
	for (auto& bq : kBaseQueries) {
		const Query cachedTotalNoCondQ = Query(bq).CachedTotal();
		const Query totalCountNoCondQ = Query(bq).ReqTotal();
		QueryResults qrRegular;
		auto err = rt.reindexer->Select(totalCountNoCondQ, qrRegular);
		ASSERT_TRUE(err.ok()) << err.what() << "; " << totalCountNoCondQ.GetSQL();
		// Run all the queries with CountCached twice to check main and cached values
		for (int i = 0; i < 2; ++i) {
			QueryResults qrCached;
			err = rt.reindexer->Select(cachedTotalNoCondQ, qrCached);
			ASSERT_TRUE(err.ok()) << err.what() << "; i = " << i << "; " << cachedTotalNoCondQ.GetSQL();
			EXPECT_EQ(qrCached.TotalCount(), qrRegular.TotalCount()) << " i = " << i << "; " << bq.GetSQL();
		}
	}
}

TEST_F(JoinSelectsApi, CountCachedWithJoinNsUpdates) {
	const Genre kLastGenre = *genres.rbegin();
	const std::vector<Query> kBaseQueries = {
		Query(books_namespace)
			.InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 100))
			.OrInnerJoin(genreId_fk, genreid, CondEq,
						 Query(genres_namespace)
							 .Where(genrename, CondSet,
									{Variant{"non fiction"}, Variant{"poetry"}, Variant{"documentary"}, Variant{kLastGenre.name}}))
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 100))
			.InnerJoin(genreId_fk, genreid, CondEq,
					   Query(genres_namespace)
						   .Where(genrename, CondSet,
								  {Variant{"non fiction"}, Variant{"poetry"}, Variant{"documentary"}, Variant{kLastGenre.name}}))
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 100))
			.OpenBracket()
			.InnerJoin(genreId_fk, genreid, CondEq,
					   Query(genres_namespace).Where(genrename, CondSet, {Variant{"non fiction"}, Variant{kLastGenre.name}}))
			.OrInnerJoin(
				genreId_fk, genreid, CondEq,
				Query(genres_namespace).Where(genrename, CondSet, {Variant{"poetry"}, Variant{"documentary"}, Variant{kLastGenre.name}}))
			.CloseBracket()
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 100))
			.OpenBracket()
			.InnerJoin(genreId_fk, genreid, CondEq,
					   Query(genres_namespace).Where(genrename, CondSet, {Variant{"non fiction"}, Variant{kLastGenre.name}}))
			.InnerJoin(genreId_fk, genreid, CondEq, Query(genres_namespace))
			.CloseBracket()
			.Limit(10),
		Query(books_namespace)
			.InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 100))
			.OpenBracket()
			.InnerJoin(genreId_fk, genreid, CondEq,
					   Query(genres_namespace).Where(genrename, CondSet, {Variant{"non fiction"}, Variant{kLastGenre.name}}))
			.OrInnerJoin(genreId_fk, genreid, CondEq, Query(genres_namespace))
			.CloseBracket()
			.Limit(10)};

	SetQueriesCacheHitsCount(1);
	for (auto& bq : kBaseQueries) {
		const Query cachedTotalNoCondQ = Query(bq).CachedTotal();
		const Query totalCountNoCondQ = Query(bq).ReqTotal();
		auto checkQuery = [&](std::string_view step) {
			// With Initial data
			QueryResults qrRegular;
			auto err = rt.reindexer->Select(totalCountNoCondQ, qrRegular);
			ASSERT_TRUE(err.ok()) << err.what() << "; step: " << step << "; " << totalCountNoCondQ.GetSQL();
			// Run all the queries with CountCached twice to check main and cached values
			for (int i = 0; i < 2; ++i) {
				QueryResults qrCached;
				err = rt.reindexer->Select(cachedTotalNoCondQ, qrCached);
				ASSERT_TRUE(err.ok()) << err.what() << "; step: " << step << "; i = " << i << "; " << cachedTotalNoCondQ.GetSQL();
				EXPECT_EQ(qrCached.TotalCount(), qrRegular.TotalCount()) << "step: " << step << "; i = " << i << "; " << bq.GetSQL();
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
		R"(select * from books_namespace inner join books_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages %s books_namespace.pages);)";
	for (CondType condition : {CondLt, CondLe, CondGt, CondGe, CondEq}) {
		Query queryBooks = Query::FromSQL(GetSql(sqlTemplate, condition));
		QueryResults qr;
		Error err = rt.reindexer->Select(queryBooks, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		for (auto it : qr) {
			const auto item = it.GetItem();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			const Variant authorid1 = item[authorid_fk];
			const Variant pages1 = item[pages];
			const auto joined = it.GetJoined();
			ASSERT_EQ(joined.getJoinedFieldsCount(), 1);
			LocalQueryResults jqr = joined.begin().ToQueryResults();
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
		{R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk %s authors_namespace.authorid);)",
		 R"(select * from books_namespace inner join authors_namespace on (authors_namespace.authorid %s books_namespace.authorid_fk);)"}};
	const std::vector<std::pair<CondType, CondType>> conditions = {{CondLt, CondGt}, {CondLe, CondGe}, {CondGt, CondLt},
																   {CondGe, CondLe}, {CondEq, CondEq}, {CondSet, CondSet}};
	for (size_t i = 0; i < sqlTemplates.size(); ++i) {
		const auto& sqlTemplate = sqlTemplates[i];
		for (const auto& condition : conditions) {
			Query query1 = Query::FromSQL(GetSql(sqlTemplate.first, condition.first));
			QueryResults qr1;
			Error err = rt.reindexer->Select(query1, qr1);
			ASSERT_TRUE(err.ok()) << err.what();

			Query query2 = Query::FromSQL(GetSql(sqlTemplate.second, condition.second));
			QueryResults qr2;
			err = rt.reindexer->Select(query2, qr2);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(query1.GetJSON(), query2.GetJSON());
			ASSERT_EQ(qr1.Count(), qr2.Count());
			for (QueryResults::Iterator it1 = qr1.begin(), it2 = qr2.begin(); it1 != qr1.end(); ++it1, ++it2) {
				auto item1 = it1.GetItem();
				ASSERT_TRUE(item1.Status().ok()) << item1.Status().what();
				auto joined1 = it1.GetJoined();
				ASSERT_EQ(joined1.getJoinedFieldsCount(), 1);
				LocalQueryResults jqr1 = joined1.begin().ToQueryResults();
				jqr1.addNSContext(qr1, 1, reindexer::lsn_t());

				auto item2 = it2.GetItem();
				ASSERT_TRUE(item2.Status().ok()) << item2.Status().what();
				auto joined2 = it2.GetJoined();
				ASSERT_EQ(joined2.getJoinedFieldsCount(), 1);
				LocalQueryResults jqr2 = joined2.begin().ToQueryResults();
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
	const std::vector<std::string> results = {
						R"({"id":1,"joined_rightNs":[{"id":10,"set":[1,2,3]}]})",
						R"({"id":3,"joined_rightNs":[{"id":10,"set":[1,2,3]},{"id":11,"set":[3,4,5]}]})",
						R"({"id":10})"
	};
	// clang-format on

	auto execQuery = [&results, this](Query& q) {
		QueryResults qr;
		Error err = rt.reindexer->Select(q, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_EQ(qr.Count(), results.size());
		int k = 0;
		for (auto it = qr.begin(); it != qr.end(); ++it, ++k) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			reindexer::WrSerializer ser;
			err = it.GetJSON(ser, false);
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_EQ(ser.c_str(), results[k]);
		}
	};

	{
		Query q(leftNs);
		q.Sort("id", false);
		reindexer::Query qj(rightNs);
		q.LeftJoin("id", "set", CondSet, qj);
		reindexer::WrSerializer ser;
		execQuery(q);
	}

	auto sqlTestCase = [execQuery](const std::string& s) {
		Query q = Query::FromSQL(s);
		execQuery(q);
	};

	sqlTestCase(fmt::sprintf("select * from %s left join %s on %s.id IN %s.set order by id", leftNs, rightNs, leftNs, rightNs));
	sqlTestCase(fmt::sprintf("select * from %s left join %s on %s.set IN %s.id order by id", leftNs, rightNs, rightNs, leftNs));
	sqlTestCase(fmt::sprintf("select * from %s left join %s on %s.id = %s.set order by id", leftNs, rightNs, leftNs, rightNs));
	sqlTestCase(fmt::sprintf("select * from %s left join %s on %s.set = %s.id order by id", leftNs, rightNs, rightNs, leftNs));
}

TEST_F(JoinOnConditionsApi, TestInvalidConditions) {
	const std::vector<std::string> sqls = {
		R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages is null);)",
		R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages range(0, 1000));)",
		R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk = books_namespace.authorid_fk and books_namespace.pages in(1, 50, 100, 500, 1000, 1500));)",
	};
	for (const std::string& sql : sqls) {
		EXPECT_THROW((void)Query::FromSQL(sql), Error);
	}
	QueryResults qr;
	Error err = rt.reindexer->Select(Query(books_namespace).InnerJoin(authorid_fk, authorid, CondAllSet, Query(authors_namespace)), qr);
	EXPECT_FALSE(err.ok());
	qr.Clear();
	err = rt.reindexer->Select(Query(books_namespace).InnerJoin(authorid_fk, authorid, CondLike, Query(authors_namespace)), qr);
	EXPECT_FALSE(err.ok());
}
