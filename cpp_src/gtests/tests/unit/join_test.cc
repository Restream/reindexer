#include <chrono>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include "core/itemimpl.h"
#include "core/nsselecter/joinedselector.h"
#include "join_selects_api.h"

TEST_F(JoinSelectsApi, JoinsAsWhereConditionsTest) {
	Query queryGenres = Query(genres_namespace).Not().Where(genreid, CondEq, 1);
	Query queryAuthors = Query(authors_namespace).Where(authorid, CondGe, 10).Where(authorid, CondLe, 25);
	Query queryAuthors2 = Query(authors_namespace).Where(authorid, CondGe, 300).Where(authorid, CondLe, 400);
	Query queryBooks = Query(books_namespace, 0, 50)
						   .OpenBracket()
						   .Where(price, CondGe, 9540)
						   .Where(price, CondLe, 9550)
						   .CloseBracket()
						   .Or()
						   .OpenBracket()
						   .Where(price, CondGe, 1000)
						   .Where(price, CondLe, 2000)
						   .InnerJoin(authorid_fk, authorid, CondEq, queryAuthors)
						   .OrInnerJoin(genreId_fk, genreid, CondEq, queryGenres)
						   .CloseBracket()
						   .Or()
						   .OpenBracket()
						   .Where(pages, CondEq, 0)
						   .CloseBracket()
						   .Or()
						   .InnerJoin(authorid_fk, authorid, CondEq, queryAuthors2);

	reindexer::QueryResults qr;
	Error err = rt.reindexer->Select(queryBooks, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() <= 50);
	CheckJoinsInComplexWhereCondition(qr);
}

TEST_F(JoinSelectsApi, JoinsAsWhereConditionsTest2) {
	string sql =
		"SELECT * FROM books_namespace WHERE "
		"(price >= 9540 AND price <= 9550) "
		"OR (price >= 1000 AND price <= 2000 INNER JOIN (SELECT * FROM authors_namespace WHERE authorid >= 10 AND authorid <= 25)ON "
		"authors_namespace.authorid = books_namespace.authorid_fk OR INNER JOIN (SELECT * FROM genres_namespace WHERE NOT genreid = 1) ON "
		"genres_namespace.genreid = books_namespace.genreid_fk) "
		"OR (pages = 0) "
		"OR INNER JOIN (SELECT *FROM authors_namespace WHERE authorid >= 300 AND authorid <= 400) ON authors_namespace.authorid = "
		"books_namespace.authorid_fk LIMIT 50";

	Query query;
	query.FromSQL(sql);
	reindexer::QueryResults qr;
	Error err = rt.reindexer->Select(query, qr);
	EXPECT_TRUE(err.ok()) << err.what();
	EXPECT_TRUE(qr.Count() <= 50);
	CheckJoinsInComplexWhereCondition(qr);
}

TEST_F(JoinSelectsApi, SqlPasringTest) {
	string sql =
		"select * from books_namespace where (pages > 0 and inner join (select * from authors_namespace limit 10) on "
		"authors_namespace.authorid = "
		"books_namespace.authorid_fk and price > 1000 or inner join (select * from genres_namespace limit 10) on "
		"genres_namespace.genreid = books_namespace.genreid_fk and pages < 10000 and inner join (select * from authors_namespace WHERE "
		"(authorid >= 10 AND authorid <= 20) limit 100) on "
		"authors_namespace.authorid = books_namespace.authorid_fk) or pages == 3 limit 20";

	Query srcQuery;
	srcQuery.FromSQL(sql);

	reindexer::WrSerializer wrser;
	srcQuery.GetSQL(wrser);

	Query dstQuery;
	dstQuery.FromSQL(wrser.Slice());

	ASSERT_TRUE(srcQuery == dstQuery);

	wrser.Reset();
	srcQuery.Serialize(wrser);
	Query deserializedQuery;
	reindexer::Serializer ser(wrser.Buf(), wrser.Len());
	deserializedQuery.Deserialize(ser);
	ASSERT_TRUE(srcQuery == deserializedQuery);
}

TEST_F(JoinSelectsApi, InnerJoinTest) {
	Query queryAuthors(authors_namespace);
	Query queryBooks = Query(books_namespace, 0, 10).Where(price, CondGe, 600);
	Query joinQuery = Query(queryBooks).InnerJoin(authorid_fk, authorid, CondEq, queryAuthors);

	reindexer::QueryResults joinQueryRes;
	Error err = rt.reindexer->Select(joinQuery, joinQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	err = VerifyResJSON(joinQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	reindexer::QueryResults pureSelectRes;
	err = rt.reindexer->Select(queryBooks, pureSelectRes);
	EXPECT_TRUE(err.ok()) << err.what();

	QueryResultRows joinSelectRows;
	QueryResultRows pureSelectRows;

	if (err.ok()) {
		for (auto it : pureSelectRes) {
			Item booksItem(it.GetItem());
			Variant authorIdKeyRef = booksItem[authorid_fk];

			reindexer::QueryResults authorsSelectRes;
			Query authorsQuery = Query(authors_namespace).Where(authorid, CondEq, authorIdKeyRef);
			err = rt.reindexer->Select(authorsQuery, authorsSelectRes);
			EXPECT_TRUE(err.ok()) << err.what();

			if (err.ok()) {
				int bookId = booksItem[bookid].Get<int>();
				QueryResultRow& pureSelectRow = pureSelectRows[bookId];

				FillQueryResultFromItem(booksItem, pureSelectRow);
				for (auto jit : authorsSelectRes) {
					Item authorsItem(jit.GetItem());
					FillQueryResultFromItem(authorsItem, pureSelectRow);
				}
			}
		}

		FillQueryResultRows(joinQueryRes, joinSelectRows);
		EXPECT_EQ(CompareQueriesResults(pureSelectRows, joinSelectRows), true);
	}
}

TEST_F(JoinSelectsApi, LeftJoinTest) {
	Query booksQuery = Query(books_namespace).Where(price, CondGe, 500);
	Query joinQuery = Query(authors_namespace).LeftJoin(authorid, authorid_fk, CondEq, booksQuery);

	reindexer::QueryResults booksQueryRes;
	Error err = rt.reindexer->Select(booksQuery, booksQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	QueryResultRows pureSelectRows;

	if (err.ok()) {
		for (auto it : booksQueryRes) {
			Item item(it.GetItem());
			BookId bookId = item[bookid].Get<int>();
			QueryResultRow& resultRow = pureSelectRows[bookId];
			FillQueryResultFromItem(item, resultRow);
		}
	}

	reindexer::QueryResults joinQueryRes;
	err = rt.reindexer->Select(joinQuery, joinQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	err = VerifyResJSON(joinQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	if (err.ok()) {
		std::unordered_set<int> presentedAuthorIds;
		std::unordered_map<int, int> rowidsIndexes;
		int i = 0;
		for (auto rowIt : joinQueryRes) {
			Item item(rowIt.GetItem());
			Variant authorIdKeyRef1 = item[authorid];
			const reindexer::ItemRef& rowid = rowIt.GetItemRef();

			auto itemIt = rowIt.GetJoinedItemsIterator();
			if (itemIt.getJoinedItemsCount() == 0) continue;
			for (auto joinedFieldIt = itemIt.begin(); joinedFieldIt != itemIt.end(); ++joinedFieldIt) {
				reindexer::ItemImpl item2(joinedFieldIt.GetItem(0, joinQueryRes.getPayloadType(1), joinQueryRes.getTagsMatcher(1)));
				Variant authorIdKeyRef2 = item2.GetField(joinQueryRes.getPayloadType(1).FieldByName(authorid_fk));
				EXPECT_TRUE(authorIdKeyRef1 == authorIdKeyRef2);
			}

			presentedAuthorIds.insert(static_cast<int>(authorIdKeyRef1));
			rowidsIndexes.insert({rowid.Id(), i});
			i++;
		}

		for (auto rowIt : joinQueryRes) {
			IdType rowid = rowIt.GetItemRef().Id();
			auto itemIt = rowIt.GetJoinedItemsIterator();
			if (itemIt.getJoinedItemsCount() == 0) continue;
			auto joinedFieldIt = itemIt.begin();
			for (int i = 0; i < joinedFieldIt.ItemsCount(); ++i) {
				reindexer::ItemImpl item(joinedFieldIt.GetItem(i, joinQueryRes.getPayloadType(1), joinQueryRes.getTagsMatcher(1)));

				Variant authorIdKeyRef1 = item.GetField(joinQueryRes.getPayloadType(1).FieldByName(authorid_fk));
				int authorId = static_cast<int>(authorIdKeyRef1);

				auto itAutorid(presentedAuthorIds.find(authorId));
				EXPECT_TRUE(itAutorid != presentedAuthorIds.end());

				auto itRowidIndex(rowidsIndexes.find(rowid));
				EXPECT_TRUE(itRowidIndex != rowidsIndexes.end());

				if (itRowidIndex != rowidsIndexes.end()) {
					Item item2((joinQueryRes.begin() + rowid).GetItem());
					Variant authorIdKeyRef2 = item2[authorid];
					EXPECT_TRUE(authorIdKeyRef1 == authorIdKeyRef2);
				}
			}
		}
	}
}

TEST_F(JoinSelectsApi, OrInnerJoinTest) {
	Query queryGenres(genres_namespace);
	Query queryAuthors(authors_namespace);
	Query queryBooks = Query(books_namespace, 0, 10).Where(price, CondGe, 500);
	Query innerJoinQuery = Query(queryBooks).InnerJoin(authorid_fk, authorid, CondEq, queryAuthors);
	Query orInnerJoinQuery = Query(innerJoinQuery).OrInnerJoin(genreId_fk, genreid, CondEq, queryGenres);

	const int authorsNsJoinIndex = 0;
	const int genresNsJoinIndex = 1;

	reindexer::QueryResults queryRes;
	Error err = rt.reindexer->Select(orInnerJoinQuery, queryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	err = VerifyResJSON(queryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	if (err.ok()) {
		for (auto rowIt : queryRes) {
			Item item(rowIt.GetItem());
			reindexer::joins::ItemIterator itemIt = rowIt.GetJoinedItemsIterator();

			reindexer::joins::JoinedFieldIterator authorIdIt = itemIt.at(authorsNsJoinIndex);
			Variant authorIdKeyRef1 = item[authorid_fk];
			for (int i = 0; i < authorIdIt.ItemsCount(); ++i) {
				reindexer::ItemImpl authorsItem(authorIdIt.GetItem(i, queryRes.getPayloadType(1), queryRes.getTagsMatcher(1)));
				Variant authorIdKeyRef2 = authorsItem.GetField(queryRes.getPayloadType(1).FieldByName(authorid));
				EXPECT_TRUE(authorIdKeyRef1 == authorIdKeyRef2);
			}

			reindexer::joins::JoinedFieldIterator genreIdIt = itemIt.at(genresNsJoinIndex);
			Variant genresIdKeyRef1 = item[genreId_fk];
			for (int i = 0; i < genreIdIt.ItemsCount(); ++i) {
				reindexer::ItemImpl genresItem = genreIdIt.GetItem(i, queryRes.getPayloadType(2), queryRes.getTagsMatcher(2));
				Variant genresIdKeyRef2 = genresItem.GetField(queryRes.getPayloadType(2).FieldByName(genreid));
				EXPECT_TRUE(genresIdKeyRef1 == genresIdKeyRef2);
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
		Query booksQuery = Query(books_namespace, 11, 1111).Where(pages, CondGe, 100).Where(price, CondGe, 200).Sort(price, true);
		Query joinQuery = Query(authors_namespace)
							  .Where(authorid, CondLe, 100)
							  .LeftJoin(authorid, authorid_fk, CondEq, booksQuery)
							  .Sort(age, false)
							  .Limit(10);

		reindexer::QueryResults joinQueryRes;
		Error err = rt.reindexer->Select(joinQuery, joinQueryRes);
		ASSERT_TRUE(err.ok()) << err.what();

		Variant prevField;
		for (auto rowIt : joinQueryRes) {
			Item item = rowIt.GetItem();
			if (prevField.Type() != KeyValueNull) {
				ASSERT_TRUE(prevField.Compare(item[age]) <= 0);
			}

			Variant key = item[authorid];
			auto itemIt = rowIt.GetJoinedItemsIterator();
			if (itemIt.getJoinedItemsCount() == 0) continue;
			auto joinedFieldIt = itemIt.begin();

			Variant prevJoinedValue;
			for (int i = 0; i < joinedFieldIt.ItemsCount(); ++i) {
				reindexer::ItemImpl joinItem(joinedFieldIt.GetItem(i, joinQueryRes.getPayloadType(1), joinQueryRes.getTagsMatcher(1)));
				Variant fkey = joinItem.GetField(joinQueryRes.getPayloadType(1).FieldByName(authorid_fk));
				ASSERT_TRUE(key.Compare(fkey) == 0) << key.As<string>() << " " << fkey.As<string>();
				Variant recentJoinedValue = joinItem.GetField(joinQueryRes.getPayloadType(1).FieldByName(price));
				ASSERT_TRUE(recentJoinedValue.As<int>() >= 200);
				if (prevJoinedValue.Type() != KeyValueNull) {
					ASSERT_TRUE(prevJoinedValue.Compare(recentJoinedValue) >= 0);
				}
				Variant pagesValue = joinItem.GetField(joinQueryRes.getPayloadType(1).FieldByName(pages));
				ASSERT_TRUE(pagesValue.As<int>() >= 100);
				prevJoinedValue = recentJoinedValue;
			}
			prevField = item[age];
		}
	}
}

TEST_F(JoinSelectsApi, JoinTestSelectNonIndexedField) {
	reindexer::QueryResults qr;
	Query authorsQuery = Query(authors_namespace);
	Error err = rt.reindexer->Select(Query(books_namespace)
										 .Where(rating, CondEq, Variant(static_cast<int64_t>(100)))
										 .InnerJoin(authorid_fk, authorid, CondEq, authorsQuery),
									 qr);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1) << err.what();

	Item theOnlyItem = qr[0].GetItem();
	VariantArray krefs = theOnlyItem[title];
	ASSERT_TRUE(krefs.size() == 1);
	ASSERT_TRUE(krefs[0].As<string>() == "Crime and Punishment");
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
								   .InnerJoin(authorid_fk, authorid, CondEq, authorsQuery),
							   qr);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1) << err.what();

	// And backwards even!
	reindexer::QueryResults qr2;
	Query testNsQuery = Query(default_namespace);
	err = rt.reindexer->Select(
		Query(authors_namespace).Where(authorid, CondEq, Variant(DostoevskyAuthorId)).InnerJoin(authorid, authorid_fk, CondEq, testNsQuery),
		qr2);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr2.Count() == 1) << err.what();
}

TEST_F(JoinSelectsApi, JoinsEasyStressTest) {
	auto selectTh = [this]() {
		Query queryGenres(genres_namespace);
		Query queryAuthors(authors_namespace);
		Query queryBooks = Query(books_namespace, 0, 10).Where(price, CondGe, 600).Sort(bookid, false);
		Query joinQuery1 = Query(queryBooks).InnerJoin(authorid_fk, authorid, CondEq, queryAuthors).Sort(pages, false);
		Query joinQuery2 = Query(joinQuery1).LeftJoin(authorid_fk, authorid, CondEq, queryAuthors);
		Query orInnerJoinQuery = Query(joinQuery2).OrInnerJoin(genreId_fk, genreid, CondEq, queryGenres).Sort(price, true).Limit(20);
		for (size_t i = 0; i < 10; ++i) {
			reindexer::QueryResults queryRes;
			Error err = rt.reindexer->Select(orInnerJoinQuery, queryRes);
			EXPECT_TRUE(err.ok()) << err.what();
			EXPECT_TRUE(queryRes.Count() > 0);
		}
	};

	auto removeTh = [this]() {
		QueryResults qres;
		Error err = rt.reindexer->Delete(Query(books_namespace, 0, 10).Where(price, CondGe, 5000), qres);
		EXPECT_TRUE(err.ok()) << err.what();
	};

	std::vector<std::thread> threads;
	for (size_t i = 0; i < 20; ++i) {
		threads.push_back(std::thread(selectTh));
		if (i % 2 == 0) threads.push_back(std::thread(removeTh));
		if (i % 4 == 0) threads.push_back(std::thread([this]() { FillBooksNamespace(1000); }));
	}
	for (size_t i = 0; i < threads.size(); ++i) threads[i].join();
}

TEST_F(JoinSelectsApi, JoinPreResultStoreValuesOptimizationStressTest) {
	using reindexer::JoinedSelector;
	static const string rightNs = "rightNs";
	static constexpr char const* data = "data";
	static constexpr int maxDataValue = 10;
	static constexpr int maxRightNsRowCount = maxDataValue * JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization();
	static constexpr int maxLeftNsRowCount = 10000;
	static constexpr size_t leftNsCount = 50;
	static vector<string> leftNs;
	if (leftNs.empty()) {
		leftNs.reserve(leftNsCount);
		for (size_t i = 0; i < leftNsCount; ++i) leftNs.push_back("leftNs" + std::to_string(i));
	}

	const auto createNs = [this](const string& ns) {
		Error err = rt.reindexer->OpenNamespace(ns);
		ASSERT_TRUE(err.ok()) << err.what();
		DefineNamespaceDataset(
			ns, {IndexDeclaration{id, "hash", "int", IndexOpts().PK(), 0}, IndexDeclaration{data, "hash", "int", IndexOpts(), 0}});
	};
	const auto fill = [this](const string& ns, int startId, int endId) {
		for (int i = startId; i < endId; ++i) {
			Item item = NewItem(ns);
			item[id] = i;
			item[data] = rand() % maxDataValue;
			Upsert(ns, item);
		}
		Commit(ns);
	};

	createNs(rightNs);
	fill(rightNs, 0, maxRightNsRowCount);
	std::atomic<bool> start{false};
	vector<std::thread> threads;
	threads.reserve(leftNs.size());
	for (size_t i = 0; i < leftNs.size(); ++i) {
		createNs(leftNs[i]);
		fill(leftNs[i], 0, maxLeftNsRowCount);
		threads.emplace_back([this, i, &start]() {
			// about 50% of queries will use the optimization
			Query q = Query(leftNs[i]).InnerJoin(data, data, CondEq, Query(rightNs).Where(data, CondEq, rand() % maxDataValue));
			QueryResults qres;
			while (!start) std::this_thread::sleep_for(std::chrono::milliseconds(1));
			Error err = rt.reindexer->Select(q, qres);
			EXPECT_TRUE(err.ok()) << err.what();
		});
	}
	start = true;
	for (auto& th : threads) th.join();
}
