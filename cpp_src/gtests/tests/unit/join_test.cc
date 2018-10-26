#include <unordered_map>
#include <unordered_set>
#include "join_selects_api.h"

TEST_F(JoinSelectsApi, InnerJoinTest) {
	Query queryAuthors(authors_namespace);
	Query queryBooks = Query(books_namespace, 0, 10).Where(price, CondGe, 600);
	Query joinQuery = Query(queryBooks).InnerJoin(authorid_fk, authorid, CondEq, queryAuthors);

	reindexer::QueryResults joinQueryRes;
	Error err = reindexer->Select(joinQuery, joinQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	int status = ParseItemJsonWithJoins(joinQueryRes);
	EXPECT_EQ(status, JSON_OK) << "Error parsing json - status " << status;

	reindexer::QueryResults pureSelectRes;
	err = reindexer->Select(queryBooks, pureSelectRes);
	EXPECT_TRUE(err.ok()) << err.what();

	QueryResultRows joinSelectRows;
	QueryResultRows pureSelectRows;

	if (err.ok()) {
		for (auto it : pureSelectRes) {
			Item booksItem(it.GetItem());
			Variant authorIdKeyRef = booksItem[authorid_fk];

			reindexer::QueryResults authorsSelectRes;
			Query authorsQuery = Query(authors_namespace).Where(authorid, CondEq, authorIdKeyRef);
			err = reindexer->Select(authorsQuery, authorsSelectRes);
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
	Error err = reindexer->Select(booksQuery, booksQueryRes);
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
	err = reindexer->Select(joinQuery, joinQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	int status = ParseItemJsonWithJoins(joinQueryRes);
	EXPECT_EQ(status, JSON_OK) << "Error parsing json - status " << status;

	if (err.ok()) {
		std::unordered_set<int> presentedAuthorIds;
		std::unordered_map<int, int> rowidsIndexes;
		int i = 0;
		for (auto rowIt : joinQueryRes) {
			Item item(rowIt.GetItem());
			Variant authorIdKeyRef1 = item[authorid];
			const reindexer::ItemRef& rowid = rowIt.GetItemRef();
			const reindexer::QRVector& queryResults = rowIt.GetJoined();
			for (const QueryResults& queryRes : queryResults) {
				Item item2(queryRes.begin().GetItem());
				Variant authorIdKeyRef2 = item2[authorid_fk];
				EXPECT_TRUE(authorIdKeyRef1 == authorIdKeyRef2);
			}

			presentedAuthorIds.insert(static_cast<int>(authorIdKeyRef1));
			rowidsIndexes.insert({rowid.id, i});
			i++;
		}

		for (const std::pair<const IdType, reindexer::QRVector>& itempair : joinQueryRes.joined_[0]) {
			if (itempair.second.empty()) continue;
			const QueryResults& joinedQueryRes(itempair.second[0]);
			for (auto it : joinedQueryRes) {
				Item item(it.GetItem());

				Variant authorIdKeyRef1 = item[authorid_fk];
				int authorId = static_cast<int>(authorIdKeyRef1);

				auto itAutorid(presentedAuthorIds.find(authorId));
				EXPECT_TRUE(itAutorid != presentedAuthorIds.end());

				int rowid(itempair.first);
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
	Error err = reindexer->Select(orInnerJoinQuery, queryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	int status = ParseItemJsonWithJoins(queryRes);
	EXPECT_EQ(status, JSON_OK) << "Error parsing json - status " << status;

	if (err.ok()) {
		for (auto rowIt : queryRes) {
			Item item(rowIt.GetItem());
			const reindexer::QRVector& joinedResult = rowIt.GetJoined();

			const QueryResults& authorNsJoinResults = joinedResult[authorsNsJoinIndex];
			const QueryResults& genresNsJoinResults = joinedResult[genresNsJoinIndex];

			Variant authorIdKeyRef1 = item[authorid_fk];
			for (auto jit : authorNsJoinResults) {
				Item authorsItem(jit.GetItem());
				Variant authorIdKeyRef2 = authorsItem[authorid];
				EXPECT_TRUE(authorIdKeyRef1 == authorIdKeyRef2);
			}

			Variant genresIdKeyRef1 = item[genreId_fk];
			for (auto jit : genresNsJoinResults) {
				VariantArray genreIdKeyRef;
				Item genresItem(jit.GetItem());
				Variant genresIdKeyRef2 = genresItem[genreid];
				EXPECT_TRUE(genresIdKeyRef1 == genresIdKeyRef2);
			}
		}
	}
}

TEST_F(JoinSelectsApi, JoinTestSorting) {
	Query booksQuery = Query(books_namespace, 11, 1111).Sort(price, true);
	Query joinQuery = Query(authors_namespace).LeftJoin(authorid, authorid_fk, CondEq, booksQuery);

	reindexer::QueryResults joinQueryRes;
	Error err = reindexer->Select(joinQuery, joinQueryRes);
	EXPECT_TRUE(err.ok()) << err.what();

	for (auto rowIt : joinQueryRes) {
		Item item(rowIt.GetItem());
		const reindexer::QRVector& joinQueryRes = rowIt.GetJoined();
		const QueryResults& joinResult(joinQueryRes[0]);

		Variant prevJoinedValue;
		for (auto itj : joinResult) {
			Item joinItem(itj.GetItem());
			Variant recentJoinedValue = joinItem[price];
			if (prevJoinedValue.Type() != KeyValueNull) {
				EXPECT_TRUE(prevJoinedValue.Compare(recentJoinedValue) >= 0);
			}
			prevJoinedValue = recentJoinedValue;
		}
	}
}

TEST_F(JoinSelectsApi, JoinTestSelectNonIndexedField) {
	reindexer::QueryResults qr;
	Query authorsQuery = Query(authors_namespace);
	Error err = reindexer->Select(Query(books_namespace)
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
	Error err = reindexer->OpenNamespace(default_namespace);
	ASSERT_TRUE(err.ok()) << err.what();
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{"id", "hash", "int", IndexOpts().PK()}});

	std::stringstream json;
	json << "{" << addQuotes(id) << ":" << 1 << "," << addQuotes(authorid_fk) << ":" << DostoevskyAuthorId << "}";
	Item lonelyItem = NewItem(default_namespace);
	ASSERT_TRUE(lonelyItem.Status().ok()) << lonelyItem.Status().what();

	err = lonelyItem.FromJSON(json.str());
	ASSERT_TRUE(err.ok()) << err.what();

	err = reindexer->Upsert(default_namespace, lonelyItem);
	ASSERT_TRUE(err.ok()) << err.what();

	err = reindexer->Commit(books_namespace);
	ASSERT_TRUE(err.ok()) << err.what();

	reindexer::QueryResults qr;
	Query authorsQuery = Query(authors_namespace);
	err = reindexer->Select(Query(default_namespace)
								.Where(authorid_fk, CondEq, Variant(DostoevskyAuthorId))
								.InnerJoin(authorid_fk, authorid, CondEq, authorsQuery),
							qr);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr.Count() == 1) << err.what();

	// And backwards even!
	reindexer::QueryResults qr2;
	Query testNsQuery = Query(default_namespace);
	err = reindexer->Select(
		Query(authors_namespace).Where(authorid, CondEq, Variant(DostoevskyAuthorId)).InnerJoin(authorid, authorid_fk, CondEq, testNsQuery),
		qr2);

	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(qr2.Count() == 1) << err.what();
}
