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
		for (size_t i = 0; i < pureSelectRes.size(); ++i) {
			unique_ptr<reindexer::Item> booksItem(pureSelectRes.GetItem(i));
			auto* booksItemr = reinterpret_cast<reindexer::ItemImpl*>(booksItem.get());
			KeyRef authorIdKeyRef = booksItemr->GetField(authorid_fk);

			reindexer::QueryResults authorsSelectRes;
			Query authorsQuery = Query(authors_namespace).Where(authorid, CondEq, authorIdKeyRef);
			err = reindexer->Select(authorsQuery, authorsSelectRes);
			EXPECT_TRUE(err.ok()) << err.what();

			if (err.ok()) {
				KeyRef bookIdKeyRef = booksItemr->GetField(bookid);
				QueryResultRow& pureSelectRow = pureSelectRows[static_cast<int>(bookIdKeyRef)];

				FillQueryResultFromItem(booksItem.get(), pureSelectRow);
				for (size_t i = 0; i < authorsSelectRes.size(); ++i) {
					unique_ptr<reindexer::Item> authorsItem(authorsSelectRes.GetItem(i));
					FillQueryResultFromItem(authorsItem.get(), pureSelectRow);
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
		for (size_t i = 0; i < booksQueryRes.size(); ++i) {
			unique_ptr<reindexer::Item> item(booksQueryRes.GetItem(i));
			auto* ritem = reinterpret_cast<reindexer::ItemImpl*>(item.get());
			KeyRef bookIdKeyRef = ritem->GetField(bookid);
			BookId bookId = static_cast<int>(bookIdKeyRef);
			QueryResultRow& resultRow = pureSelectRows[bookId];
			FillQueryResultFromItem(item.get(), resultRow);
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
		for (size_t i = 0; i < joinQueryRes.size(); ++i) {
			unique_ptr<reindexer::Item> item(joinQueryRes.GetItem(i));
			auto* ritem = reinterpret_cast<reindexer::ItemImpl*>(item.get());
			KeyRef authorIdKeyRef1 = ritem->GetField(authorid);
			const reindexer::ItemRef& rowid = joinQueryRes[i];
			vector<QueryResults>& queryResults = joinQueryRes.joined_[rowid.id];
			for (const QueryResults& queryRes : queryResults) {
				unique_ptr<reindexer::Item> item2(queryRes.GetItem(0));
				auto* ritem2 = reinterpret_cast<reindexer::ItemImpl*>(item2.get());
				KeyRef authorIdKeyRef2 = ritem2->GetField(authorid_fk);
				EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
			}
			presentedAuthorIds.insert(static_cast<int>(authorIdKeyRef1));
			rowidsIndexes.insert({rowid.id, i});
		}

		for (const std::pair<const IdType, vector<QueryResults>>& itempair : joinQueryRes.joined_) {
			if (itempair.second.empty()) continue;
			const QueryResults& joinedQueryRes(itempair.second[0]);
			for (int i = 0; i < joinedQueryRes.size(); ++i) {
				unique_ptr<reindexer::Item> item(joinedQueryRes.GetItem(i));
				auto* ritem = reinterpret_cast<reindexer::ItemImpl*>(item.get());

				KeyRef authorIdKeyRef1 = ritem->GetField(authorid_fk);
				int authorId = static_cast<int>(authorIdKeyRef1);

				auto itAutorid(presentedAuthorIds.find(authorId));
				EXPECT_TRUE(itAutorid != presentedAuthorIds.end());

				int rowid(itempair.first);
				auto itRowidIndex(rowidsIndexes.find(rowid));
				EXPECT_TRUE(itRowidIndex != rowidsIndexes.end());

				if (itRowidIndex != rowidsIndexes.end()) {
					unique_ptr<reindexer::Item> item2(joinQueryRes.GetItem(rowid));
					auto* ritem2 = reinterpret_cast<reindexer::ItemImpl*>(item2.get());
					KeyRef authorIdKeyRef2 = ritem2->GetField(authorid);
					EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
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
		for (size_t i = 0; i < queryRes.size(); ++i) {
			unique_ptr<reindexer::Item> item(queryRes.GetItem(i));
			auto* ritem = reinterpret_cast<reindexer::ItemImpl*>(item.get());
			reindexer::ItemRef& itemRef(queryRes[i]);
			vector<QueryResults>& joinedResult = queryRes.joined_[itemRef.id];
			QueryResults& authorNsJoinResults = joinedResult[authorsNsJoinIndex];
			QueryResults& genresNsJoinResults = joinedResult[genresNsJoinIndex];

			KeyRef authorIdKeyRef1 = ritem->GetField(authorid_fk);
			for (size_t j = 0; j < authorNsJoinResults.size(); ++j) {
				KeyRefs authorIdKeyRefs;
				unique_ptr<reindexer::Item> authorsItem(authorNsJoinResults.GetItem(j));
				auto* authorsRItem = reinterpret_cast<reindexer::ItemImpl*>(authorsItem.get());
				authorsRItem->Get(authorid, authorIdKeyRefs);
				KeyRef authorIdKeyRef2 = authorIdKeyRefs[0];
				EXPECT_EQ(authorIdKeyRef1, authorIdKeyRef2);
			}

			KeyRef genresIdKeyRef1 = ritem->GetField(genreId_fk);
			for (size_t k = 0; k < genresNsJoinResults.size(); ++k) {
				KeyRefs genreIdKeyRef;
				unique_ptr<reindexer::Item> genresItem(genresNsJoinResults.GetItem(k));
				auto* genresRItem = reinterpret_cast<reindexer::ItemImpl*>(genresItem.get());
				genresRItem->Get(genreid, genreIdKeyRef);
				KeyRef genresIdKeyRef2 = genreIdKeyRef[0];
				EXPECT_EQ(genresIdKeyRef1, genresIdKeyRef2);
			}
		}
	}
}
