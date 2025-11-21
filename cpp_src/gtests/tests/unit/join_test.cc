#include "core/nsselecter/joinedselector.h"
#include "core/queryresults/joinresults.h"
#include "join_on_conditions_api.h"
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
	auto qr = rt.Select(queryBooks);
	EXPECT_LE(qr.Count(), 50);
	CheckJoinsInComplexWhereCondition(qr);
}

TEST_F(JoinSelectsApi, JoinsLockWithCache_364) {
	Query queryGenres{Query(genres_namespace).Where(genreid, CondEq, 1)};
	Query queryBooks{Query(books_namespace, 0, 50).InnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres))};
	QueryWatcher watcher{queryBooks};
	TurnOnJoinCache(genres_namespace);

	for (int i = 0; i < 10; ++i) {
		SCOPED_TRACE(std::to_string(i));
		std::ignore = rt.Select(queryBooks);
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
	auto q = Query(books_namespace).Not().InnerJoin("authorid_fk", "authorid", CondEq, Query(authors_namespace));
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

	Query srcQuery = Query::FromSQL(sql);
	QueryWatcher watcher{srcQuery};

	reindexer::WrSerializer wrser;
	srcQuery.GetSQL(wrser);

	Query dstQuery = Query::FromSQL(wrser.Slice());
	ASSERT_EQ(srcQuery, dstQuery);

	wrser.Reset();
	srcQuery.Serialize(wrser);
	reindexer::Serializer ser(wrser.Buf(), wrser.Len());
	Query deserializedQuery1 = Query::Deserialize(ser);
	ASSERT_EQ(srcQuery, deserializedQuery1) << "Original query:\n"
											<< srcQuery.GetSQL() << "\nDeserialized query:\n"
											<< deserializedQuery1.GetSQL();

	const auto json = srcQuery.GetJSON();
	Query deserializedQuery2 = Query::FromJSON(json);
	ASSERT_EQ(srcQuery, deserializedQuery2) << "Original query:\n"
											<< srcQuery.GetSQL() << "\nDeserialized query:\n"
											<< deserializedQuery2.GetSQL();
}

TEST_F(JoinSelectsApi, InnerJoinTest) {
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 600)};
	Query joinQuery = queryBooks.InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors));
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

	Query joinQuery{Query(authors_namespace).LeftJoin(authorid, authorid_fk, CondEq, std::move(booksQuery))};

	QueryWatcher watcher{joinQuery};
	auto joinQueryRes = rt.Select(joinQuery);
	auto err = VerifyResJSON(joinQueryRes);
	ASSERT_TRUE(err.ok()) << err.what();

	std::unordered_set<int> presentedAuthorIds;
	std::unordered_map<int, int> rowidsIndexes;
	int i = 0;
	for (auto rowIt : joinQueryRes.ToLocalQr()) {
		Item item(rowIt.GetItem(false));
		Variant authorIdKeyRef1 = item[authorid];
		const reindexer::ItemRef& rowid = rowIt.GetItemRef();

		auto itemIt = rowIt.GetJoined();
		if (itemIt.getJoinedItemsCount() == 0) {
			continue;
		}
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
		if (itemIt.getJoinedItemsCount() == 0) {
			continue;
		}
		auto joinedFieldIt = itemIt.begin();
		for (i = 0; i < joinedFieldIt.ItemsCount(); ++i) {
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

TEST_F(JoinSelectsApi, OrInnerJoinTest) {
	Query queryGenres(genres_namespace);
	Query queryAuthors(authors_namespace);
	Query queryBooks{Query(books_namespace, 0, 10).Where(price, CondGe, 500)};
	Query innerJoinQuery = std::move(queryBooks.InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors)));
	Query orInnerJoinQuery = std::move(innerJoinQuery.OrInnerJoin(genreId_fk, genreid, CondEq, std::move(queryGenres)));
	QueryWatcher watcher{orInnerJoinQuery};

	const int authorsNsJoinIndex = 0;
	const int genresNsJoinIndex = 1;

	auto queryRes = rt.Select(orInnerJoinQuery);
	auto err = VerifyResJSON(queryRes);
	ASSERT_TRUE(err.ok()) << err.what();

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
		auto joinQueryRes = rt.Select(joinQuery);
		Variant prevField;
		for (auto rowIt : joinQueryRes) {
			Item item = rowIt.GetItem(false);
			const auto cmpRes = prevField.Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(item[age]);
			ASSERT_NE(cmpRes & reindexer::ComparationResult::Le, 0);

			Variant key = item[authorid];
			auto itemIt = rowIt.GetJoined();
			if (itemIt.getJoinedItemsCount() == 0) {
				continue;
			}
			auto joinedFieldIt = itemIt.begin();

			std::optional<Variant> prevJoinedValue;
			for (int j = 0; j < joinedFieldIt.ItemsCount(); ++j) {
				reindexer::ItemImpl joinItem(joinedFieldIt.GetItem(j, joinQueryRes.GetPayloadType(1), joinQueryRes.GetTagsMatcher(1)));
				Variant fkey = joinItem.GetField(joinQueryRes.GetPayloadType(1).FieldByName(authorid_fk));
				auto cmpRes = key.Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(fkey);
				ASSERT_EQ(cmpRes, reindexer::ComparationResult::Eq) << key.As<std::string>() << " " << fkey.As<std::string>();
				Variant recentJoinedValue = joinItem.GetField(joinQueryRes.GetPayloadType(1).FieldByName(price));
				ASSERT_GE(recentJoinedValue.As<int>(), 200);

				if (prevJoinedValue.has_value()) {
					cmpRes =
						prevJoinedValue->Compare<reindexer::NotComparable::Return, reindexer::kDefaultNullsHandling>(recentJoinedValue);
					ASSERT_TRUE(cmpRes & reindexer::ComparationResult::Ge)
						<< prevJoinedValue->As<std::string>() << " " << recentJoinedValue.As<std::string>();
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
	EXPECT_STREQ(err.what(), "Not found value joined from ns books_namespace");

	Query joinedQuery2 = Query(authors_namespace);
	Query query2{Query(books_namespace)
					 .InnerJoin(authorid_fk, authorid, CondEq, std::move(joinedQuery2))
					 .Sort(authors_namespace + '.' + age, false)};

	QueryWatcher watcher{query2};
	auto joinQueryRes2 = rt.Select(query2);
	Variant prevValue;
	for (auto rowIt : joinQueryRes2) {
		const auto itemIt = rowIt.GetJoined();
		ASSERT_EQ(itemIt.getJoinedItemsCount(), 1);
		const auto joinedFieldIt = itemIt.begin();
		reindexer::ItemImpl joinItem(joinedFieldIt.GetItem(0, joinQueryRes2.GetPayloadType(1), joinQueryRes2.GetTagsMatcher(1)));
		const Variant recentValue = joinItem.GetField(joinQueryRes2.GetPayloadType(1).FieldByName(age));

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
							.InnerJoin(authorid_fk, authorid, CondEq, std::move(authorsQuery)));
	ASSERT_EQ(qr.Count(), 1);

	Item theOnlyItem = qr.begin().GetItem(false);
	VariantArray krefs = theOnlyItem[title];
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
							.InnerJoin(authorid_fk, authorid, CondEq, std::move(authorsQuery)));
	ASSERT_EQ(qr.Count(), 1);

	// And backwards even!
	Query testNsQuery = Query(default_namespace);
	auto qr2 = rt.Select(Query(authors_namespace)
							 .Where(authorid, CondEq, Variant(DostoevskyAuthorId))
							 .InnerJoin(authorid, authorid_fk, CondEq, std::move(testNsQuery)));
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
			auto queryRes = rt.Select(orInnerJoinQuery);
			EXPECT_GT(queryRes.Count(), 0);
		}
	};

	auto removeTh = [this]() { std::ignore = rt.Delete(Query(books_namespace, 0, 10).Where(price, CondGe, 5000)); };

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

TEST_F(JoinSelectsApi, JoinPreResultStoreValuesOptimizationStressTest) {
	using reindexer::JoinedSelector;
	static const std::string rightNs = "rightNs";
	static constexpr const char* data = "data";
	static constexpr int maxDataValue = 10;
	static constexpr int maxRightNsRowCount = maxDataValue * JoinedSelector::MaxIterationsForPreResultStoreValuesOptimization();
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
			Query q{Query(leftNs[i]).InnerJoin(data, data, CondEq, Query(rightNs).Where(data, CondEq, rand() % maxDataValue))};
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
	Query queryAuthors = Query(authors_namespace).Select({name, age});

	Query queryBooks{Query(books_namespace)
						 .Where(pages, CondGe, 100)
						 .InnerJoin(authorid_fk, authorid, CondEq, std::move(queryAuthors))
						 .Select({title, price})};

	auto qr = rt.Select(queryBooks);
	for (auto it : qr) {
		ASSERT_TRUE(it.Status().ok()) << it.Status().what();
		reindexer::WrSerializer wrser;
		auto err = it.GetJSON(wrser, false);
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
	auto qr = rt.Select(queryBooks);
	auto err = VerifyResJSON(qr);
	ASSERT_TRUE(err.ok()) << err.what();

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
	constexpr auto sqlPattern =
		R"(select * from books_namespace {} (select * from authors_namespace {} (select * from books_namespace) on authors_namespace.authorid = books_namespace.authorid_fk) on authors_namespace.authorid = books_namespace.authorid_fk)";
	auto joinTypes = {"inner join", "join", "left join"};
	for (auto& firstJoin : joinTypes) {
		for (auto& secondJoin : joinTypes) {
			auto sql = fmt::format(sqlPattern, firstJoin, secondJoin);
			ValidateQueryThrow(sql, errParseSQL, "Expected ')', but found .*, line: 1 column: .*");
		}
	}
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
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondLe, 100)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 200)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondLe, 400)).Limit(10),
		Query(books_namespace).InnerJoin(authorid_fk, authorid, CondEq, Query(authors_namespace).Where(authorid, CondGe, 400)).Limit(10)};

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
		{R"(select * from books_namespace inner join authors_namespace on (books_namespace.authorid_fk {} authors_namespace.authorid);)",
		 R"(select * from books_namespace inner join authors_namespace on (authors_namespace.authorid {} books_namespace.authorid_fk);)"}};
	const std::vector<std::pair<CondType, CondType>> conditions = {{CondLt, CondGt}, {CondLe, CondGe}, {CondGt, CondLt},
																   {CondGe, CondLe}, {CondEq, CondEq}, {CondSet, CondSet}};
	for (size_t i = 0; i < sqlTemplates.size(); ++i) {
		const auto& sqlTemplate = sqlTemplates[i];
		for (const auto& condition : conditions) {
			Query query1 = Query::FromSQL(GetSql(sqlTemplate.first, condition.first));
			auto qr1 = rt.Select(query1);
			Query query2 = Query::FromSQL(GetSql(sqlTemplate.second, condition.second));
			auto qr2 = rt.Select(query2);
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
		EXPECT_THROW((void)Query::FromSQL(sql), Error);
	}
	QueryResults qr;
	Error err = rt.reindexer->Select(Query(books_namespace).InnerJoin(authorid_fk, authorid, CondAllSet, Query(authors_namespace)), qr);
	EXPECT_FALSE(err.ok());
	qr.Clear();
	err = rt.reindexer->Select(Query(books_namespace).InnerJoin(authorid_fk, authorid, CondLike, Query(authors_namespace)), qr);
	EXPECT_FALSE(err.ok());
}

void CheckJoinIds(std::map<int, std::vector<std::set<int>>> ids, const reindexer::QueryResults& qr) {
	ASSERT_EQ(ids.size(), qr.Count());
	for (auto it : qr) {
		{
			const auto item = it.GetItem();
			const int id = item["id"].Get<int>();
			const auto idIt = ids.find(id);
			ASSERT_NE(idIt, ids.end()) << id;

			const auto joined = it.GetJoined();
			const auto& joinedIds = idIt->second;
			ASSERT_EQ(joinedIds.size(), joined.getJoinedFieldsCount());
			for (size_t i = 0; i < joinedIds.size(); ++i) {
				const auto& joinedItems = joined.at(i);
				const auto& joinedIdsSet = joinedIds[i];
				ASSERT_EQ(joinedIds[i].size(), joinedItems.ItemsCount());
				for (size_t j = 0; j < joinedIdsSet.size(); ++j) {
					const auto nsId = joinedItems[j].Nsid();
					auto itemImpl = joined.at(i).GetItem(j, qr.GetPayloadType(nsId), qr.GetTagsMatcher(nsId));
					const int joinedId = reindexer::Item::FieldRefByNameOrJsonPath("id", itemImpl).Get<int>();
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

	auto qr = rt.Select(Query(mainNs).InnerJoin("id", "id", CondEq, Query(joinNs)));
	CheckJoinIds({{0, {{0}}}, {1, {{1}}}}, qr);

	qr = rt.Select(Query(mainNs).InnerJoin("id", "id", CondEq, Query(joinNs)).LeftJoin("join_id", "id", CondEq, Query(joinNs)));
	CheckJoinIds({{0, {{0}, {2}}}, {1, {{1}, {3}}}}, qr);

	qr = rt.Select(Query(mainNs).InnerJoin("id", "id", CondEq, Query(joinNs)).LeftJoin("join_id", "id", CondGe, Query(joinNs)));
	CheckJoinIds({{0, {{0}, {0, 1, 2}}}, {1, {{1}, {0, 1, 2, 3}}}}, qr);

	qr = rt.Select(Query(mainNs)
					   .InnerJoin("id", "id", CondEq, Query(joinNs))
					   .LeftJoin("join_id", "id", CondGe, Query(joinNs))
					   .LeftJoin("join_id", "id", CondEq, Query(joinNs)));
	CheckJoinIds({{0, {{0}, {0, 1, 2}, {2}}}, {1, {{1}, {0, 1, 2, 3}, {3}}}}, qr);
}
