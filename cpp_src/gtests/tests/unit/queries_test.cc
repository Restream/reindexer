#include <thread>
#include "queries_api.h"

#if !defined(REINDEX_WITH_TSAN)
TEST_F(QueriesApi, QueriesStandardTestSet) {
	FillDefaultNamespace(0, 2500, 20);
	FillDefaultNamespace(2500, 2500, 0);
	FillCompositeIndexesNamespace(0, 1000);
	FillTestSimpleNamespace();
	FillComparatorsNamespace();
	FillTestJoinNamespace();
	FillGeomNamespace();

	CheckStandartQueries();
	CheckAggregationQueries();
	CheckSqlQueries();
	CheckDslQueries();
	CheckCompositeIndexesQueries();
	CheckComparatorsQueries();
	CheckDistinctQueries();
	CheckGeomQueries();

	int itemsCount = 0;
	InsertedItemsByPk& items = insertedItems[default_namespace];
	for (auto it = items.begin(); it != items.end();) {
		Error err = rt.reindexer->Delete(default_namespace, it->second);
		EXPECT_TRUE(err.ok()) << err.what();
		it = items.erase(it);
		if (++itemsCount == 4000) break;
	}

	FillDefaultNamespace(0, 500, 0);
	FillDefaultNamespace(0, 1000, 5);

	itemsCount = 0;
	// items = insertedItems[default_namespace];
	for (auto it = items.begin(); it != items.end();) {
		Error err = rt.reindexer->Delete(default_namespace, it->second);
		EXPECT_TRUE(err.ok()) << err.what();
		it = items.erase(it);
		if (++itemsCount == 5000) break;
	}

	for (size_t i = 0; i < 5000; ++i) {
		auto itToRemove = items.begin();
		if (itToRemove != items.end()) {
			Error err = rt.reindexer->Delete(default_namespace, itToRemove->second);
			EXPECT_TRUE(err.ok()) << err.what();
			items.erase(itToRemove);
		}
		FillDefaultNamespace(rand() % 100, 1, 0);

		if (!items.empty()) {
			itToRemove = items.begin();
			std::advance(itToRemove, rand() % std::min(100, int(items.size())));
			if (itToRemove != items.end()) {
				Error err = rt.reindexer->Delete(default_namespace, itToRemove->second);
				EXPECT_TRUE(err.ok()) << err.what();
				items.erase(itToRemove);
			}
		}
	}

	for (auto it = items.begin(); it != items.end();) {
		Error err = rt.reindexer->Delete(default_namespace, it->second);
		EXPECT_TRUE(err.ok()) << err.what();
		it = items.erase(it);
	}

	FillDefaultNamespace(3000, 1000, 20);
	FillDefaultNamespace(1000, 500, 00);
	FillCompositeIndexesNamespace(1000, 1000);
	FillComparatorsNamespace();
	FillGeomNamespace();

	CheckStandartQueries();
	CheckAggregationQueries();
	CheckSqlQueries();
	CheckDslQueries();
	CheckCompositeIndexesQueries();
	CheckComparatorsQueries();
	CheckDistinctQueries();
	CheckGeomQueries();
}
#endif

TEST_F(QueriesApi, IndexCacheInvalidationTest) {
	std::vector<std::pair<int, int>> data{{0, 10}, {1, 9}, {2, 8}, {3, 7}, {4, 6},	{5, 5},
										  {6, 4},  {7, 3}, {8, 2}, {9, 1}, {10, 0}, {11, -1}};
	for (auto values : data) {
		UpsertBtreeIdxOptNsItem(values);
	}
	Query q(btreeIdxOptNs);
	q.Where(kFieldNameId, CondSet, {3, 5, 7}).Where(kFieldNameStartTime, CondGt, 2).Debug(LogTrace);
	std::this_thread::sleep_for(std::chrono::seconds(1));
	for (size_t i = 0; i < 10; ++i) {
		ExecuteAndVerify(q);
	}

	UpsertBtreeIdxOptNsItem({5, 0});
	std::this_thread::sleep_for(std::chrono::seconds(5));
	for (size_t i = 0; i < 10; ++i) {
		ExecuteAndVerify(q);
	}
}

TEST_F(QueriesApi, SelectRaceWithIdxCommit) {
	FillDefaultNamespace(0, 1000, 1);
	Query q{Query(default_namespace)
				.Where(kFieldNameYear, CondGt, {2025})
				.Where(kFieldNameYear, CondLt, {2045})
				.Where(kFieldNameGenre, CondGt, {25})
				.Where(kFieldNameGenre, CondLt, {45})};
	for (unsigned i = 0; i < 50; ++i) {
		ExecuteAndVerify(q);
	}
}

TEST_F(QueriesApi, TransactionStress) {
	std::vector<std::thread> pool;
	FillDefaultNamespace(0, 350, 20);
	FillDefaultNamespace(3500, 350, 0);
	std::atomic_uint current_size;
	current_size = 350;
	uint32_t stepSize = 1000;

	for (size_t i = 0; i < 4; i++) {
		pool.push_back(std::thread([this, i, &current_size, stepSize]() {
			size_t start_pos = i * stepSize;
			if (i % 2 == 0) {
				uint32_t steps = 10;
				for (uint32_t j = 0; j < steps; ++j) {
					current_size += stepSize / steps;
					AddToDefaultNamespace(start_pos, start_pos + stepSize / steps, 20);
					start_pos = start_pos + stepSize / steps;
				}
			} else if (i % 2 == 1) {
				uint32_t oldsize = current_size.load();
				current_size += oldsize;
				FillDefaultNamespaceTransaction(current_size, start_pos + oldsize, 10);
			};
		}));
	}

	for (auto& tr : pool) {
		tr.join();
	}
}

TEST_F(QueriesApi, SqlParseGenerate) {
	using namespace std::string_literals;
	enum Direction { PARSE = 1, GEN = 2, BOTH = PARSE | GEN };
	struct {
		std::string sql;
		std::variant<Query, Error> expected;
		Direction direction = BOTH;
	} cases[]{
		{"SELECT * FROM test_namespace WHERE index = 5", Query{"test_namespace"}.Where("index", CondEq, 5)},
		{"SELECT * FROM test_namespace WHERE index LIKE 'str'", Query{"test_namespace"}.Where("index", CondLike, "str")},
		{"SELECT * FROM test_namespace WHERE index <= field", Query{"test_namespace"}.WhereBetweenFields("index", CondLe, "field")},
		{"SELECT * FROM test_namespace WHERE index+field = 5", Error{errParseSQL, "Expected condition operator, but found '+' in query"}},
		{"SELECT * FROM test_namespace WHERE \"index+field\" > 5", Query{"test_namespace"}.Where("index+field", CondGt, 5)},
		{"SELECT * FROM test_namespace WHERE \"index+field\" LIKE index2.field2",
		 Query{"test_namespace"}.WhereBetweenFields("index+field", CondLike, "index2.field2")},
		{"SELECT * FROM test_namespace WHERE index2.field2 <> \"index+field\"",
		 Query{"test_namespace"}.Not().WhereBetweenFields("index2.field2", CondEq, "index+field"), PARSE},
		{"SELECT * FROM test_namespace WHERE NOT index2.field2 = \"index+field\"",
		 Query{"test_namespace"}.Not().WhereBetweenFields("index2.field2", CondEq, "index+field")},
		{"SELECT * FROM test_namespace WHERE 'index+field' = 5",
		 Error{errParseSQL, "String is invalid at this location. (text = 'index+field'  location = line: 1 column: 49 52)"}},
		{"SELECT * FROM test_namespace WHERE \"index\" = 5", Query{"test_namespace"}.Where("index", CondEq, 5), PARSE},
		{"SELECT * FROM test_namespace WHERE 'index' = 5",
		 Error{errParseSQL, "String is invalid at this location. (text = 'index'  location = line: 1 column: 43 46)"}},
		{"SELECT * FROM test_namespace WHERE NOT index ALLSET 3489578", Query{"test_namespace"}.Not().Where("index", CondAllSet, 3489578)},
		{"SELECT * FROM test_namespace WHERE NOT index ALLSET (0,1)", Query{"test_namespace"}.Not().Where("index", CondAllSet, {0, 1})},
		{"SELECT ID, Year, Genre FROM test_namespace WHERE year > '2016' ORDER BY 'year' DESC LIMIT 10000000",
		 Query{"test_namespace"}.Select({"ID", "Year", "Genre"}).Where("year", CondGt, "2016").Sort("year", true).Limit(10000000)},
		{"SELECT ID FROM test_namespace WHERE name LIKE 'something' AND (genre IN ('1','2','3') AND year > '2016') OR age IN "
		 "('1','2','3','4') LIMIT 10000000",
		 Query{"test_namespace"}
			 .Select({"ID"})
			 .Where("name", CondLike, "something")
			 .OpenBracket()
			 .Where("genre", CondSet, {"1", "2", "3"})
			 .Where("year", CondGt, "2016")
			 .CloseBracket()
			 .Or()
			 .Where("age", CondSet, {"1", "2", "3", "4"})
			 .Limit(10000000)},
		{"SELECT * FROM test_namespace WHERE  INNER JOIN join_ns ON test_namespace.id = join_ns.id ORDER BY 'year + join_ns.year * (5 - "
		 "rand())'",
		 Query{"test_namespace"}.InnerJoin("id", "id", CondEq, Query{"join_ns"}).Sort("year + join_ns.year * (5 - rand())", false)},
		{"SELECT * FROM "s + geomNs + " WHERE ST_DWithin(" + kFieldNamePointNonIndex + ", ST_GeomFromText('POINT(1.25 -7.25)'), 0.5)",
		 Query{geomNs}.DWithin(kFieldNamePointNonIndex, {1.25, -7.25}, 0.5)},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, 10, 20, 30)", Query{"test_namespace"}.Sort("index", false, {10, 20, 30})},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, 'str1', 'str2', 'str3') DESC",
		 Query{"test_namespace"}.Sort("index", true, {"str1", "str2", "str3"})},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, {10, 'str1'}, {20, 'str2'}, {30, 'str3'})",
		 Query{"test_namespace"}.Sort("index", false, std::vector<std::tuple<int, std::string>>{{10, "str1"}, {20, "str2"}, {30, "str3"}})},
	};

	for (const auto& [sql, expected, direction] : cases) {
		if (std::holds_alternative<Query>(expected)) {
			const Query& q = std::get<Query>(expected);
			if (direction & GEN) {
				EXPECT_EQ(q.GetSQL(), sql);
			}
			if (direction & PARSE) {
				Query parsed;
				try {
					parsed.FromSQL(sql);
				} catch (const Error& err) {
					ADD_FAILURE() << "Unexpected error: " << err.what() << "\nSQL: " << sql;
					continue;
				}
				EXPECT_EQ(parsed, q) << sql;
			}
		} else {
			const Error& expectedErr = std::get<Error>(expected);
			Query parsed;
			try {
				parsed.FromSQL(sql);
				ADD_FAILURE() << "Expected error: " << expectedErr.what() << "\nSQL: " << sql;
			} catch (const Error& err) {
				EXPECT_EQ(err.what(), expectedErr.what()) << "\nSQL: " << sql;
			}
		}
	}
}

TEST_F(QueriesApi, DslGenerateParse) {
	using namespace std::string_literals;
	enum Direction { PARSE = 1, GEN = 2, BOTH = PARSE | GEN };
	struct {
		std::string dsl;
		std::variant<Query, Error> expected;
		Direction direction = BOTH;
	} cases[]{
		{R"({"namespace":")"s + geomNs +
			 R"(","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"dwithin","field":")" +
			 kFieldNamePointLinearRTree + R"(","value":[[-9.2,-0.145],0.581]}],"merge_queries":[],"aggregations":[]})",
		 Query{geomNs}.DWithin(kFieldNamePointLinearRTree, reindexer::Point{-9.2, -0.145}, 0.581)},
		{R"({"namespace":")"s + default_namespace +
			 R"(","limit":-1,"offset":0,"req_total":"disabled","explain":false,"type":"select","select_with_rank":false,"select_filter":[],"select_functions":[],"sort":[],"filters":[{"op":"and","cond":"gt","first_field":")" +
			 kFieldNameStartTime + R"(","second_field":")" + kFieldNamePackages + R"("}],"merge_queries":[],"aggregations":[]})",
		 Query{Query(default_namespace).WhereBetweenFields(kFieldNameStartTime, CondGt, kFieldNamePackages)}}};
	for (const auto& [dsl, expected, direction] : cases) {
		if (std::holds_alternative<Query>(expected)) {
			const Query& q = std::get<Query>(expected);
			if (direction & GEN) {
				EXPECT_EQ(q.GetJSON(), dsl);
			}
			if (direction & PARSE) {
				Query parsed;
				try {
					parsed.FromJSON(dsl);
				} catch (const Error& err) {
					ADD_FAILURE() << "Unexpected error: " << err.what() << "\nDSL: " << dsl;
					continue;
				}
				EXPECT_EQ(parsed, q) << dsl;
			}
		} else {
			const Error& expectedErr = std::get<Error>(expected);
			Query parsed;
			try {
				parsed.FromJSON(dsl);
				ADD_FAILURE() << "Expected error: " << expectedErr.what() << "\nDSL: " << dsl;
			} catch (const Error& err) {
				EXPECT_EQ(err.what(), expectedErr.what()) << "\nDSL: " << dsl;
			}
		}
	}
}

std::vector<int> generateForcedSortOrder(int maxValue, size_t size) {
	std::set<int> res;
	while (res.size() < size) res.insert(rand() % maxValue);
	return {res.cbegin(), res.cend()};
}

TEST_F(QueriesApi, ForcedSortOffsetTest) {
	FillForcedSortNamespace();
	for (size_t i = 0; i < 100; ++i) {
		const auto forcedSortOrder =
			generateForcedSortOrder(forcedSortOffsetMaxValue * 1.1, rand() % static_cast<int>(forcedSortOffsetNsSize * 1.1));
		const size_t offset = rand() % static_cast<size_t>(forcedSortOffsetNsSize * 1.1);
		const size_t limit = rand() % static_cast<size_t>(forcedSortOffsetNsSize * 1.1);
		const bool desc = rand() % 2;
		// Single column sort
		auto expectedResults = ForcedSortOffsetTestExpectedResults(offset, limit, desc, forcedSortOrder, First);
		ExecuteAndVerify(Query(forcedSortOffsetNs).Sort(kFieldNameColumnHash, desc, forcedSortOrder).Offset(offset).Limit(limit),
						 kFieldNameColumnHash, expectedResults);
		expectedResults = ForcedSortOffsetTestExpectedResults(offset, limit, desc, forcedSortOrder, Second);
		ExecuteAndVerify(Query(forcedSortOffsetNs).Sort(kFieldNameColumnTree, desc, forcedSortOrder).Offset(offset).Limit(limit),
						 kFieldNameColumnTree, expectedResults);
		// Multicolumn sort
		const bool desc2 = rand() % 2;
		auto expectedResultsMult = ForcedSortOffsetTestExpectedResults(offset, limit, desc, desc2, forcedSortOrder, First);
		ExecuteAndVerify(Query(forcedSortOffsetNs)
							 .Sort(kFieldNameColumnHash, desc, forcedSortOrder)
							 .Sort(kFieldNameColumnTree, desc2)
							 .Offset(offset)
							 .Limit(limit),
						 kFieldNameColumnHash, expectedResultsMult.first, kFieldNameColumnTree, expectedResultsMult.second);
		expectedResultsMult = ForcedSortOffsetTestExpectedResults(offset, limit, desc, desc2, forcedSortOrder, Second);
		ExecuteAndVerify(Query(forcedSortOffsetNs)
							 .Sort(kFieldNameColumnTree, desc2, forcedSortOrder)
							 .Sort(kFieldNameColumnHash, desc)
							 .Offset(offset)
							 .Limit(limit),
						 kFieldNameColumnHash, expectedResultsMult.first, kFieldNameColumnTree, expectedResultsMult.second);
	}
}

TEST_F(QueriesApi, StrictModeTest) {
	FillTestSimpleNamespace();

	const std::string kNotExistingField = "some_random_name123";
	QueryResults qr;
	{
		Query query = Query(testSimpleNs).Where(kNotExistingField, CondEmpty, 0);
		Error err = rt.reindexer->Select(query.Strict(StrictModeNames), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr.Clear();
		err = rt.reindexer->Select(query.Strict(StrictModeIndexes), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr.Clear();
		err = rt.reindexer->Select(query.Strict(StrictModeNone), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		Verify(qr, Query(testSimpleNs));
		qr.Clear();
	}

	{
		Query query = Query(testSimpleNs).Where(kNotExistingField, CondEq, 0);
		Error err = rt.reindexer->Select(query.Strict(StrictModeNames), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr.Clear();
		err = rt.reindexer->Select(query.Strict(StrictModeIndexes), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr.Clear();
		err = rt.reindexer->Select(query.Strict(StrictModeNone), qr);
		EXPECT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.Count(), 0);
	}
}

TEST_F(QueriesApi, SQLLeftJoinSerialize) {
	const char* condNames[] = {"IS NOT NULL", "=", "<", "<=", ">", ">=", "RANGE", "IN", "ALLSET", "IS NULL", "LIKE"};
	const std::string sqlTemplate = "SELECT * FROM tleft LEFT JOIN tright ON %s.%s %s %s.%s";

	const std::string tLeft = "tleft";
	const std::string tRight = "tright";
	const std::string iLeft = "ileft";
	const std::string iRight = "iright";

	auto createQuery = [&sqlTemplate, &condNames](const std::string& leftTable, const std::string& rightTable, const std::string& leftIndex,
												  const std::string& rightIndex, CondType t) -> std::string {
		return fmt::sprintf(sqlTemplate, leftTable, leftIndex, condNames[t], rightTable, rightIndex);
	};

	std::vector<std::pair<CondType, CondType>> conditions = {{CondLe, CondGe}, {CondGe, CondLe}, {CondLt, CondGt}, {CondGt, CondLt}};

	for (auto& c : conditions) {
		try {
			reindexer::Query q(tLeft);
			reindexer::Query qr(tRight);
			q.LeftJoin(iLeft, iRight, c.first, qr);

			{
				std::string sqlQCmp = createQuery(tLeft, tRight, iLeft, iRight, c.first);
				reindexer::WrSerializer wrSer;
				q.GetSQL(wrSer);
				ASSERT_EQ(sqlQCmp, std::string(wrSer.c_str()));
			}

			{
				Query qSql;
				std::string sqlQ = createQuery(tLeft, tRight, iLeft, iRight, c.first);
				qSql.FromSQL(sqlQ);

				reindexer::WrSerializer wrSer;
				qSql.GetSQL(wrSer);
				ASSERT_EQ(sqlQ, std::string(wrSer.c_str()));
			}
			{
				Query qSql;
				std::string sqlQ = createQuery(tRight, tLeft, iRight, iLeft, c.second);
				qSql.FromSQL(sqlQ);
				ASSERT_EQ(q.GetJSON(), qSql.GetJSON());
				reindexer::WrSerializer wrSer;
				qSql.GetSQL(wrSer);
				ASSERT_EQ(sqlQ, std::string(wrSer.c_str()));
			}
		} catch (const Error& e) {
			ASSERT_TRUE(e.ok()) << e.what();
		}
	}
}

TEST_F(QueriesApi, JoinByNotIndexField) {
	static constexpr int kItemsCount = 10;
	const std::string leftNs = "join_by_not_index_field_left_ns";
	const std::string rightNs = "join_by_not_index_field_right_ns";

	reindexer::WrSerializer ser;
	for (const auto& nsName : {leftNs, rightNs}) {
		Error err = rt.reindexer->OpenNamespace(nsName);
		ASSERT_TRUE(err.ok()) << err.what();
		err = rt.reindexer->AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "tree", "int", IndexOpts{}.PK()});
		ASSERT_TRUE(err.ok()) << err.what();
		for (int i = 0; i < kItemsCount; ++i) {
			ser.Reset();
			reindexer::JsonBuilder json{ser};
			json.Put("id", i);
			if (i % 2 == 1) json.Put("f", i);
			json.End();
			Item item = rt.reindexer->NewItem(nsName);
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			err = item.FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			Upsert(nsName, item);
		}
	}
	reindexer::QueryResults qr;
	Error err = rt.reindexer->Select(
		Query(leftNs).Strict(StrictModeNames).Join(InnerJoin, Query(rightNs).Where("id", CondGe, 5)).On("f", CondEq, "f"), qr);
	ASSERT_TRUE(err.ok()) << err.what();
	const int expectedIds[] = {5, 7, 9};
	ASSERT_EQ(qr.Count(), sizeof(expectedIds) / sizeof(int));
	unsigned i = 0;
	for (auto& it : qr) {
		Item item = it.GetItem(false);
		VariantArray values = item["id"];
		ASSERT_EQ(values.size(), 1);
		EXPECT_EQ(values[0].As<int>(), expectedIds[i++]);
	}
}

TEST_F(QueriesApi, AllSet) {
	const std::string nsName = "allset_ns";
	Error err = rt.reindexer->OpenNamespace(nsName);
	ASSERT_TRUE(err.ok()) << err.what();
	err = rt.reindexer->AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	ASSERT_TRUE(err.ok()) << err.what();
	reindexer::WrSerializer ser;
	reindexer::JsonBuilder json{ser};
	json.Put("id", 0);
	json.Array("array", {0, 1, 2});
	json.End();
	Item item = rt.reindexer->NewItem(nsName);
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	Upsert(nsName, item);
	Query q{nsName};
	q.Where("array", CondAllSet, {0, 1, 2});
	reindexer::QueryResults qr;
	err = rt.reindexer->Select(q, qr);
	ASSERT_TRUE(err.ok()) << err.what();
	EXPECT_EQ(qr.Count(), 1);
}
