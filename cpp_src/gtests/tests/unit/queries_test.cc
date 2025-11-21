#include <gmock/gmock.h>

#include <thread>

#include "core/cjson/csvbuilder.h"
#include "core/schema.h"
#include "csv2jsonconverter.h"
#include "queries_api.h"
#include "tools/jsontools.h"

TEST_F(QueriesApi, QueriesStandardTestSet) {
	try {
		FillDefaultNamespace(0, 2500, 20);
		FillDefaultNamespace(2500, 2500, 0);
		FillCompositeIndexesNamespace(0, 1000);
		FillTestSimpleNamespace();
		FillComparatorsNamespace();
		FillTestJoinNamespace(0, 300);
		FillGeomNamespace();

		CheckStandardQueries();
		CheckAggregationQueries();
		CheckSqlQueries();
		CheckDslQueries();
		CheckCompositeIndexesQueries();
		CheckComparatorsQueries();
		CheckDistinctQueries();
		CheckGeomQueries();
		CheckMergeQueriesWithLimit();
		CheckMergeQueriesWithAggregation();

		int itemsCount = 0;
		auto& items = insertedItems_[default_namespace];
		for (auto it = items.begin(); it != items.end();) {
			rt.Delete(default_namespace, it->second);
			it = items.erase(it);
			if (++itemsCount == 4000) {
				break;
			}
		}

		FillDefaultNamespace(0, 500, 0);
		FillDefaultNamespace(0, 1000, 5);

		itemsCount = 0;
		for (auto it = items.begin(); it != items.end();) {
			rt.Delete(default_namespace, it->second);
			it = items.erase(it);
			if (++itemsCount == 5000) {
				break;
			}
		}

		for (size_t i = 0; i < 5000; ++i) {
			auto itToRemove = items.begin();
			if (itToRemove != items.end()) {
				rt.Delete(default_namespace, itToRemove->second);
				items.erase(itToRemove);
			}
			FillDefaultNamespace(rand() % 100, 1, 0);

			if (!items.empty()) {
				itToRemove = items.begin();
				std::advance(itToRemove, rand() % std::min(100, int(items.size())));
				if (itToRemove != items.end()) {
					rt.Delete(default_namespace, itToRemove->second);
					items.erase(itToRemove);
				}
			}
		}

		for (auto it = items.begin(); it != items.end();) {
			rt.Delete(default_namespace, it->second);
			it = items.erase(it);
		}

		FillDefaultNamespace(3000, 1000, 20);
		FillDefaultNamespace(1000, 500, 00);
		FillCompositeIndexesNamespace(1000, 1000);
		FillComparatorsNamespace();
		FillGeomNamespace();

		CheckStandardQueries();
		CheckAggregationQueries();
		CheckSqlQueries();
		CheckDslQueries();
		CheckCompositeIndexesQueries();
		CheckComparatorsQueries();
		CheckDistinctQueries();
		CheckGeomQueries();
		CheckMergeQueriesWithLimit();
		CheckConditionsMergingQueries();
	} catch (const reindexer::Error& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (const std::exception& e) {
		ASSERT_TRUE(false) << e.what() << std::endl;
	} catch (...) {
		ASSERT_TRUE(false);
	}
}

TEST_F(QueriesApi, QueriesConditions) {
	FillConditionsNs();
	CheckConditions();
}

TEST_F(QueriesApi, UuidQueries) {
	FillUUIDNs();
	// hack to obtain not index not string uuid fields
	/*rt.DropIndex(uuidNs, {kFieldNameUuidNotIndex2});  // TODO uncomment this #1470
	rt.DropIndex(uuidNs, {kFieldNameUuidNotIndex3});*/
	CheckUUIDQueries();
}

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

	constexpr size_t kThreads = 4;
	pool.reserve(kThreads);
	for (size_t i = 0; i < kThreads; i++) {
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
			}
		}));
	}

	for (auto& tr : pool) {
		tr.join();
	}
}

TEST_F(QueriesApi, SqlParseGenerate) {
	using namespace std::string_literals;
	enum [[nodiscard]] Direction { PARSE = 1, GEN = 2, BOTH = PARSE | GEN };
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
		 Error{errParseSQL, "String is invalid at this location. (text = 'index+field'  location = line: 1 column: 36 47)"}},
		{"SELECT * FROM test_namespace WHERE \"index\" = 5", Query{"test_namespace"}.Where("index", CondEq, 5), PARSE},
		{"SELECT * FROM test_namespace WHERE 'index' = 5",
		 Error{errParseSQL, "String is invalid at this location. (text = 'index'  location = line: 1 column: 36 41)"}},
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
		{"SELECT * FROM test_namespace WHERE INNER JOIN join_ns ON test_namespace.id = join_ns.id "
		 "ORDER BY 'year + join_ns.year * (5 - rand())'",
		 Query{"test_namespace"}.InnerJoin("id", "id", CondEq, Query{"join_ns"}).Sort("year + join_ns.year * (5 - rand())", false)},
		{"SELECT * FROM "s + geomNs + " WHERE ST_DWithin(" + kFieldNamePointNonIndex + ", ST_GeomFromText('POINT(1.25 -7.25)'), 0.5)",
		 Query{geomNs}.DWithin(kFieldNamePointNonIndex, reindexer::Point{1.25, -7.25}, 0.5)},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, 10, 20, 30)", Query{"test_namespace"}.Sort("index", false, {10, 20, 30})},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, 'str1', 'str2', 'str3') DESC",
		 Query{"test_namespace"}.Sort("index", true, {"str1", "str2", "str3"})},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, {10, 'str1'}, {20, 'str2'}, {30, 'str3'})",
		 Query{"test_namespace"}.Sort("index", false, std::vector<std::tuple<int, std::string>>{{10, "str1"}, {20, "str2"}, {30, "str3"}})},
		{"SELECT * FROM main_ns WHERE (SELECT * FROM second_ns WHERE id < 10 LIMIT 0) IS NOT NULL",
		 Query{"main_ns"}.Where(Query{"second_ns"}.Where("id", CondLt, 10), CondAny, VariantArray{})},
		{"SELECT * FROM main_ns WHERE id = (SELECT id FROM second_ns WHERE id < 10)",
		 Query{"main_ns"}.Where("id", CondEq, Query{"second_ns"}.Select({"id"}).Where("id", CondLt, 10))},
		{"SELECT * FROM main_ns WHERE (SELECT max(id) FROM second_ns WHERE id < 10) > 18",
		 Query{"main_ns"}.Where(Query{"second_ns"}.Aggregate(AggMax, {"id"}).Where("id", CondLt, 10), CondGt, {18})},
		{"SELECT * FROM main_ns WHERE id > (SELECT avg(id) FROM second_ns WHERE id < 10)",
		 Query{"main_ns"}.Where("id", CondGt, Query{"second_ns"}.Aggregate(AggAvg, {"id"}).Where("id", CondLt, 10))},
		{"SELECT * FROM main_ns WHERE id > (SELECT COUNT(*) FROM second_ns WHERE id < 10 LIMIT 0)",
		 Query{"main_ns"}.Where("id", CondGt, Query{"second_ns"}.Where("id", CondLt, 10).ReqTotal())},
		{"SELECT * FROM main_ns WHERE (SELECT * FROM second_ns WHERE id < 10 LIMIT 0) IS NOT NULL AND value IN (5,4,1)",
		 Query{"main_ns"}
			 .Where(Query{"second_ns"}.Where("id", CondLt, 10), CondAny, VariantArray{})
			 .Where("value", CondSet, {Variant{5}, Variant{4}, Variant{1}})},
		{"SELECT * FROM main_ns WHERE ((SELECT * FROM second_ns WHERE id < 10 LIMIT 0) IS NOT NULL) AND value IN (5,4,1)",
		 Query{"main_ns"}
			 .OpenBracket()
			 .Where(Query{"second_ns"}.Where("id", CondLt, 10), CondAny, VariantArray{})
			 .CloseBracket()
			 .Where("value", CondSet, {Variant{5}, Variant{4}, Variant{1}})},
		{"SELECT * FROM main_ns WHERE id IN (SELECT id FROM second_ns WHERE id < 999) AND value >= 1000",
		 Query{"main_ns"}.Where("id", CondSet, Query{"second_ns"}.Select({"id"}).Where("id", CondLt, 999)).Where("value", CondGe, 1000)},
		{"SELECT * FROM main_ns WHERE (id IN (SELECT id FROM second_ns WHERE id < 999)) AND value >= 1000",
		 Query{"main_ns"}
			 .OpenBracket()
			 .Where("id", CondSet, Query{"second_ns"}.Select({"id"}).Where("id", CondLt, 999))
			 .CloseBracket()
			 .Where("value", CondGe, 1000)},
		{"SELECT * FROM main_ns "
		 "WHERE (SELECT id FROM second_ns WHERE id < 999 AND xxx IS NULL ORDER BY 'value' DESC LIMIT 10) = 0 "
		 "ORDER BY 'tree'",
		 Query{"main_ns"}
			 .Where(Query{"second_ns"}
						.Select({"id"})
						.Where("id", CondLt, 999)
						.Where("xxx", CondEmpty, VariantArray{})
						.Limit(10)
						.Sort("value", true),
					CondEq, 0)
			 .Sort("tree", false)},
		{"SELECT * FROM main_ns "
		 "WHERE ((SELECT id FROM second_ns WHERE id < 999 AND xxx IS NULL ORDER BY 'value' DESC LIMIT 10) = 0) "
		 "ORDER BY 'tree'",
		 Query{"main_ns"}
			 .OpenBracket()
			 .Where(Query{"second_ns"}
						.Select({"id"})
						.Where("id", CondLt, 999)
						.Where("xxx", CondEmpty, VariantArray{})
						.Limit(10)
						.Sort("value", true),
					CondEq, 0)
			 .CloseBracket()
			 .Sort("tree", false)},
		{"SELECT * FROM main_ns "
		 "WHERE INNER JOIN (SELECT * FROM second_ns WHERE NOT val = 10) ON main_ns.id = second_ns.uid "
		 "AND id IN (SELECT id FROM third_ns WHERE id < 999) "
		 "AND INNER JOIN (SELECT * FROM fourth_ns WHERE val IS NOT NULL OFFSET 2 LIMIT 1) ON main_ns.uid = fourth_ns.id",
		 Query{"main_ns"}
			 .InnerJoin("id", "uid", CondEq, Query("second_ns").Not().Where("val", CondEq, 10))
			 .Where("id", CondSet, Query{"third_ns"}.Select({"id"}).Where("id", CondLt, 999))
			 .InnerJoin("uid", "id", CondEq, Query("fourth_ns").Where("val", CondAny, VariantArray{}).Limit(1).Offset(2))},
		{"SELECT * FROM main_ns "
		 "WHERE INNER JOIN (SELECT * FROM second_ns WHERE NOT val = 10 OFFSET 2 LIMIT 1) ON main_ns.id = second_ns.uid "
		 "AND id IN (SELECT id FROM third_ns WHERE id < 999) "
		 "LEFT JOIN (SELECT * FROM fourth_ns WHERE val IS NOT NULL) ON main_ns.uid = fourth_ns.id",
		 Query{"main_ns"}
			 .InnerJoin("id", "uid", CondEq, Query("second_ns").Not().Where("val", CondEq, 10).Limit(1).Offset(2))
			 .Where("id", CondSet, Query{"third_ns"}.Select({"id"}).Where("id", CondLt, 999))
			 .LeftJoin("uid", "id", CondEq, Query("fourth_ns").Where("val", CondAny, VariantArray{}))},
		{"SELECT * FROM main_ns "
		 "WHERE id IN (SELECT id FROM third_ns WHERE id < 999 OFFSET 7 LIMIT 5) "
		 "LEFT JOIN (SELECT * FROM second_ns WHERE NOT val = 10 OFFSET 2 LIMIT 1) ON main_ns.id = second_ns.uid "
		 "LEFT JOIN (SELECT * FROM fourth_ns WHERE val IS NOT NULL) ON main_ns.uid = fourth_ns.id",
		 Query{"main_ns"}
			 .LeftJoin("id", "uid", CondEq, Query("second_ns").Not().Where("val", CondEq, 10).Limit(1).Offset(2))
			 .Where("id", CondSet, Query{"third_ns"}.Select({"id"}).Where("id", CondLt, 999).Limit(5).Offset(7))
			 .LeftJoin("uid", "id", CondEq, Query("fourth_ns").Where("val", CondAny, VariantArray{}))},
		{"SELECT * FROM ns WHERE ft = 'text' ORDER BY 'rank(ft, 10.0)'",
		 Query{"ns"}.Where("ft", CondEq, "text").Sort("rank(ft, 10.0)", false)}};

	for (const auto& [sql, expected, direction] : cases) {
		if (std::holds_alternative<Query>(expected)) {
			const Query& q = std::get<Query>(expected);
			if (direction & GEN) {
				EXPECT_EQ(q.GetSQL(), sql);
			}
			if (direction & PARSE) {
				try {
					Query parsed = Query::FromSQL(sql);
					EXPECT_EQ(parsed, q) << sql;
				} catch (const Error& err) {
					ADD_FAILURE() << "Unexpected error: " << err.what() << "\nSQL: " << sql;
					continue;
				}
			}
		} else {
			const Error& expectedErr = std::get<Error>(expected);
			try {
				Query parsed = Query::FromSQL(sql);
				ADD_FAILURE() << "Expected error: " << expectedErr.what() << "\nSQL: " << sql;
			} catch (const Error& err) {
				EXPECT_STREQ(err.what(), expectedErr.what()) << "\nSQL: " << sql;
			}
		}
	}
}

TEST_F(QueriesApi, DslGenerateParse) {
	using namespace std::string_literals;
	enum [[nodiscard]] Direction { PARSE = 1, GEN = 2, BOTH = PARSE | GEN };
	struct {
		std::string dsl;
		std::variant<Query, Error> expected;
		Direction direction = BOTH;
	} cases[]{{fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "always": true
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   geomNs),
			   Query{geomNs}.AppendQueryEntry<reindexer::AlwaysTrue>(OpAnd)},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [
      {{
         "field": "rank(ft, 2.0) + 5",
         "desc": false
      }}
   ],
   "filters": [
      {{
         "op": "and",
         "cond": "eq",
         "field": "ft",
         "value": "text"
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   geomNs),
			   Query{geomNs}.Where("ft", CondEq, "text").Sort("rank(ft, 2.0) + 5", false)},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "cond": "dwithin",
         "field": "{}",
         "value": [
            [
               -9.2,
               -0.145
            ],
            0.581
         ]
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   geomNs, kFieldNamePointLinearRTree),
			   Query{geomNs}.DWithin(kFieldNamePointLinearRTree, reindexer::Point{-9.2, -0.145}, 0.581)},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "cond": "gt",
         "first_field": "{}",
         "second_field": "{}"
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   default_namespace, kFieldNameStartTime, kFieldNamePackages),
			   Query(default_namespace).WhereBetweenFields(kFieldNameStartTime, CondGt, kFieldNamePackages)},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "cond": "gt",
         "subquery": {{
            "namespace": "{}",
            "limit": 10,
            "offset": 10,
            "req_total": "disabled",
            "select_filter": [],
            "sort": [],
            "filters": [],
            "aggregations": [
               {{
                  "type": "max",
                  "fields": [
                     "{}"
                  ]
               }}
            ]
         }},
         "value": 18
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   default_namespace, joinNs, kFieldNameAge),
			   Query(default_namespace).Where(Query(joinNs).Aggregate(AggMax, {kFieldNameAge}).Limit(10).Offset(10), CondGt, {18})},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "cond": "any",
         "subquery": {{
            "namespace": "{}",
            "limit": 0,
            "offset": 0,
            "req_total": "disabled",
            "select_filter": [],
            "sort": [],
            "filters": [
               {{
                  "op": "and",
                  "cond": "eq",
                  "field": "{}",
                  "value": 1
               }}
            ],
            "aggregations": []
         }}
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   default_namespace, joinNs, kFieldNameId),
			   Query(default_namespace).Where(Query(joinNs).Where(kFieldNameId, CondEq, 1), CondAny, VariantArray{})},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "cond": "eq",
         "field": "{}",
         "subquery": {{
            "namespace": "{}",
            "limit": -1,
            "offset": 0,
            "req_total": "disabled",
            "select_filter": [
               "{}"
            ],
            "sort": [],
            "filters": [
               {{
                  "op": "and",
                  "cond": "set",
                  "field": "{}",
                  "value": [
                     1,
                     10,
                     100
                  ]
               }}
            ],
            "aggregations": []
         }}
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   default_namespace, kFieldNameName, joinNs, kFieldNameName, kFieldNameId),
			   Query(default_namespace)
				   .Where(kFieldNameName, CondEq, Query(joinNs).Select({kFieldNameName}).Where(kFieldNameId, CondSet, {1, 10, 100}))},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "cond": "gt",
         "field": "{}",
         "subquery": {{
            "namespace": "{}",
            "limit": -1,
            "offset": 0,
            "req_total": "disabled",
            "select_filter": [],
            "sort": [],
            "filters": [],
            "aggregations": [
               {{
                  "type": "avg",
                  "fields": [
                     "{}"
                  ]
               }}
            ]
         }}
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   default_namespace, kFieldNameId, joinNs, kFieldNameId),
			   Query(default_namespace).Where(kFieldNameId, CondGt, Query(joinNs).Aggregate(AggAvg, {kFieldNameId}))},
			  {fmt::format(
				   R"({{
   "namespace": "{}",
   "limit": -1,
   "offset": 0,
   "req_total": "disabled",
   "explain": false,
   "type": "select",
   "select_with_rank": false,
   "select_filter": [],
   "select_functions": [],
   "sort": [],
   "filters": [
      {{
         "op": "and",
         "cond": "gt",
         "field": "{}",
         "subquery": {{
            "namespace": "{}",
            "limit": 0,
            "offset": 0,
            "req_total": "enabled",
            "select_filter": [],
            "sort": [],
            "filters": [],
            "aggregations": []
         }}
      }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   default_namespace, kFieldNameId, joinNs, kFieldNameId),
			   Query(default_namespace).Where(kFieldNameId, CondGt, Query(joinNs).ReqTotal())}};
	for (const auto& [dsl, expected, direction] : cases) {
		if (std::holds_alternative<Query>(expected)) {
			const Query& q = std::get<Query>(expected);
			if (direction & GEN) {
				reindexer::WrSerializer ser;
				reindexer::prettyPrintJSON(std::string_view(q.GetJSON()), ser, 3);
				EXPECT_EQ(ser.Slice(), dsl);
			}
			if (direction & PARSE) {
				try {
					Query parsed = Query::FromJSON(dsl);
					EXPECT_EQ(parsed, q) << dsl;
				} catch (const Error& err) {
					ADD_FAILURE() << "Unexpected error: " << err.what() << "\nDSL: " << dsl;
					continue;
				}
			}
		} else {
			const Error& expectedErr = std::get<Error>(expected);
			try {
				Query parsed = Query::FromJSON(dsl);
				ADD_FAILURE() << "Expected error: " << expectedErr.what() << "\nDSL: " << dsl;
			} catch (const Error& err) {
				EXPECT_STREQ(err.what(), expectedErr.what()) << "\nDSL: " << dsl;
			}
		}
	}
}

static std::vector<int> generateForcedSortOrder(int maxValue, size_t size) {
	std::set<int> res;
	while (res.size() < size) {
		res.insert(rand() % maxValue);
	}
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
		ExecuteAndVerify(Query(forcedSortNs).Sort(kFieldNameColumnHash, desc, forcedSortOrder).Offset(offset).Limit(limit),
						 kFieldNameColumnHash, expectedResults);
		expectedResults = ForcedSortOffsetTestExpectedResults(offset, limit, desc, forcedSortOrder, Second);
		ExecuteAndVerify(Query(forcedSortNs).Sort(kFieldNameColumnTree, desc, forcedSortOrder).Offset(offset).Limit(limit),
						 kFieldNameColumnTree, expectedResults);
		// Multicolumn sort
		const bool desc2 = rand() % 2;
		auto expectedResultsMult = ForcedSortOffsetTestExpectedResults(offset, limit, desc, desc2, forcedSortOrder, First);
		ExecuteAndVerify(Query(forcedSortNs)
							 .Sort(kFieldNameColumnHash, desc, forcedSortOrder)
							 .Sort(kFieldNameColumnTree, desc2)
							 .Offset(offset)
							 .Limit(limit),
						 kFieldNameColumnHash, expectedResultsMult.first, kFieldNameColumnTree, expectedResultsMult.second);
		expectedResultsMult = ForcedSortOffsetTestExpectedResults(offset, limit, desc, desc2, forcedSortOrder, Second);
		ExecuteAndVerify(Query(forcedSortNs)
							 .Sort(kFieldNameColumnTree, desc2, forcedSortOrder)
							 .Sort(kFieldNameColumnHash, desc)
							 .Offset(offset)
							 .Limit(limit),
						 kFieldNameColumnHash, expectedResultsMult.first, kFieldNameColumnTree, expectedResultsMult.second);
	}
}

TEST_F(QueriesApi, ForcedSortByValuesOfWrongTypes) {
	FillForcedSortNamespace();
	reindexer::QueryResults qr;
	auto query = Query(forcedSortNs).Sort(kFieldNameColumnString, true, std::vector{Variant{VariantArray{Variant{1}, Variant{3}}}});
	const Error err = rt.reindexer->Select(query, qr);
	ASSERT_FALSE(err.ok()) << query.GetSQL();
}

TEST_F(QueriesApi, StrictModeTest) {
	FillTestSimpleNamespace();

	const std::string kNotExistingField = "some_random_name123";
	QueryResults qr;
	{
		Query query = Query(testSimpleNs).Where(kNotExistingField, CondEmpty, VariantArray{});
		Error err = rt.reindexer->Select(query.Strict(StrictModeNames), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr.Clear();
		err = rt.reindexer->Select(query.Strict(StrictModeIndexes), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr = rt.Select(query.Strict(StrictModeNone));
		Verify(qr, Query(testSimpleNs), *rt.reindexer);
		qr.Clear();
	}

	{
		Query query = Query(testSimpleNs).Where(kNotExistingField, CondEq, 0);
		Error err = rt.reindexer->Select(query.Strict(StrictModeNames), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr.Clear();
		err = rt.reindexer->Select(query.Strict(StrictModeIndexes), qr);
		EXPECT_EQ(err.code(), errStrictMode);
		qr = rt.Select(query.Strict(StrictModeNone));
		EXPECT_EQ(qr.Count(), 0);
	}
}

TEST_F(QueriesApi, SQLLeftJoinSerialize) {
	const char* condNames[] = {"IS NOT NULL", "=", "<", "<=", ">", ">=", "RANGE", "IN", "ALLSET", "IS NULL", "LIKE"};
	constexpr auto sqlTemplate = "SELECT * FROM tleft LEFT JOIN tright ON {}.{} {} {}.{}";

	const std::string tLeft = "tleft";
	const std::string tRight = "tright";
	const std::string iLeft = "ileft";
	const std::string iRight = "iright";

	auto createQuery = [&sqlTemplate, &condNames](const std::string& leftTable, const std::string& rightTable, const std::string& leftIndex,
												  const std::string& rightIndex, CondType t) -> std::string {
		return fmt::format(sqlTemplate, leftTable, leftIndex, condNames[t], rightTable, rightIndex);
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
				ASSERT_EQ(sqlQCmp, wrSer.Slice());
			}

			{
				std::string sqlQ = createQuery(tLeft, tRight, iLeft, iRight, c.first);
				Query qSql = Query::FromSQL(sqlQ);

				reindexer::WrSerializer wrSer;
				qSql.GetSQL(wrSer);
				ASSERT_EQ(sqlQ, wrSer.Slice());
			}
			{
				std::string sqlQ = createQuery(tRight, tLeft, iRight, iLeft, c.second);
				Query qSql = Query::FromSQL(sqlQ);
				ASSERT_EQ(q.GetJSON(), qSql.GetJSON());
				reindexer::WrSerializer wrSer;
				qSql.GetSQL(wrSer);
				ASSERT_EQ(sqlQ, wrSer.Slice());
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
		rt.OpenNamespace(nsName);
		rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "tree", "int", IndexOpts{}.PK()});
		for (int i = 0; i < kItemsCount; ++i) {
			ser.Reset();
			reindexer::JsonBuilder json{ser};
			json.Put("id", i);
			if (i % 2 == 1) {
				json.Put("f", i);
			}
			json.End();
			Item item(rt.NewItem(nsName));
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			auto err = item.FromJSON(ser.Slice());
			ASSERT_TRUE(err.ok()) << err.what();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			Upsert(nsName, item);
		}
	}
	auto qr = rt.Select(Query(leftNs).Strict(StrictModeNames).Join(InnerJoin, Query(rightNs).Where("id", CondGe, 5)).On("f", CondEq, "f"));
	const int expectedIds[] = {5, 7, 9};
	ASSERT_EQ(qr.Count(), sizeof(expectedIds) / sizeof(int));
	unsigned i = 0;
	for (auto& it : qr) {
		Item item(it.GetItem(false));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		VariantArray values = item["id"];
		ASSERT_EQ(values.size(), 1);
		EXPECT_EQ(values[0].As<int>(), expectedIds[i++]);
	}
}

TEST_F(QueriesApi, AllSet) {
	const std::string nsName = "allset_ns";
	rt.OpenNamespace(nsName);
	rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	reindexer::WrSerializer ser;
	reindexer::JsonBuilder json{ser};
	json.Put("id", 0);
	json.Array("array", {0, 1, 2});
	json.End();
	Item item(rt.NewItem(nsName));
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	auto err = item.FromJSON(ser.Slice());
	ASSERT_TRUE(err.ok()) << err.what();
	ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	Upsert(nsName, item);
	Query q{nsName};
	q.Where("array", CondAllSet, {0, 1, 2});
	auto qr = rt.Select(q);
	EXPECT_EQ(qr.Count(), 1);
}

TEST_F(QueriesApi, SetByTreeIndex) {
	// Execute query with sort and set condition by btree index
	const std::string nsName = "set_by_tree_ns";
	constexpr int kMaxID = 20;
	rt.OpenNamespace(nsName);
	rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "tree", "int", IndexOpts{}.PK()});
	setPkFields(nsName, {"id"});
	for (int id = kMaxID; id != 0; --id) {
		Item item(rt.NewItem(nsName));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		item["id"] = id;
		Upsert(nsName, item);
		saveItem(std::move(item), nsName);
	}

	Query q{nsName};
	q.Where("id", CondSet, {rand() % kMaxID, rand() % kMaxID, rand() % kMaxID, rand() % kMaxID}).Sort("id", false);
	{
		QueryResults qr;
		ExecuteAndVerifyWithSql(q, qr);
		// Expecting no sort index and filtering by index
		EXPECT_NE(qr.GetExplainResults().find(",\"sort_index\":\"-\","), std::string::npos);
		EXPECT_NE(qr.GetExplainResults().find(",\"method\":\"index\","), std::string::npos);
		EXPECT_EQ(qr.GetExplainResults().find("\"scan\""), std::string::npos);
	}

	{
		// Execute the same query after indexes optimization
		AwaitIndexOptimization(nsName);
		QueryResults qr;
		ExecuteAndVerifyWithSql(q, qr);
		// Expecting 'id' as a sort index and filtering by index
		EXPECT_NE(qr.GetExplainResults().find(",\"sort_index\":\"id\","), std::string::npos);
		EXPECT_NE(qr.GetExplainResults().find(",\"method\":\"index\","), std::string::npos);
		EXPECT_EQ(qr.GetExplainResults().find("\"scan\""), std::string::npos);
	}
}

TEST_F(QueriesApi, TestCsvParsing) {
	std::vector<std::vector<std::string_view>> fieldsArr{
		{"field0", "", "\"field1\"", "field2", "", "", "", "field3", "field4", "", "", "field5", ""},
		{"", "", "\"field6\"", "field7", "field8", "field9", "", "", "", "", "", "field10", "field11"},
		{"field12", "field13", "\"field14\"", "", "", "", "", "field15", "field16", "", "", "field17", "field18"},
		{"", "", "\"field19\"", "field20", "", "", "", "field21", "", "", "field22", "", ""},
		{"", "", "\"\"", "", "", "", "", "", "", "", "", "", ""},
		{"", "field23", "\"field24\"", "field25", "", "", "", "", "field26", "", "", "", ""}};

	std::string_view dblQuote = "\"\"";

	for (const auto& fields : fieldsArr) {
		std::stringstream ss;
		for (size_t i = 0; i < fields.size(); ++i) {
			if (i == 2) {
				ss << dblQuote << fields[i] << dblQuote;
			} else {
				ss << fields[i];
			}
			if (i < fields.size() - 1) {
				ss << ',';
			}
		}

		auto resFields = reindexer::parseCSVRow(ss.str());
		ASSERT_EQ(resFields.size(), fields.size());

		for (size_t i = 0; i < fields.size(); ++i) {
			ASSERT_EQ(resFields[i], fields[i]);
		}
	}
}

TEST_F(QueriesApi, TestCsvProcessingWithSchema) {
	using namespace std::string_literals;
	std::array<const std::string, 3> nsNames = {"csv_test1", "csv_test2", "csv_test3"};

	auto openNs = [this](std::string_view nsName) {
		rt.OpenNamespace(nsName);
		rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	};

	for (auto& nsName : nsNames) {
		openNs(nsName);
	}

	const std::string jsonschema = R"!(
	{
		"required":
		[
			"id",
			"Field0",
			"Field1",
			"Field2",
			"Field3",
			"Field4",
			"Field5",
			"Field6",
			"Field7",
			"quoted_field",
			"join_field",
			"Array_level0_id_0",
			"Array_level0_id_1",
			"Array_level0_id_2",
			"Array_level0_id_3",
			"Array_level0_id_4",
			"Object_level0_id_0",
			"Object_level0_id_1",
			"Object_level0_id_2",
			"Object_level0_id_3",
			"Object_level0_id_4"
		],
		"properties":
		{
			"id": { "type": "int" },
			"Field0": { "type": "string" },
			"Field1": { "type": "string" },
			"Field2": { "type": "string" },
			"Field3": { "type": "string" },
			"Field4": { "type": "string" },
			"Field5": { "type": "string" },
			"Field6": { "type": "string" },
			"Field7": { "type": "string" },

			"quoted_field":{ "type": "string" },
			"join_field": { "type": "int" },

			"Array_level0_id_0":{"items":{"type": "string"},"type": "array"},
			"Array_level0_id_1":{"items":{"type": "string"},"type": "array"},
			"Array_level0_id_2":{"items":{"type": "string"},"type": "array"},
			"Array_level0_id_3":{"items":{"type": "string"},"type": "array"},
			"Array_level0_id_4":{"items":{"type": "string"},"type": "array"}

			"Object_level0_id_0":{"additionalProperties": false,"type": "object"},
			"Object_level0_id_1":{"additionalProperties": false,"type": "object"},
			"Object_level0_id_2":{"additionalProperties": false,"type": "object"},
			"Object_level0_id_3":{"additionalProperties": false,"type": "object"},
			"Object_level0_id_4":{"additionalProperties": false,"type": "object"}
		},
		"additionalProperties": false,
		"type": "object"
	})!";

	rt.SetSchema(nsNames[0], jsonschema);
	int fieldNum = 0;
	const auto addItem = [&fieldNum, this](int id, std::string_view nsName, bool needJoinField = true) {
		reindexer::WrSerializer ser;
		{
			reindexer::JsonBuilder json{ser};
			json.Put("id", id);
			json.Put(fmt::format("Field{}", fieldNum), fmt::format("field_{}_data", fieldNum));
			++fieldNum;
			json.Put(fmt::format("Field{}", fieldNum), fmt::format("field_{}_data", fieldNum));
			++fieldNum;
			json.Put(fmt::format("Field{}", fieldNum), fmt::format("field_{}_data", fieldNum));
			++fieldNum;
			json.Put(fmt::format("Field{}", fieldNum), fmt::format("field_{}_data", fieldNum));
			json.Put("quoted_field", "\"field_with_\"quoted\"");
			if (needJoinField) {
				json.Put("join_field", id % 3);
			}
			{
				auto data0 = json.Array(fmt::format("Array_level0_id_{}", id));
				for (int i = 0; i < 5; ++i) {
					data0.Put(reindexer::TagName::Empty(), fmt::format("array_data_0_{}", i));
				}
				data0.Put(reindexer::TagName::Empty(), std::string("\"arr_quoted_field(\"this is quoted too\")\""));
			}
			{
				auto data0 = json.Object(fmt::format("Object_level0_id_{}", id));
				for (int i = 0; i < 5; ++i) {
					data0.Put(fmt::format("Object_{}", i), fmt::format("object_data_0_{}", i));
				}
				data0.Put("Quoted Field lvl0", std::string("\"obj_quoted_field(\"this is quoted too\")\""));
				{
					auto data1 = data0.Object(fmt::format("Object_level1_id_{}", id));
					for (int j = 0; j < 5; ++j) {
						data1.Put(fmt::format("objectData1 {}", j), fmt::format("objectData1 {}", j));
					}
					data1.Put("Quoted Field lvl1", std::string("\"obj_quoted_field(\"this is quoted too\")\""));
				}
			}
		}
		Item item(rt.NewItem(nsName));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		Upsert(nsName, item);
	};

	for (auto& nsName : nsNames) {
		for (int i = 0; i < 5; i++) {
			addItem(i, nsName, !(i == 4 && nsName == "csv_test1"));	 // one item for check when item without joined nss
			fieldNum -= 2;
		}
	}

	Query q = Query{nsNames[0]};
	q.Join(LeftJoin, "join_field", "join_field", CondEq, OpAnd, Query(nsNames[1]));
	q.Join(LeftJoin, "id", "join_field", CondEq, OpAnd, Query(nsNames[2]));
	auto qr = rt.Select(q);

	for (auto& ordering : std::array<reindexer::CsvOrdering, 2>{qr.GetSchema(0)->MakeCsvTagOrdering(qr.GetTagsMatcher(0)),
																qr.ToLocalQr().MakeCSVTagOrdering(std::numeric_limits<int>::max(), 0)}) {
		auto csv2jsonSchema = [&ordering, &qr] {
			std::vector<std::string> res;
			for (auto tag : ordering) {
				const auto tm = qr.GetTagsMatcher(0);
				res.emplace_back(tm.tag2name(tag));
			}
			res.emplace_back("joined_nss_map");
			return res;
		}();

		reindexer::WrSerializer serCsv, serJson;
		for (auto& q : qr) {
			auto err = q.GetCSV(serCsv, ordering);
			ASSERT_TRUE(err.ok()) << err.what();

			err = q.GetJSON(serJson, false);
			ASSERT_TRUE(err.ok()) << err.what();

			gason::JsonParser parserCsv, parserJson;
			auto converted = parserCsv.Parse(std::string_view(reindexer::csv2json(serCsv.Slice(), csv2jsonSchema)));
			auto orig = parserJson.Parse(serJson.Slice());

			// for check that all tags related to joined nss from json-result are present in csv-result
			std::set<std::string_view> checkJoinedNssTags;
			for (const auto& node : orig) {
				if (std::string_view(node.key).substr(0, 7) == "joined_") {
					checkJoinedNssTags.insert(node.key);
				}
			}

			for (const auto& fieldName : csv2jsonSchema) {
				if (fieldName == "joined_nss_map" && !converted[fieldName].empty()) {
					EXPECT_EQ(converted[fieldName].value.getTag(), gason::JsonTag::OBJECT);
					for (auto& node : converted[fieldName]) {
						EXPECT_TRUE(!orig[node.key].empty()) << "not found joined data: " << node.key;
						auto origStr = reindexer::stringifyJson(orig[node.key]);
						auto convertedStr = reindexer::stringifyJson(node);
						EXPECT_EQ(origStr, convertedStr);
						checkJoinedNssTags.erase(node.key);
					}
					continue;
				}
				if (converted[fieldName].empty() || orig[fieldName].empty()) {
					EXPECT_TRUE(converted[fieldName].empty() && orig[fieldName].empty()) << "fieldName: " << fieldName;
					continue;
				}
				switch (orig[fieldName].value.getTag()) {
					case gason::JsonTag::NUMBER:
						EXPECT_EQ(orig[fieldName].As<int>(), converted[fieldName].As<int>());
						break;
					case gason::JsonTag::STRING:
						EXPECT_EQ(orig[fieldName].As<std::string>(), converted[fieldName].As<std::string>());
						break;
					case gason::JsonTag::OBJECT:
					case gason::JsonTag::ARRAY: {
						auto origStr = reindexer::stringifyJson(orig[fieldName]);
						auto convertedStr = reindexer::stringifyJson(converted[fieldName]);
						EXPECT_EQ(origStr, convertedStr);
					} break;
					case gason::JsonTag::DOUBLE:
					case gason::JsonTag::JFALSE:
					case gason::JsonTag::JTRUE:
					case gason::JsonTag::JSON_NULL:
					case gason::JsonTag::EMPTY:
						break;
				}
			}

			EXPECT_TRUE(checkJoinedNssTags.empty());
			serCsv.Reset();
			serJson.Reset();
		}
	}
}

TEST_F(QueriesApi, ConvertStringToDoubleDuringSorting) {
	using namespace std::string_literals;
	const std::string nsName = "ns_convert_string_to_double_during_sorting";
	rt.OpenNamespace(nsName);
	rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	rt.AddIndex(nsName, reindexer::IndexDef{"str_idx", {"str_idx"}, "hash", "string", IndexOpts{}});

	const auto addItem = [&](int id, std::string_view strIdx, std::string_view strFld) {
		reindexer::WrSerializer ser;
		{
			reindexer::JsonBuilder json{ser};
			json.Put("id", id);
			json.Put("str_idx", strIdx);
			json.Put("str_fld", strFld);
		}
		Item item(rt.NewItem(nsName));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		Upsert(nsName, item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	};
	addItem(0, "123.5", "123.5");
	addItem(1, " 23.5", " 23.5");
	addItem(2, "3.5 ", "3.5 ");
	addItem(3, " .5", " .5");
	addItem(4, " .15 ", " .15 ");
	addItem(10, "123.5 and something", "123.5 and something");
	addItem(11, " 23.5 and something", " 23.5 and something");
	addItem(12, "3.5 and something", "3.5 and something");
	addItem(13, " .5 and something", " .5 and something");

	for (const auto& f : {"str_idx"s, "str_fld"s}) {
		Query q = Query{nsName}.Where("id", CondLt, 5).Sort("2 * "s + f, false).Strict(StrictModeNames);
		auto qr = rt.Select(q);
		int prevId = 10;
		for (auto& it : qr) {
			ASSERT_TRUE(it.Status().ok()) << it.Status().what();
			const auto item = it.GetItem();
			ASSERT_TRUE(item.Status().ok()) << item.Status().what();
			const auto currId = item["id"].As<int>();
			EXPECT_LT(currId, prevId);
			prevId = currId;
		}
	}

	for (const auto& f : {"str_idx"s, "str_fld"s}) {
		Query q = Query{nsName}.Where("id", CondGt, 5).Sort("2 * "s + f, false).Strict(StrictModeNames);
		reindexer::QueryResults qr;
		auto err = rt.reindexer->Select(q, qr);
		EXPECT_FALSE(err.ok());
		EXPECT_THAT(err.what(), testing::MatchesRegex("Can't convert '.*' to number"));
	}
}

std::string print(const reindexer::Query& q, reindexer::QueryResults::Iterator& currIt, reindexer::QueryResults::Iterator& prevIt,
				  const reindexer::QueryResults& qr) {
	assertrx(currIt.Status().ok());
	std::string res = '\n' + q.GetSQL() + "\ncurr: ";
	reindexer::WrSerializer ser;
	const auto err = currIt.GetJSON(ser, false);
	assertrx(err.ok());
	res += ser.Slice();
	if (prevIt != qr.end()) {
		assertrx(prevIt.Status().ok());
		res += "\nprev: ";
		ser.Reset();
		const auto err = prevIt.GetJSON(ser, false);
		assertrx(err.ok());
		res += ser.Slice();
	}
	return res;
}

void QueriesApi::sortByNsDifferentTypesImpl(std::string_view fillingNs, const reindexer::Query& qTemplate, const std::string& sortPrefix) {
	const auto addItem = [&](int id, const auto& v) {
		reindexer::WrSerializer ser;
		{
			reindexer::JsonBuilder json{ser};
			json.Put("id", id);
			json.Put("value", v);
			{
				auto obj = json.Object("object");
				obj.Put("nested_value", v);
			}
		}
		Item item(rt.NewItem(fillingNs));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		const auto err = item.FromJSON(ser.Slice());
		ASSERT_TRUE(err.ok()) << err.what();
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		Upsert(fillingNs, item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	};
	for (int id = 0; id < 100; ++id) {
		addItem(id, id);
	}
	for (int id = 100; id < 200; ++id) {
		addItem(id, int64_t(id));
	}
	for (int id = 200; id < 300; ++id) {
		addItem(id, double(id) + 0.5);
	}
	for (int id = 500; id < 600; ++id) {
		addItem(id, std::to_string(id));
	}
	for (int id = 600; id < 700; ++id) {
		addItem(id, std::to_string(id) + RandString());
	}
	for (int id = 700; id < 800; ++id) {
		addItem(id, char('a' + (id % 100) / 10) + std::string{char('a' + id % 10)} + RandString());
	}

	const auto check = [&](CondType cond, std::vector<int> values, const char* expectedErr = nullptr) {
		for (bool desc : {true, false}) {
			for (const char* sortField : {"value", "object.nested_value"}) {
				auto q = qTemplate;
				q.Where("id", cond, values).Sort(sortPrefix + sortField, desc);
				reindexer::QueryResults qr;
				const auto err = rt.reindexer->Select(q, qr);
				if (expectedErr) {
					EXPECT_FALSE(err.ok()) << q.GetSQL();
					EXPECT_STREQ(err.what(), expectedErr) << q.GetSQL();
				} else {
					ASSERT_TRUE(err.ok()) << err.what() << '\n' << q.GetSQL();
					switch (cond) {
						case CondRange:
							EXPECT_EQ(qr.Count(), values.at(1) - values.at(0) + 1) << q.GetSQL();
							break;
						case CondSet:
						case CondEq:
							EXPECT_EQ(qr.Count(), values.size()) << q.GetSQL();
							break;
						case CondAny:
						case CondEmpty:
						case CondLike:
						case CondDWithin:
						case CondLt:
						case CondLe:
						case CondGt:
						case CondGe:
						case CondAllSet:
						case CondKnn:
							assert(0);
					}
					int prevId = 10000 * (desc ? 1 : -1);
					auto prevIt = qr.end();
					for (auto& it : qr) {
						ASSERT_TRUE(it.Status().ok()) << it.Status().what() << print(q, it, prevIt, qr);
						const auto item = it.GetItem();
						ASSERT_TRUE(item.Status().ok()) << item.Status().what() << print(q, it, prevIt, qr);
						const auto currId = item["id"].As<int>();
						if (desc) {
							EXPECT_LT(currId, prevId) << print(q, it, prevIt, qr);
						} else {
							EXPECT_GT(currId, prevId) << print(q, it, prevIt, qr);
						}
						prevId = currId;
						prevIt = it;
					}
				}
			}
		}
	};
	// same types
	for (int id : {0, 100, 200, 500, 600, 700}) {
		check(CondRange, {id, id + 99});
	}
	// numeric types
	check(CondRange, {0, 299});
	// string
	check(CondRange, {500, 799});
	// different types
	for (int i = 0; i < 10; ++i) {
		check(CondSet, {rand() % 100 + 100, 500 + rand() % 300}, "Not comparable types: string and int64");
		check(CondSet, {rand() % 100 + 200, 500 + rand() % 300}, "Not comparable types: string and double");
	}
}

TEST_F(QueriesApi, SortByJoinedNsDifferentTypes) {
	const std::string nsMain{"sort_by_joined_ns_different_types_main"};
	const std::string nsRight{"sort_by_joined_ns_different_types_right"};
	rt.OpenNamespace(nsMain);
	rt.AddIndex(nsMain, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	rt.OpenNamespace(nsRight);
	rt.AddIndex(nsRight, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	for (int id = 0; id < 1000; ++id) {
		Item item(rt.NewItem(nsMain));
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		item["id"] = id;
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
		Upsert(nsMain, item);
		ASSERT_TRUE(item.Status().ok()) << item.Status().what();
	}

	sortByNsDifferentTypesImpl(nsRight, Query{nsMain}.InnerJoin("id", "id", CondEq, Query{nsRight}), nsRight + '.');
}

TEST_F(QueriesApi, SortByFieldWithDifferentTypes) {
	const std::string nsName{"sort_by_field_different_types"};
	rt.OpenNamespace(nsName);
	rt.AddIndex(nsName, reindexer::IndexDef{"id", {"id"}, "hash", "int", IndexOpts{}.PK()});
	sortByNsDifferentTypesImpl(nsName, Query{nsName}, "");
}

TEST_F(QueriesApi, SerializeDeserialize) {
	Query queries[]{
		Query(default_namespace).Where(Query(default_namespace), CondAny, VariantArray{}),
		Query(default_namespace).Where(kFieldNameUuidArr, CondRange, randHeterogeneousUuidArray(2, 2)),
		Query(default_namespace)
			.WhereComposite(kCompositeFieldUuidName, CondRange,
							{VariantArray::Create(nilUuid(), RandString()), VariantArray::Create(randUuid(), RandString())}),
		Query(default_namespace).Where(Query(default_namespace).Where(kFieldNameId, CondEq, 10), CondAny, VariantArray{}),
		Query(default_namespace).Not().Where(Query(default_namespace), CondEmpty, VariantArray{}),
		Query(default_namespace).Where(kFieldNameId, CondLt, Query(default_namespace).Aggregate(AggAvg, {kFieldNameId})),
		Query(default_namespace)
			.Where(kFieldNameGenre, CondSet, Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondSet, {10, 20, 30, 40})),

		Query(default_namespace).Where(Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondGt, 10), CondSet, {10, 20, 30, 40}),
		Query(default_namespace)
			.Where(Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondGt, 10).Offset(1), CondSet, {10, 20, 30, 40}),
		Query(default_namespace)
			.Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).Aggregate(AggMax, {kFieldNameGenre}), CondRange, {48, 50}),
		Query(default_namespace).Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).ReqTotal(), CondGt, {50}),
		Query(default_namespace)
			.Debug(LogTrace)
			.Where(kFieldNameGenre, CondEq, 5)
			.Not()
			.Where(Query(default_namespace).Where(kFieldNameGenre, CondEq, 5), CondAny, VariantArray{})
			.Or()
			.Where(kFieldNameGenre, CondSet, Query(joinNs).Select({kFieldNameGenre}).Where(kFieldNameId, CondSet, {10, 20, 30, 40}))
			.Not()
			.OpenBracket()
			.Where(kFieldNameYear, CondRange, {2001, 2020})
			.Or()
			.Where(kFieldNameName, CondLike, RandLikePattern())
			.Or()
			.Where(Query(joinNs).Where(kFieldNameYear, CondEq, 2000 + rand() % 210), CondEmpty, VariantArray{})
			.CloseBracket()
			.Or()
			.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
			.OpenBracket()
			.Where(kFieldNameNumeric, CondLt, std::to_string(600))
			.Not()
			.OpenBracket()
			.Where(kFieldNamePackages, CondSet, RandIntVector(5, 10000, 50))
			.Where(kFieldNameGenre, CondLt, 6)
			.Or()
			.Where(kFieldNameId, CondLt, Query(default_namespace).Aggregate(AggAvg, {kFieldNameId}))
			.CloseBracket()
			.Not()
			.Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).Aggregate(AggMax, {kFieldNameGenre}), CondRange, {48, 50})
			.Or()
			.Where(kFieldNameYear, CondEq, 10)
			.CloseBracket(),

		Query(default_namespace)
			.Where(kCompositeFieldIdTemp, CondEq, Query(default_namespace).Select({kCompositeFieldIdTemp}).Where(kFieldNameId, CondGt, 10)),
		Query(default_namespace)
			.Where(Query(default_namespace).Select({kCompositeFieldUuidName}).Where(kFieldNameId, CondGt, 10), CondRange,
				   {VariantArray::Create(nilUuid(), RandString()), VariantArray::Create(randUuid(), RandString())}),
		Query(default_namespace)
			.Where(Query(default_namespace).Select({kCompositeFieldAgeGenre}).Where(kFieldNameId, CondGt, 10).Limit(10), CondLe,
				   {Variant(VariantArray::Create(rand() % 50, rand() % 50))}),
	};
	for (Query& q : queries) {
		reindexer::WrSerializer wser;
		q.Serialize(wser);
		reindexer::Serializer rser(wser.Slice());
		const auto deserializedQuery = Query::Deserialize(rser);
		EXPECT_EQ(q, deserializedQuery) << "Origin query:\n" << q.GetSQL() << "\nDeserialized query:\n" << deserializedQuery.GetSQL();
	}
}

TEST_F(QueriesApi, DistinctWithDuplicatesWhereCondTest) {
	for (int id = 0; id < 10; ++id) {
		auto item = GenerateDefaultNsItem(id, 0);
		item[kFieldNameAge] = id / 4;
		Upsert(default_namespace, item);
	}

	auto check = [this](auto&&... args) {
		auto values = VariantArray::Create({std::forward<decltype(args)>(args)...});
		auto q = Query(default_namespace).Distinct(kFieldNameAge).Where(kFieldNameAge, CondEq, values);
		std::sort(values.begin(), values.end());
		auto it = std::unique(values.begin(), values.end());
		values.erase(it, values.end());

		auto qr = rt.Select(q);
		ASSERT_EQ(qr.Count(), values.size());

		unsigned int rows = qr.GetAggregationResults().front().GetDistinctRowCount();
		ASSERT_EQ(rows, values.size());
		VariantArray distincts;
		for (size_t i = 0; i < rows; ++i) {
			auto d = qr.GetAggregationResults().front().GetDistinctRow(i);
			ASSERT_EQ(d.size(), 1);
			distincts.emplace_back(d[0]);
		}
		std::sort(distincts.begin(), distincts.end());
		for (size_t i = 0; i < distincts.size(); ++i) {
			ASSERT_EQ(distincts[i], values[i]);
		}
	};

	check(1);
	check(0, 1);
	check(1, 0, 2);
	check(0, 1, 2);
	check(0, 0, 0, 0, 0);
	check(0, 1, 1, 1, 2);
	check(0, 0, 1, 1, 1, 1, 1, 1);
	check(0, 0, 1, 1, 1, 1, 2, 1);
	check(0, 0, 1, 1, 2, 1, 2, 1);
	check(1, 2, 0, 2, 0, 1, 2, 2, 1, 0);
}

TEST_F(QueriesApi, ExtraSpacesUpdateSQL) {
	{
		SCOPED_TRACE("ExtraSpacesUpdateSQL Step 1");
		Query updateQuery = Query::FromSQL(
			R"(update #config set "profiling.long_queries_logging.update_delete" = {"threshold_us":11, "normalized":false} where "type"='profiling')");
		std::ignore = rt.Update(updateQuery);
	}
	{
		SCOPED_TRACE("ExtraSpacesUpdateSQL Step 2");
		Query updateQuery = Query::FromSQL(
			R"(update #config set "profiling.long_queries_logging.update_delete"={ "threshold_us" : 11, "normalized":false } where "type"='profiling')");
		std::ignore = rt.Update(updateQuery);
	}
	{
		SCOPED_TRACE("ExtraSpacesUpdateSQL Step 3");
		Query updateQuery = Query::FromSQL(
			R"(update #config set "profiling.long_queries_logging.update_delete"={  "threshold_us" : 11,  "normalized": false } where "type" = 'profiling')");
		std::ignore = rt.Update(updateQuery);
	}
}

TEST_F(QueriesApi, EmptyResultForceSortedWithLimitTest) {
	for (int id = 0; id < 10; ++id) {
		auto item = GenerateDefaultNsItem(id, 0);
		item[kFieldNameIsDeleted] = true;
		Upsert(default_namespace, item);
	}

	auto check = [this](Query& q, int expected) {
		QueryResults qr = rt.Select(q);
		ASSERT_EQ(qr.Count(), expected);
	};

	auto query = Query(default_namespace).Where(kFieldNameIsDeleted, CondEq, {true}).Sort(kFieldNameGenre, false, {1, 2, 3}).Limit(10);
	check(query, 10);
	query.Where(kFieldNameIsDeleted, CondEq, {false});
	check(query, 0);
}

TEST_F(QueriesApi, DistinctWithForcedSortAndLimitTest) {
	std::vector<Item> items;
	for (int id = 0; id < 10; ++id) {
		items.emplace_back(GenerateDefaultNsItem(id, 0));
		auto& item = items.back();
		item[kFieldNameAge] = id / 3;
		Upsert(default_namespace, item);
	}

	VariantArray forceSortOrder{Variant{5}, Variant{7}, Variant{1}};

	auto query = Query(default_namespace).Distinct(kFieldNameAge).Sort(kFieldNameId, false, forceSortOrder).Limit(5);

	int expectedCnt = 4;
	QueryResults qr = rt.Select(query);
	ASSERT_EQ(qr.Count(), expectedCnt);

	ASSERT_EQ(qr.GetAggregationResults().front().GetDistinctRowCount(), expectedCnt);

	size_t order = 0;
	for (auto it : qr) {
		auto item = it.GetItem();
		gason::JsonParser parser;
		auto root = parser.Parse(item.GetJSON());
		int id = root[kFieldNameId].As<int>();
		ASSERT_EQ(id, order < forceSortOrder.size() ? int(forceSortOrder[order]) : 9) << fmt::format("order: {}", order);
		ASSERT_EQ(root[kFieldNameAge].As<int>(), items[id][kFieldNameAge].As<int>()) << fmt::format("Id: {}; order: {}", id, order);
		order++;
	}
}
