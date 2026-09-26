#include <gmock/gmock.h>
#include "core/query/query_impl.h"

#include <optional>
#include <thread>
#include <tuple>

#include "core/cjson/csvbuilder.h"
#include "core/enums.h"
#include "core/function/function.h"
#include "core/function/precomputed_values.h"
#include "core/query/expression/arithmetic_expression.h"
#include "core/query/functions_optimizations.h"
#include "core/schema.h"
#include "csv2jsonconverter.h"
#include "queries_api.h"
#include "tools/json2kv.h"
#include "tools/jsontools.h"
#include "tools/timetools.h"

namespace reindexer_tests {

using reindexer::IndexOpts;

TEST(QueryImplWrapper, EqualityAndNestedWalk) {
	using namespace reindexer;

	Query left{"main"};
	Query right{"main"};
	Impl(left).Join(OpAnd, JoinedQuery{JoinType::LeftJoin, Query{"joined"}});
	Impl(right).Join(OpAnd, JoinedQuery{JoinType::InnerJoin, Query{"joined"}});
	EXPECT_NE(left, right);

	JoinedQuery leftJoin{JoinType::LeftJoin, Query{"joined"}};
	JoinedQuery rightJoin{JoinType::LeftJoin, Query{"joined"}};
	JoinedImpl(leftJoin).EmplaceBackOnEntry(OpAnd, "left", CondEq, "right");
	JoinedImpl(rightJoin).EmplaceBackOnEntry(OpAnd, "left", CondEq, "other");
	EXPECT_NE(leftJoin, rightJoin);

	Query nested{"main"};
	Impl(nested).Join(OpAnd, std::move(leftJoin));
	nested.Merge(Query{"merged"});
	size_t visited = 0;
	Impl(std::as_const(nested)).WalkNested(true, true, true, [&visited](ConstQueryImpl) noexcept { ++visited; });
	EXPECT_EQ(visited, 3);
}

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
		CheckArithmeticQueries();
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
		CheckArithmeticQueries();
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

TEST_F(QueriesApi, SelectionResultsJsonTest) {
	// Check, that selected JSON corresponds to CJSON for each individual field
	FillDefaultNamespace(0, 100, 1);
	QueryResults qr = rt.Select(Query(default_namespace));
	VariantArray jsonArr, cjsonArr;
	reindexer::fast_hash_set<std::string_view> found;
	std::vector<std::string_view> testFields{kFieldNameId,			kFieldNameYear,		 kFieldNameYearSparse, kFieldNameGenre,
											 kFieldNameName,		kFieldNameCountries, kFieldNameAge,		   kFieldNameDescription,
											 kFieldNamePackages,	kFieldNameRate,		 kFieldNamePriceId,	   kFieldNameLocation,
											 kFieldNameStartTime,	kFieldNameEndTime,	 kFieldNameActor,	   kFieldNameNumeric,
											 kFieldNameBtreeIdsets, kFieldNameUuid,		 kFieldNameUuidArr};
	for (auto it : qr) {
		auto item = it.GetItem();
		gason::JsonParser parser;
		auto json = parser.Parse(item.GetJSON());
		for (auto field : testFields) {
			SCOPED_TRACE(field);
			auto node = json[field];
			jsonArr.Clear();
			if (!node.isEmpty()) {
				if (node.isArray()) {
					for (auto& it : node) {
						jsonArr.emplace_back(reindexer::jsonValue2Variant(it.value, reindexer::KeyValueType::Undefined{}, field, nullptr,
																		  reindexer::ConvertToString_False, reindexer::ConvertNull_False));
					}
					std::ignore = jsonArr.MarkArray();
				} else {
					jsonArr.emplace_back(reindexer::jsonValue2Variant(node.value, reindexer::KeyValueType::Undefined{}, field, nullptr,
																	  reindexer::ConvertToString_False, reindexer::ConvertNull_False));
				}
			}
			auto cjsonArr = VariantArray(item[field]);
			for (auto& v : cjsonArr) {
				if (v.Type().Is<reindexer::KeyValueType::Uuid>()) {
					std::ignore = v.convert(reindexer::KeyValueType::String{});
				}
			}
			EXPECT_EQ(jsonArr, cjsonArr) << "jsonArr: " << jsonArr.Dump() << "\ncjsonArr: " << cjsonArr.Dump();
			if (!jsonArr.empty()) {
				found.emplace(field);
			}
		}
	}

	// Check if all fields were found with non-empty values
	std::vector<std::string_view> foundFields(found.begin(), found.end());
	std::ranges::sort(foundFields);
	std::ranges::sort(testFields);
	ASSERT_EQ(foundFields, testFields);
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
	auto q = Query(btreeIdxOptNs).Where(kFieldNameId, CondSet, {3, 5, 7}).Where(kFieldNameStartTime, CondGt, 2).Debug(LogTrace);
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
		 Error{errParseSQL, "Expected field or index name, but found '5' in query, line: 1 column: 51 52"}},
		{"SELECT * FROM test_namespace WHERE \"index\" = 5", Query{"test_namespace"}.Where("index", CondEq, 5), PARSE},
		{"SELECT * FROM test_namespace WHERE 'index' = 5",
		 Error{errParseSQL, "Expected field or index name, but found '5' in query, line: 1 column: 45 46"}},
		{"SELECT * FROM test_namespace WHERE index = true", Query{"test_namespace"}.Where("index", CondEq, true), PARSE},
		{"SELECT * FROM test_namespace WHERE true = index", Query{"test_namespace"}.Where("index", CondEq, true), PARSE},
		{"SELECT * FROM test_namespace WHERE TRUE = index", Query{"test_namespace"}.Where("index", CondEq, true), PARSE},
		{"SELECT * FROM test_namespace WHERE index = false", Query{"test_namespace"}.Where("index", CondEq, false), PARSE},
		{"SELECT * FROM test_namespace WHERE false = index", Query{"test_namespace"}.Where("index", CondEq, false), PARSE},
		{"SELECT * FROM test_namespace WHERE FALSE = index", Query{"test_namespace"}.Where("index", CondEq, false), PARSE},
		{"SELECT * FROM test_namespace WHERE 5 = index", Query{"test_namespace"}.Where("index", CondEq, 5), PARSE},
		{"SELECT * FROM test_namespace WHERE 5 > index", Query{"test_namespace"}.Where("index", CondLt, 5), PARSE},
		{"SELECT * FROM test_namespace WHERE 5 < index", Query{"test_namespace"}.Where("index", CondGt, 5), PARSE},
		{"SELECT * FROM test_namespace WHERE 'asd' = index", Query{"test_namespace"}.Where("index", CondEq, "asd"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = 'true'", Query{"test_namespace"}.Where("index", CondEq, "true"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = 'false'", Query{"test_namespace"}.Where("index", CondEq, "false"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = \"true\"", Query{"test_namespace"}.WhereBetweenFields("index", CondEq, "true"), PARSE},
		{"SELECT * FROM test_namespace WHERE \"true\" = index", Query{"test_namespace"}.WhereBetweenFields("true", CondEq, "index"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = \"false\"", Query{"test_namespace"}.WhereBetweenFields("index", CondEq, "false"),
		 PARSE},
		{"SELECT * FROM test_namespace WHERE \"false\" = index", Query{"test_namespace"}.WhereBetweenFields("false", CondEq, "index"),
		 PARSE},
		{"SELECT * FROM test_namespace WHERE true = \"index\"", Query{"test_namespace"}.Where("index", CondEq, true), PARSE},
		{"SELECT * FROM test_namespace WHERE false = \"index\"", Query{"test_namespace"}.Where("index", CondEq, false), PARSE},
		{"SELECT * FROM test_namespace WHERE \"true\" = 'asd'", Query{"test_namespace"}.Where("true", CondEq, "asd"), PARSE},
		{"SELECT * FROM test_namespace WHERE 'asd' = \"true\"", Query{"test_namespace"}.Where("true", CondEq, "asd"), PARSE},
		{"SELECT * FROM test_namespace WHERE 'asd' = \"false\"", Query{"test_namespace"}.Where("false", CondEq, "asd"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = 'null'", Query{"test_namespace"}.Where("index", CondEq, "null"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = \"null\"", Query{"test_namespace"}.WhereBetweenFields("index", CondEq, "null"), PARSE},
		{"SELECT * FROM test_namespace WHERE \"null\" = index", Query{"test_namespace"}.WhereBetweenFields("null", CondEq, "index"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = \"not\"", Query{"test_namespace"}.WhereBetweenFields("index", CondEq, "not"), PARSE},
		{"SELECT * FROM test_namespace WHERE \"not\" = index", Query{"test_namespace"}.WhereBetweenFields("not", CondEq, "index"), PARSE},
		{"SELECT * FROM test_namespace WHERE index = \"join\"", Query{"test_namespace"}.WhereBetweenFields("index", CondEq, "join"), PARSE},
		{"SELECT * FROM test_namespace WHERE \"join\" = index", Query{"test_namespace"}.WhereBetweenFields("join", CondEq, "index"), PARSE},
		{"SELECT * FROM test_namespace WHERE NULL = index", Query{"test_namespace"}.Where("index", CondEmpty, VariantArray{}), PARSE},
		{"SELECT * FROM test_namespace WHERE NULL = \"index\"", Query{"test_namespace"}.Where("index", CondEmpty, VariantArray{}), PARSE},
		{"SELECT * FROM test_namespace WHERE true = false",
		 Error{errParseSQL, "Expected field or index name, but found 'false' in query, line: 1 column: 42 47"}},
		{"SELECT * FROM test_namespace WHERE NOT index ALLSET 3489578", Query{"test_namespace"}.Not().Where("index", CondAllSet, 3489578)},
		{"SELECT * FROM test_namespace WHERE NOT index ALLSET (0, 1)", Query{"test_namespace"}.Not().Where("index", CondAllSet, {0, 1})},
		{"SELECT ID, Year, Genre FROM test_namespace WHERE year > '2016' ORDER BY 'year' DESC LIMIT 10000000",
		 Query{"test_namespace"}.Select("ID", "Year", "Genre").Where("year", CondGt, "2016").Sort("year", SortOrder::Desc).Limit(10000000)},
		{"SELECT ID FROM test_namespace WHERE name LIKE 'something' AND (genre IN ('1', '2', '3') AND year > '2016') OR age IN "
		 "('1', '2', '3', '4') LIMIT 10000000",
		 Query{"test_namespace"}
			 .Select("ID")
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
		 Query{"test_namespace"}
			 .InnerJoin(Query{"join_ns"}, "id", CondEq, "id")
			 .Sort("year + join_ns.year * (5 - rand())", SortOrder::Asc)},
		{"SELECT * FROM ns WHERE INNER JOIN (SELECT * FROM ns2 WHERE (SELECT * FROM ns2 WHERE id = 10 AND id <= 10 LIMIT 0) IS NOT NULL) "
		 "ON ns.id = ns2.id",
		 Query{"ns"}.InnerJoin(Query{"ns2"}.Where(Query{"ns2"}.Select("id").Where("id", CondEq, 10), CondLe, {10}), "id", CondEq, "id")},
		{"SELECT * FROM "s + geomNs + " WHERE ST_DWithin(" + kFieldNamePointNonIndex + ", ST_GeomFromText('POINT(1.25 -7.25)'), 0.5)",
		 Query{geomNs}.DWithin(kFieldNamePointNonIndex, reindexer::Point{1.25, -7.25}, 0.5)},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, 10, 20, 30)",
		 Query{"test_namespace"}.Sort("index", SortOrder::Asc, {10, 20, 30})},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, 'str1', 'str2', 'str3') DESC",
		 Query{"test_namespace"}.Sort("index", SortOrder::Desc, {"str1", "str2", "str3"})},
		{"SELECT * FROM test_namespace ORDER BY FIELD(index, {10, 'str1'}, {20, 'str2'}, {30, 'str3'})",
		 Query{"test_namespace"}.Sort("index", SortOrder::Asc,
									  std::vector<std::tuple<int, std::string>>{{10, "str1"}, {20, "str2"}, {30, "str3"}})},
		{"SELECT * FROM test_namespace WHERE index IS NULL", Query{"test_namespace"}.Where("index", CondEmpty, VariantArray{})},
		{"SELECT * FROM test_namespace WHERE index IS EMPTY", Query{"test_namespace"}.Where("index", CondEmpty, VariantArray{}), PARSE},
		{"SELECT * FROM test_namespace WHERE index = NULL", Query{"test_namespace"}.Where("index", CondEmpty, VariantArray{}), PARSE},
		{"SELECT * FROM test_namespace WHERE index < NULL",
		 Error{errParams, "Conditions CondGe|CondGt|CondLt|CondLe can't have null argument"}},
		{"SELECT * FROM test_namespace WHERE index > NULL",
		 Error{errParams, "Conditions CondGe|CondGt|CondLt|CondLe can't have null argument"}},
		{"SELECT * FROM test_namespace WHERE index <= NULL",
		 Error{errParams, "Conditions CondGe|CondGt|CondLt|CondLe can't have null argument"}},
		{"SELECT * FROM test_namespace WHERE index >= NULL",
		 Error{errParams, "Conditions CondGe|CondGt|CondLt|CondLe can't have null argument"}},
		{"SELECT * FROM test_namespace WHERE index < NOT NULL",
		 Error{errParseSQL, "Expected parameter, but found 'not' in query, line: 1 column: 43 46"}},
		{"SELECT * FROM main_ns WHERE (SELECT * FROM second_ns WHERE id < 10 LIMIT 0) IS NOT NULL",
		 Query{"main_ns"}.Where(Query{"second_ns"}.Where("id", CondLt, 10), CondAny, VariantArray{})},
		{"SELECT * FROM main_ns WHERE id = (SELECT id FROM second_ns WHERE id < 10)",
		 Query{"main_ns"}.Where("id", CondEq, Query{"second_ns"}.Select("id").Where("id", CondLt, 10))},
		{"SELECT * FROM main_ns WHERE (SELECT max(id) FROM second_ns WHERE id < 10) > 18",
		 Query{"main_ns"}.Where(Query{"second_ns"}.Aggregate(AggMax, {"id"}).Where("id", CondLt, 10), CondGt, {18})},
		{"SELECT * FROM main_ns WHERE id > (SELECT avg(id) FROM second_ns WHERE id < 10)",
		 Query{"main_ns"}.Where("id", CondGt, Query{"second_ns"}.Aggregate(AggAvg, {"id"}).Where("id", CondLt, 10))},
		{"SELECT * FROM main_ns WHERE id > (SELECT COUNT(*) FROM second_ns WHERE id < 10 LIMIT 0)",
		 Query{"main_ns"}.Where("id", CondGt, Query{"second_ns"}.Where("id", CondLt, 10).ReqTotal())},
		{"SELECT * FROM main_ns WHERE (SELECT * FROM second_ns WHERE id < 10 LIMIT 0) IS NOT NULL AND value IN (5, 4, 1)",
		 Query{"main_ns"}
			 .Where(Query{"second_ns"}.Where("id", CondLt, 10), CondAny, VariantArray{})
			 .Where("value", CondSet, {Variant{5}, Variant{4}, Variant{1}})},
		{"SELECT * FROM main_ns WHERE ((SELECT * FROM second_ns WHERE id < 10 LIMIT 0) IS NOT NULL) AND value IN (5, 4, 1)",
		 Query{"main_ns"}
			 .OpenBracket()
			 .Where(Query{"second_ns"}.Where("id", CondLt, 10), CondAny, VariantArray{})
			 .CloseBracket()
			 .Where("value", CondSet, {Variant{5}, Variant{4}, Variant{1}})},
		{"SELECT * FROM main_ns WHERE id IN (SELECT id FROM second_ns WHERE id < 999) AND value >= 1000",
		 Query{"main_ns"}.Where("id", CondSet, Query{"second_ns"}.Select("id").Where("id", CondLt, 999)).Where("value", CondGe, 1000)},
		{"SELECT * FROM main_ns WHERE (id IN (SELECT id FROM second_ns WHERE id < 999)) AND value >= 1000",
		 Query{"main_ns"}
			 .OpenBracket()
			 .Where("id", CondSet, Query{"second_ns"}.Select("id").Where("id", CondLt, 999))
			 .CloseBracket()
			 .Where("value", CondGe, 1000)},
		{"SELECT * FROM main_ns "
		 "WHERE (SELECT id FROM second_ns WHERE id < 999 AND xxx IS NULL ORDER BY 'value' DESC LIMIT 10) = 0 "
		 "ORDER BY 'tree'",
		 Query{"main_ns"}
			 .Where(Query{"second_ns"}
						.Select("id")
						.Where("id", CondLt, 999)
						.Where("xxx", CondEmpty, VariantArray{})
						.Limit(10)
						.Sort("value", SortOrder::Desc),
					CondEq, 0)
			 .Sort("tree", SortOrder::Asc)},
		{"SELECT * FROM main_ns "
		 "WHERE ((SELECT id FROM second_ns WHERE id < 999 AND xxx IS NULL ORDER BY 'value' DESC LIMIT 10) = 0) "
		 "ORDER BY 'tree'",
		 Query{"main_ns"}
			 .OpenBracket()
			 .Where(Query{"second_ns"}
						.Select("id")
						.Where("id", CondLt, 999)
						.Where("xxx", CondEmpty, VariantArray{})
						.Limit(10)
						.Sort("value", SortOrder::Desc),
					CondEq, 0)
			 .CloseBracket()
			 .Sort("tree", SortOrder::Asc)},
		{"SELECT * FROM main_ns "
		 "WHERE INNER JOIN (SELECT * FROM second_ns WHERE NOT val = 10) ON main_ns.id = second_ns.uid "
		 "AND id IN (SELECT id FROM third_ns WHERE id < 999) "
		 "AND INNER JOIN (SELECT * FROM fourth_ns WHERE val IS NOT NULL OFFSET 2 LIMIT 1) ON main_ns.uid = fourth_ns.id",
		 Query{"main_ns"}
			 .InnerJoin(Query("second_ns").Not().Where("val", CondEq, 10), "id", CondEq, "uid")
			 .Where("id", CondSet, Query{"third_ns"}.Select("id").Where("id", CondLt, 999))
			 .InnerJoin(Query("fourth_ns").Where("val", CondAny, VariantArray{}).Limit(1).Offset(2), "uid", CondEq, "id")},
		{"SELECT * FROM main_ns "
		 "WHERE INNER JOIN (SELECT * FROM second_ns WHERE NOT val = 10 OFFSET 2 LIMIT 1) ON main_ns.id = second_ns.uid "
		 "AND id IN (SELECT id FROM third_ns WHERE id < 999) "
		 "LEFT JOIN (SELECT * FROM fourth_ns WHERE val IS NOT NULL) ON main_ns.uid = fourth_ns.id",
		 Query{"main_ns"}
			 .InnerJoin(Query("second_ns").Not().Where("val", CondEq, 10).Limit(1).Offset(2), "id", CondEq, "uid")
			 .Where("id", CondSet, Query{"third_ns"}.Select("id").Where("id", CondLt, 999))
			 .LeftJoin(Query("fourth_ns").Where("val", CondAny, VariantArray{}), "uid", CondEq, "id")},
		{"SELECT * FROM main_ns "
		 "WHERE id IN (SELECT id FROM third_ns WHERE id < 999 OFFSET 7 LIMIT 5) "
		 "LEFT JOIN (SELECT * FROM second_ns WHERE NOT val = 10 OFFSET 2 LIMIT 1) ON main_ns.id = second_ns.uid "
		 "LEFT JOIN (SELECT * FROM fourth_ns WHERE val IS NOT NULL) ON main_ns.uid = fourth_ns.id",
		 Query{"main_ns"}
			 .LeftJoin(Query("second_ns").Not().Where("val", CondEq, 10).Limit(1).Offset(2), "id", CondEq, "uid")
			 .Where("id", CondSet, Query{"third_ns"}.Select("id").Where("id", CondLt, 999).Limit(5).Offset(7))
			 .LeftJoin(Query("fourth_ns").Where("val", CondAny, VariantArray{}), "uid", CondEq, "id")},
		{"SELECT * FROM ns WHERE ft = 'text' ORDER BY 'rank(ft, 10.0)'",
		 Query{"ns"}.Where("ft", CondEq, "text").Sort("rank(ft, 10.0)", SortOrder::Asc)},
		{"select ssdfs", Error{errParseSQL, "Expected 'FROM', but found '' in query, line: 1 column: 12 12"}},
		{"SELECT * FROM ns WHERE flat_array_len(arr1) > 2", Query{"ns"}.Where(reindexer::functions::FlatArrayLen("arr1"), CondGt, 2)},
		{"SELECT * FROM ns WHERE flat_array_len(arr2) = 12", Query{"ns"}.Where(reindexer::functions::FlatArrayLen("arr2"), CondEq, 12)},
		{"SELECT * FROM ns WHERE flat_array_len(arr3) <= 55", Query{"ns"}.Where(reindexer::functions::FlatArrayLen("arr3"), CondLe, 55)},
		{"SELECT * FROM ns WHERE f1 > now(sec)", Query{"ns"}.Where("f1", CondGt, reindexer::functions::Now())},
		{"SELECT * FROM ns WHERE f2 = now(msec)", Query{"ns"}.Where("f2", CondEq, reindexer::functions::Now("msec"))},
		{"SELECT * FROM ns WHERE f3 <= now(usec)", Query{"ns"}.Where("f3", CondLe, reindexer::functions::Now(reindexer::TimeUnit::usec))},
		{"SELECT * FROM ns WHERE INNER JOIN (SELECT * FROM ns WHERE field1 > 10) ON ns.id IN ns.id OR INNER JOIN (SELECT * FROM ns WHERE "
		 "field2 < 17) ON ns.id IN ns.id LEFT JOIN (SELECT * FROM ns WHERE field3 = 'media') ON ns.id = ns.id",
		 Query{"ns"}
			 .InnerJoin(Query{"ns"}.Where("field1", CondGt, 10), "id", CondSet, "id")
			 .Or()
			 .InnerJoin(Query{"ns"}.Where("field2", CondLt, 17), "id", CondSet, "id")
			 .LeftJoin(Query{"ns"}.Where("field3", CondEq, "media"), "id", CondEq, "id")}};

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
				std::ignore = Query::FromSQL(sql);
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
		 "left_expression": {{
			"type": "field",
			"value": "ft"
		 }},
		 "right_expression": {{
			"type": "values",
			"value": "text"
		 }}
	  }}
   ],
   "merge_queries": [],
   "aggregations": []
}})",
				   geomNs),
			   Query{geomNs}.Where("ft", CondEq, "text").Sort("rank(ft, 2.0) + 5", SortOrder::Asc)},
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
		 "left_expression": {{
			"type": "field",
			"value": "{}"
		 }},
		 "right_expression": {{
			"type": "values",
			"value": [
			   [
				  -9.2,
				  -0.145
			   ],
			   0.581
			]
		 }}
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
		 "left_expression": {{
			"type": "field",
			"value": "{}"
		 }},
		 "right_expression": {{
			"type": "field",
			"value": "{}"
		 }}
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
				  "left_expression": {{
					 "type": "field",
					 "value": "{}"
				  }},
				  "right_expression": {{
					 "type": "values",
					 "value": 1
				  }}
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
		 "left_expression": {{
			"type": "field",
			"value": "{}"
		 }},
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
				  "left_expression": {{
					 "type": "field",
					 "value": "{}"
				  }},
				  "right_expression": {{
					 "type": "values",
					 "value": [
						1,
						10,
						100
					 ]
				  }}
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
				   .Where(kFieldNameName, CondEq, Query(joinNs).Select(kFieldNameName).Where(kFieldNameId, CondSet, {1, 10, 100}))},
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
		 "left_expression": {{
			"type": "field",
			"value": "{}"
		 }},
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
		 "left_expression": {{
			"type": "field",
			"value": "{}"
		 }},
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
	auto jsonPrettyPrint = [](const std::string_view& json) {
		reindexer::WrSerializer ser;
		reindexer::prettyPrintJSON(std::string_view(json), ser, 3);
		return std::string{ser.Slice()};
	};
	for (const auto& [dsl, expected, direction] : cases) {
		if (std::holds_alternative<Query>(expected)) {
			const Query& q = (std::get<Query>(expected));
			if (direction & GEN) {
				EXPECT_EQ(jsonPrettyPrint(q.GetJSON()), jsonPrettyPrint(dsl));
			}
			if (direction & PARSE) {
				try {
					auto parsed = Query::FromJSON(dsl);
					EXPECT_EQ(parsed, q) << dsl;
				} catch (const Error& err) {
					ADD_FAILURE() << "Unexpected error: " << err.what() << "\nDSL: " << dsl;
					continue;
				}
			}
		} else {
			const Error& expectedErr = std::get<Error>(expected);
			try {
				auto parsed = Query::FromJSON(dsl);
				ADD_FAILURE() << "Expected error: " << expectedErr.what() << "\nDSL: " << dsl;
			} catch (const Error& err) {
				EXPECT_STREQ(err.what(), expectedErr.what()) << "\nDSL: " << dsl;
			}
		}
	}
}

TEST_F(QueriesApi, FunctionDslTest) {
	const auto getFunction = [](const auto& function) -> const reindexer::functions::Function& {
		return std::visit([](const auto& f) -> const reindexer::functions::Function& { return f; }, function);
	};

	auto checkFunctionDsl = [&](const std::string& dsl, const reindexer::h_vector<std::string, 1>& fields, FunctionType type,
								std::string_view name, const VariantArray& args) {
		gason::JsonParser jsonParser;
		auto json = jsonParser.Parse(dsl);

		auto function = reindexer::functions::Function::FromJSON(json["function"]);
		EXPECT_EQ(getFunction(function).FieldNames(), fields);
		EXPECT_EQ(getFunction(function).Type(), type);
		EXPECT_EQ(getFunction(function).Name(), name);
		EXPECT_EQ(getFunction(function).Arguments(), args);

		reindexer::WrSerializer ser;
		reindexer::builders::JsonBuilder jsonBuilder{ser};
		getFunction(function).GetJSON(jsonBuilder);
		jsonBuilder.End();
		EXPECT_EQ(dsl, ser.Slice());
	};

	checkFunctionDsl(R"({"function":{"name":"flat_array_len","fields":["prices"]}})", {"prices"}, FunctionFlatArrayLen, "flat_array_len",
					 {});

	checkFunctionDsl(R"({"function":{"name":"now","arguments":["sec"]}})", {}, FunctionNow, "now", {Variant("sec")});
}

TEST_F(QueriesApi, ArithmeticFilterTest) {
	FillDefaultNamespace(0, 20, 0);
	UpsertArithmeticSampleItem();

	ExecuteAndVerify(
		Query(default_namespace).Where(reindexer::expressions::ArithmeticExpression("1+2*3"), CondEq, VariantArray{Variant{7}}));
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2+1"), CondEq, VariantArray{Variant{21}}));
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "+" + kFieldNameYear + "+" + kFieldNameAge),
				   CondEq, VariantArray{Variant{2030}}));
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(kFieldNameAge, CondEq, reindexer::expressions::ArithmeticExpression(std::string(kFieldNameYear) + "-2000")));
	ExecuteAndVerify(Query(default_namespace)
						 .Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq,
								reindexer::expressions::ArithmeticExpression("(" + std::string(kFieldNameYear) + "-2000)*2")));
	ExecuteAndVerify(Query(default_namespace)
						 .Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq, kFieldNameYear));
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondLt, VariantArray{Variant{100}}));
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameYear) + "-2000"), CondGt, VariantArray{Variant{0}}));
}

TEST_F(QueriesApi, ArithmeticTrivialExpressionsFastPathTest) {
	using reindexer::expressions::ArithmeticExpression;

	FillDefaultNamespace(0, 20, 0);
	UpsertArithmeticSampleItem();

	const auto expectIndex = [this](Query&& query) {
		QueryResults qr;
		ExecuteAndVerify(query, qr);
		EXPECT_NE(qr.GetExplainResults().find(",\"field\":\"year\","), std::string::npos) << qr.GetExplainResults();
		EXPECT_NE(qr.GetExplainResults().find(",\"method\":\"index\","), std::string::npos) << qr.GetExplainResults();
	};
	expectIndex(Query(default_namespace).Where(kFieldNameYear, CondGt, ArithmeticExpression("2000")));
	expectIndex(Query(default_namespace).Where(ArithmeticExpression("2000"), CondLt, ArithmeticExpression(kFieldNameYear)));
	expectIndex(Query(default_namespace).Where(ArithmeticExpression("2000"), CondLt, std::string{kFieldNameYear}));
	expectIndex(Query(default_namespace).Where(ArithmeticExpression(kFieldNameYear), CondGt, VariantArray{Variant{2000}}));

	const auto expectIndexNot = [this](Query&& query) {
		QueryResults qr;
		ExecuteAndVerify(query, qr);
		EXPECT_NE(qr.GetExplainResults().find("\"field\":\"not year\""), std::string::npos) << qr.GetExplainResults();
		EXPECT_NE(qr.GetExplainResults().find(",\"method\":\"index\","), std::string::npos) << qr.GetExplainResults();
	};
	expectIndexNot(Query(default_namespace).Not().Where(ArithmeticExpression(kFieldNameYear), CondGt, VariantArray{Variant{2000}}));
	expectIndexNot(Query(default_namespace)
					   .Not()
					   .OpenBracket()
					   .Where(ArithmeticExpression(kFieldNameYear), CondGt, VariantArray{Variant{2000}})
					   .CloseBracket());

	const auto expectTwoFieldsComparison = [this](Query&& query) {
		QueryResults qr;
		ExecuteAndVerify(query, qr);
		EXPECT_EQ(qr.GetExplainResults().find("\"type\":\"ArithmeticComparator\""), std::string::npos) << qr.GetExplainResults();
	};
	expectTwoFieldsComparison(Query(default_namespace).Where(ArithmeticExpression(kFieldNameAge), CondLt, std::string{kFieldNameYear}));
	expectTwoFieldsComparison(Query(default_namespace).Where(kFieldNameAge, CondLt, ArithmeticExpression(kFieldNameYear)));
	expectTwoFieldsComparison(
		Query(default_namespace).Where(ArithmeticExpression(kFieldNameAge), CondLt, ArithmeticExpression(kFieldNameYear)));
	{
		QueryResults qr;
		auto query = Query(default_namespace)
						 .Not()
						 .OpenBracket()
						 .Not()
						 .Where(ArithmeticExpression(kFieldNameYear), CondGt, VariantArray{Variant{2000}})
						 .CloseBracket();
		ExecuteAndVerify(query, qr);
		EXPECT_EQ(qr.GetExplainResults().find("\"type\":\"ArithmeticComparator\""), std::string::npos) << qr.GetExplainResults();
		EXPECT_NE(qr.GetExplainResults().find(",\"method\":\"index\","), std::string::npos) << qr.GetExplainResults();
	}
}

TEST_F(QueriesApi, ArithmeticCachedTotalTest) {
	FillDefaultNamespace(0, 20, 0);

	auto query =
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondGe, VariantArray{Variant{10}});
	ExecuteAndVerify(Query(query));
	const auto expectedTotal = rt.Select(query).Count();
	ASSERT_GT(expectedTotal, 0);

	for (int i = 0; i < 2; ++i) {
		QueryResults qr;
		auto cachedQuery = query;
		cachedQuery.CachedTotal().Limit(1);
		const auto err = rt.reindexer->Select(cachedQuery, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.TotalCount(), expectedTotal);
	}

	{
		auto totalQuery = query;
		totalQuery.ReqTotal().Limit(1);
		QueryResults qr;
		const auto err = rt.reindexer->Select(totalQuery, qr);
		ASSERT_TRUE(err.ok()) << err.what();
		EXPECT_EQ(qr.TotalCount(), expectedTotal);
	}
}

TEST_F(QueriesApi, ArithmeticConditionsTest) {
	FillDefaultNamespace(0, 20, 0);
	UpsertArithmeticSampleItem();

	ExecuteAndVerify(Query(default_namespace)
						 .Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondRange,
								VariantArray::Create(20, 20)));
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondSet, VariantArray::Create(19, 20)));
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondAllSet, VariantArray::Create(20)));
}

TEST_F(QueriesApi, ArithmeticNowIsConsistentTest) {
	FillDefaultNamespace(0, 5, 0);

	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression("now(msec)-now(sec)*1000"), CondRange, VariantArray::Create(0, 999)));
}

TEST_F(QueriesApi, ArithmeticNowMixedWithFunctionIsConsistentTest) {
	FillDefaultNamespace(0, 5, 0);

	ExecuteAndVerify(
		Query(default_namespace)
			.Where(kFieldNameStartTime, CondLt, reindexer::functions::Now(reindexer::TimeUnit::nsec))
			.Where(reindexer::expressions::ArithmeticExpression("now(msec)-now(sec)*1000"), CondRange, VariantArray::Create(0, 999)));
}

TEST_F(QueriesApi, ArithmeticBooleanTreeTest) {
	FillDefaultNamespace(0, 20, 0);
	UpsertArithmeticSampleItem();

	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq, VariantArray{Variant{20}})
			.Where(kFieldNameYear, CondEq, 2010)
			.Or()
			.Where(reindexer::expressions::ArithmeticExpression("1+1"), CondEq, VariantArray{Variant{3}}));
	ExecuteAndVerify(
		Query(default_namespace)
			.Not()
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq, VariantArray{Variant{20}}));
	ExecuteAndVerify(
		Query(default_namespace)
			.Not()
			.OpenBracket()
			.Not()
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq, VariantArray{Variant{20}})
			.CloseBracket());
	ExecuteAndVerify(
		Query(default_namespace)
			.OpenBracket()
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondGe, VariantArray{Variant{0}})
			.Where(kFieldNameYear, CondEq, 2010)
			.CloseBracket()
			.Or()
			.Where(reindexer::expressions::ArithmeticExpression("1+1"), CondEq, VariantArray{Variant{2}}));
}

TEST_F(QueriesApi, ArithmeticEmptyAndNullFieldsTest) {
	using reindexer::expressions::ArithmeticExpression;
	const std::string kNs = "arith_empty_null_ns";
	constexpr const char* kSparse = "sparse_age";
	constexpr const char* kVal = "val";

	rt.OpenNamespace(kNs);
	DefineNamespaceDataset(kNs, {IndexDeclaration{kFieldNameId, "hash", "int", IndexOpts().PK(), 0},
								 IndexDeclaration{kFieldNameAge, "hash", "int", IndexOpts(), 0},
								 IndexDeclaration{kSparse, "hash", "int", IndexOpts().Sparse(), 0}});
	setPkFields(kNs, {kFieldNameId});
	addIndexFields(kNs, kFieldNameId, {{kFieldNameId, reindexer::KeyValueType::Int{}}});
	addIndexFields(kNs, kFieldNameAge, {{kFieldNameAge, reindexer::KeyValueType::Int{}}});
	addIndexFields(kNs, kSparse, {{kSparse, reindexer::KeyValueType::Int{}}});

	const auto upsertJson = [&](std::string_view json) {
		Item item = NewItem(kNs);
		const auto err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what() << ' ' << json;
		Upsert(kNs, item);
		saveItem(std::move(item), kNs);
	};
	upsertJson(R"json({"id":1,"age":10})json");
	upsertJson(R"json({"id":2,"age":10,"sparse_age":7})json");
	upsertJson(R"json({"id":3,"age":10,"val":null})json");
	upsertJson(R"json({"id":4,"age":10,"val":5})json");
	upsertJson(R"json({"id":5,"age":10,"sparse_age":7,"val":5})json");

	const auto executeAndExpectCount = [this](Query&& query, size_t expectedCount) {
		QueryResults qr;
		ExecuteAndVerify(query, qr);
		EXPECT_EQ(qr.Count(), expectedCount) << query.GetSQL() << '\n' << qr.GetExplainResults();
	};

	ExecuteAndVerify(Query(kNs).Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{8}}));
	ExecuteAndVerify(Query(kNs).Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondRange, VariantArray::Create(0, 100)));
	ExecuteAndVerify(Query(kNs).Where(kFieldNameAge, CondAllSet, ArithmeticExpression(std::string(kSparse) + "+1")));
	{
		QueryResults qr;
		auto query = Query(kNs).Where(kFieldNameAge, CondAllSet, ArithmeticExpression(kSparse));
		ExecuteAndVerify(query, qr);
		EXPECT_EQ(qr.Count(), 0);
		EXPECT_NE(qr.GetExplainResults().find("\"type\":\"ArithmeticComparator\""), std::string::npos) << qr.GetExplainResults();
	}
	ExecuteAndVerify(Query(kNs).Not().Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondGt, VariantArray{Variant{0}}));
	ExecuteAndVerify(Query(kNs).Not().Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondGt, VariantArray{Variant{100}}));
	{
		QueryResults qr;
		auto query = Query(kNs).Not().Where(ArithmeticExpression(kSparse), CondEq, VariantArray{Variant{7}});
		ExecuteAndVerify(query, qr);
		EXPECT_EQ(qr.Count(), 3);
	}
	{
		QueryResults qr;
		auto query = Query(kNs).Not().OpenBracket().Where(ArithmeticExpression(kSparse), CondEq, VariantArray{Variant{7}}).CloseBracket();
		ExecuteAndVerify(query, qr);
		EXPECT_EQ(qr.Count(), 3);
	}
	{
		QueryResults qr;
		auto query =
			Query(kNs).Not().OpenBracket().Not().Where(ArithmeticExpression(kSparse), CondEq, VariantArray{Variant{7}}).CloseBracket();
		ExecuteAndVerify(query, qr);
		EXPECT_EQ(qr.Count(), 2);
		EXPECT_EQ(qr.GetExplainResults().find("\"type\":\"ArithmeticComparator\""), std::string::npos) << qr.GetExplainResults();
	}
	ExecuteAndVerify(Query(kNs)
						 .Not()
						 .OpenBracket()
						 .Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{8}})
						 .CloseBracket());
	ExecuteAndVerify(Query(kNs)
						 .Not()
						 .OpenBracket()
						 .Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{100}})
						 .CloseBracket());
	ExecuteAndVerify(Query(kNs)
						 .Not()
						 .OpenBracket()
						 .Not()
						 .Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{8}})
						 .CloseBracket());
	ExecuteAndVerify(Query(kNs)
						 .Not()
						 .OpenBracket()
						 .Not()
						 .Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{100}})
						 .CloseBracket());
	ExecuteAndVerify(Query(kNs).Where(ArithmeticExpression(std::string(kVal) + "+1"), CondEq, VariantArray{Variant{6}}));
	ExecuteAndVerify(Query(kNs).Not().Where(ArithmeticExpression(std::string(kVal) + "+1"), CondEq, VariantArray{Variant{6}}));
	ExecuteAndVerify(Query(kNs).Not().Where(ArithmeticExpression(std::string(kVal) + "+1"), CondEq, VariantArray{Variant{0}}));
	ExecuteAndVerify(Query(kNs).Not().Where(kVal, CondLt, ArithmeticExpression("now()")));
	ExecuteAndVerify(Query(kNs).Not().Where(ArithmeticExpression("now()"), CondLt, kVal));

	executeAndExpectCount(Query(kNs)
							  .Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{100}})
							  .Or()
							  .Where(kFieldNameAge, CondEq, Variant{10}),
						  5);
	executeAndExpectCount(Query(kNs)
							  .Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{100}})
							  .Or()
							  .Where(kFieldNameId, CondEq, Variant{999}),
						  0);
	executeAndExpectCount(Query(kNs)
							  .Not()
							  .OpenBracket()
							  .Where(ArithmeticExpression(std::string(kSparse) + "+1"), CondEq, VariantArray{Variant{100}})
							  .Or()
							  .Where(kFieldNameId, CondEq, Variant{999})
							  .CloseBracket(),
						  5);
}

TEST_F(QueriesApi, ArithmeticJoinTest) {
	FillDefaultNamespace(0, 40, 0);
	FillTestJoinNamespace(0, 40);
	ExecuteAndVerify(
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondGe, VariantArray{Variant{0}})
			.InnerJoin(Query(joinNs).Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "+1"), CondGe,
										   VariantArray{Variant{0}}),
					   kFieldNameId, CondEq, kFieldNameId));
}

TEST_F(QueriesApi, ArithmeticUpdateAndDeleteTest) {
	FillDefaultNamespace(0, 20, 0);
	UpsertArithmeticSampleItem();

	const Query filter =
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq, VariantArray{Variant{20}});
	ExecuteAndVerify(Query(filter));
	const auto matching = rt.Select(filter).Count();
	ASSERT_GE(matching, 1);

	auto updateQuery = filter;
	EXPECT_EQ(rt.Update(updateQuery.Set(kFieldNameGenre, 7)), matching);
	EXPECT_EQ(rt.Delete(Query(filter)), matching);
	EXPECT_EQ(rt.Select(filter).Count(), 0);
}

TEST_F(QueriesApi, ArithmeticNowSharedAcrossComparatorsTest) {
	FillDefaultNamespace(0, 5, 0);
	ExecuteAndVerify(Query(default_namespace)
						 .Where(reindexer::expressions::ArithmeticExpression("now(nsec)-now(nsec)"), CondEq, VariantArray{Variant{0}})
						 .Where(reindexer::expressions::ArithmeticExpression("now(nsec)"), CondEq,
								reindexer::expressions::ArithmeticExpression("now(nsec)")));
}

TEST_F(QueriesApi, ArithmeticNowSharesFunctionSnapshotTest) {
	Query query = Query(default_namespace)
					  .Where(kFieldNameStartTime, CondGt, reindexer::functions::Now(reindexer::TimeUnit::nsec))
					  .Where(reindexer::expressions::ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{0}});

	reindexer::functions::PrecomputedValues precomputedValues;
	std::optional<Query> queryCopy;
	reindexer::OptimizeFunctionEntries(query, queryCopy, precomputedValues);

	ASSERT_TRUE(queryCopy.has_value());
	// NOLINTBEGIN(bugprone-unchecked-optional-access) queryCopy / now snapshots checked with ASSERT_TRUE(...has_value()) above
	const auto queryCopyImpl = Impl(*queryCopy);
	ASSERT_TRUE(queryCopyImpl.ExecutionNowNsec().has_value());
	ASSERT_TRUE(precomputedValues.GetNowNsec().has_value());
	EXPECT_EQ(*queryCopyImpl.ExecutionNowNsec(), *precomputedValues.GetNowNsec());
	ASSERT_TRUE(queryCopyImpl.Entries().Is<reindexer::QueryEntry>(0));
	const auto& optimizedNow = queryCopyImpl.Entries().Get<reindexer::QueryEntry>(0).Values();
	ASSERT_EQ(optimizedNow.size(), 1);
	EXPECT_EQ(optimizedNow[0].As<int64_t>(), *queryCopyImpl.ExecutionNowNsec());
	// NOLINTEND(bugprone-unchecked-optional-access)
}

TEST_F(QueriesApi, ArithmeticNowSnapshotIsSharedAcrossNestedQueries) {
	using reindexer::expressions::ArithmeticExpression;

	Query level3{"level3"};
	level3.Where(ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{0}});
	Query level2{"level2"};
	level2.InnerJoin(std::move(level3), "id", CondEq, "id");

	Query mergedChild{"merged_child"};
	mergedChild.Where(ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{0}});
	Query merged{"merged"};
	merged.InnerJoin(std::move(mergedChild), "id", CondEq, "id");

	Query sub{"sub"};
	sub.Where(ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{0}});

	Query level1{"level1"};
	level1.InnerJoin(std::move(level2), "id", CondEq, "id");
	level1.Merge(std::move(merged));
	level1.Where(std::move(sub), CondAny, VariantArray{});

	Query root{"root"};
	root.Where("start_time", CondGt, reindexer::functions::Now(reindexer::TimeUnit::nsec));
	root.InnerJoin(std::move(level1), "id", CondEq, "id");

	reindexer::functions::PrecomputedValues precomputedValues;
	std::optional<Query> queryCopy;
	reindexer::OptimizeFunctionEntries(root, queryCopy, precomputedValues);

	ASSERT_TRUE(queryCopy.has_value());
	const auto snapshot = precomputedValues.GetNowNsec();
	ASSERT_TRUE(snapshot.has_value());
	// NOLINTBEGIN(bugprone-unchecked-optional-access) queryCopy / now snapshots checked with ASSERT_TRUE(...has_value()) above
	const auto rootImpl = Impl(*queryCopy);
	ASSERT_TRUE(rootImpl.Entries().Is<reindexer::QueryEntry>(0));
	const auto& optimizedNow = rootImpl.Entries().Get<reindexer::QueryEntry>(0).Values();
	ASSERT_EQ(optimizedNow.size(), 1);
	EXPECT_EQ(optimizedNow[0].As<int64_t>(), *snapshot);
	EXPECT_FALSE(rootImpl.ExecutionNowNsec().has_value());

	ASSERT_EQ(rootImpl.JoinQueries().size(), 1);
	const auto& level1Join = rootImpl.JoinQueries()[0];
	EXPECT_EQ(JoinedImpl(level1Join).JoinEntries().size(), 1);
	EXPECT_FALSE(Impl(level1Join).ExecutionNowNsec().has_value());

	const auto level1Impl = Impl(level1Join);
	ASSERT_EQ(level1Impl.JoinQueries().size(), 1);
	const auto& level2Join = level1Impl.JoinQueries()[0];
	EXPECT_FALSE(Impl(level2Join).ExecutionNowNsec().has_value());
	ASSERT_EQ(Impl(level2Join).JoinQueries().size(), 1);
	EXPECT_EQ(Impl(Impl(level2Join).JoinQueries()[0]).ExecutionNowNsec(), snapshot);

	ASSERT_EQ(level1Impl.MergeQueries().size(), 1);
	const auto& mergedQuery = level1Impl.MergeQueries()[0];
	EXPECT_FALSE(Impl(mergedQuery).ExecutionNowNsec().has_value());
	ASSERT_EQ(Impl(mergedQuery).JoinQueries().size(), 1);
	EXPECT_EQ(Impl(Impl(mergedQuery).JoinQueries()[0]).ExecutionNowNsec(), snapshot);

	ASSERT_EQ(Impl(level1Join).SubQueries().size(), 1);
	EXPECT_EQ(Impl(Impl(level1Join).SubQueries()[0]).ExecutionNowNsec(), snapshot);
	// NOLINTEND(bugprone-unchecked-optional-access)

	Query onlyDeep{"only_deep_3"};
	onlyDeep.Where(ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{0}});
	Query onlyMid{"only_deep_2"};
	onlyMid.InnerJoin(std::move(onlyDeep), "id", CondEq, "id");
	Query onlyRoot{"only_deep_1"};
	onlyRoot.InnerJoin(std::move(onlyMid), "id", CondEq, "id");

	reindexer::functions::PrecomputedValues deepValues;
	std::optional<Query> deepCopy;
	reindexer::OptimizeFunctionEntries(onlyRoot, deepCopy, deepValues);
	ASSERT_TRUE(deepCopy.has_value());
	const auto deepSnapshot = deepValues.GetNowNsec();
	ASSERT_TRUE(deepSnapshot.has_value());
	// NOLINTNEXTLINE(bugprone-unchecked-optional-access) deepCopy checked with ASSERT_TRUE(...has_value()) above
	const auto deepRoot = Impl(*deepCopy);
	EXPECT_FALSE(deepRoot.ExecutionNowNsec().has_value());
	ASSERT_EQ(deepRoot.JoinQueries().size(), 1);
	const auto& deepMid = deepRoot.JoinQueries()[0];
	EXPECT_FALSE(Impl(deepMid).ExecutionNowNsec().has_value());
	ASSERT_EQ(Impl(deepMid).JoinQueries().size(), 1);
	EXPECT_EQ(Impl(Impl(deepMid).JoinQueries()[0]).ExecutionNowNsec(), deepSnapshot);
}

// Explain reports the now() snapshot of an arithmetic comparator as "now_nsec".
// The same select must use one value at the root and in a third-level join.
TEST_F(QueriesApi, ArithmeticNowSnapshotMatchesDuringNestedSelect) {
	FillDefaultNamespace(0, 3, 0);
	FillTestJoinNamespace(0, 3);

	Query level3{joinNs};
	level3.Where(reindexer::expressions::ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{int64_t{0}}});

	Query level2{joinNs};
	level2.Explain();
	level2.InnerJoin(std::move(level3), kFieldNameId, CondEq, kFieldNameId);

	Query root{default_namespace};
	root.Explain();
	root.Where(reindexer::expressions::ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{int64_t{0}}});
	root.InnerJoin(std::move(level2), kFieldNameId, CondEq, kFieldNameId);

	const int64_t before = reindexer::getTimeNow(reindexer::TimeUnit::nsec);
	QueryResults qr = rt.Select(root);
	const int64_t after = reindexer::getTimeNow(reindexer::TimeUnit::nsec);
	const std::string explain = qr.GetExplainResults();

	std::vector<int64_t> snapshots;
	constexpr std::string_view kKey = "\"now_nsec\":";
	for (size_t pos = explain.find(kKey); pos != std::string::npos; pos = explain.find(kKey, pos + kKey.size())) {
		snapshots.push_back(std::stoll(explain.substr(pos + kKey.size())));
	}

	ASSERT_GE(snapshots.size(), 2) << explain;
	for (const int64_t snapshot : snapshots) {
		EXPECT_EQ(snapshot, snapshots.front()) << explain;
		EXPECT_GE(snapshot, before) << explain;
		EXPECT_LE(snapshot, after) << explain;
	}
	EXPECT_NE(explain.find("\"field\":\"now(nsec) > 0\""), std::string::npos) << explain;
}

TEST_F(QueriesApi, ArithmeticNowBypassesCachedTotalTest) {
	FillDefaultNamespace(0, 5, 0);
	const Query query = Query(default_namespace)
							.Where(reindexer::expressions::ArithmeticExpression("now(nsec)"), CondGt, VariantArray{Variant{0}})
							.Where(reindexer::expressions::ArithmeticExpression("start_time + 1"), CondGt, VariantArray{Variant{0}})
							.CachedTotal()
							.Limit(1);
	const auto before = getMemStat(*rt.reindexer, default_namespace)["query_cache.items_count"].As<int64_t>();

	for (int i = 0; i < 5; ++i) {
		const auto qr = rt.Select(query);
		ASSERT_EQ(qr.TotalCount(), 5);
	}
	const auto after = getMemStat(*rt.reindexer, default_namespace)["query_cache.items_count"].As<int64_t>();
	EXPECT_EQ(after, before);
}

TEST_F(QueriesApi, ArithmeticDslRoundTripTest) {
	using reindexer::expressions::ArithmeticExpression;
	const auto query = Query(default_namespace)
						   .Where(ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq, VariantArray{Variant{20}})
						   .Where(kFieldNameYear, CondLt, ArithmeticExpression(std::string(kFieldNameAge) + "+2000"))
						   .Where(ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondGe,
								  ArithmeticExpression(std::string(kFieldNameYear) + "-2000"));
	EXPECT_EQ(Query::FromJSON(query.GetJSON()), query);
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
#if RX_WITH_STDLIB_DEBUG
	constexpr size_t kIterations = 20;
#else	// !RX_WITH_STDLIB_DEBUG
	constexpr size_t kIterations = 100;
#endif	// RX_WITH_STDLIB_DEBUG
	for (size_t i = 0; i < kIterations; ++i) {
		const auto forcedSortOrder =
			generateForcedSortOrder(forcedSortOffsetMaxValue * 1.1, rand() % static_cast<int>(forcedSortOffsetNsSize * 1.1));
		const size_t offset = rand() % static_cast<size_t>(forcedSortOffsetNsSize * 1.1);
		const size_t limit = rand() % static_cast<size_t>(forcedSortOffsetNsSize * 1.1);
		const auto sortOrder = (rand() % 2) ? SortOrder::Desc : SortOrder::Asc;
		// Single column sort
		auto expectedResults = ForcedSortOffsetTestExpectedResults(offset, limit, sortOrder, forcedSortOrder, First);
		ExecuteAndVerify(Query(forcedSortNs).Sort(kFieldNameColumnHash, sortOrder, forcedSortOrder).Offset(offset).Limit(limit),
						 kFieldNameColumnHash, expectedResults);
		expectedResults = ForcedSortOffsetTestExpectedResults(offset, limit, sortOrder, forcedSortOrder, Second);
		ExecuteAndVerify(Query(forcedSortNs).Sort(kFieldNameColumnTree, sortOrder, forcedSortOrder).Offset(offset).Limit(limit),
						 kFieldNameColumnTree, expectedResults);
		// Multicolumn sort
		const auto sortOrder2 = (rand() % 2) ? SortOrder::Desc : SortOrder::Asc;
		auto expectedResultsMult = ForcedSortOffsetTestExpectedResults(offset, limit, sortOrder, sortOrder2, forcedSortOrder, First);
		ExecuteAndVerify(Query(forcedSortNs)
							 .Sort(kFieldNameColumnHash, sortOrder, forcedSortOrder)
							 .Sort(kFieldNameColumnTree, sortOrder2)
							 .Offset(offset)
							 .Limit(limit),
						 kFieldNameColumnHash, expectedResultsMult.first, kFieldNameColumnTree, expectedResultsMult.second);
		expectedResultsMult = ForcedSortOffsetTestExpectedResults(offset, limit, sortOrder, sortOrder2, forcedSortOrder, Second);
		ExecuteAndVerify(Query(forcedSortNs)
							 .Sort(kFieldNameColumnTree, sortOrder2, forcedSortOrder)
							 .Sort(kFieldNameColumnHash, sortOrder)
							 .Offset(offset)
							 .Limit(limit),
						 kFieldNameColumnHash, expectedResultsMult.first, kFieldNameColumnTree, expectedResultsMult.second);
	}
}

TEST_F(QueriesApi, ForcedSortByValuesOfWrongTypes) {
	FillForcedSortNamespace();
	reindexer::QueryResults qr;
	auto query =
		Query(forcedSortNs).Sort(kFieldNameColumnString, SortOrder::Desc, std::vector{Variant{VariantArray{Variant{1}, Variant{3}}}});
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
			q.LeftJoin(std::move(qr), iLeft, c.first, iRight);

			{
				std::string sqlQCmp = createQuery(tLeft, tRight, iLeft, iRight, c.first);
				ASSERT_EQ(sqlQCmp, q.GetSQL());
			}

			{
				std::string sqlQ = createQuery(tLeft, tRight, iLeft, iRight, c.first);
				Query qSql = Query::FromSQL(sqlQ);
				ASSERT_EQ(sqlQ, qSql.GetSQL());
			}

			{
				std::string sqlQ = createQuery(tRight, tLeft, iRight, iLeft, c.second);
				auto qSql = Query::FromSQL(sqlQ);
				ASSERT_EQ(q.GetJSON(), qSql.GetJSON());
				reindexer::WrSerializer wrSer;
				Impl(qSql).GetSQL(wrSer);
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
	const auto q = Query(nsName).Where("array", CondAllSet, {0, 1, 2});
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

	const auto q =
		Query(nsName).Where("id", CondSet, {rand() % kMaxID, rand() % kMaxID, rand() % kMaxID, rand() % kMaxID}).Sort("id", SortOrder::Asc);
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

		auto resFields = parseCSVRow(ss.str());
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

	const auto q = Query{nsNames[0]}
					   .Join(LeftJoin, Query(nsNames[1]), OpAnd, "join_field", CondEq, "join_field")
					   .Join(LeftJoin, Query(nsNames[2]), OpAnd, "id", CondEq, "join_field");
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
			auto converted = parserCsv.Parse(std::string_view(csv2json(serCsv.Slice(), csv2jsonSchema)));
			auto orig = parserJson.Parse(serJson.Slice());

			// for check that all tags related to joined nss from json-result are present in csv-result
			std::set<std::string_view> checkJoinedNssTags;
			for (const auto& node : orig) {
				if (std::string_view(node.key).substr(0, 7) == "joined_") {
					checkJoinedNssTags.insert(node.key);
				}
			}

			for (const auto& fieldName : csv2jsonSchema) {
				if (fieldName == "joined_nss_map" && !converted[fieldName].isEmpty()) {
					EXPECT_EQ(converted[fieldName].value.getTag(), gason::JsonTag::OBJECT);
					for (auto& node : converted[fieldName]) {
						EXPECT_TRUE(!orig[node.key].isEmpty()) << "not found joined data: " << node.key;
						auto origStr = reindexer::stringifyJson(orig[node.key]);
						auto convertedStr = reindexer::stringifyJson(node);
						EXPECT_EQ(origStr, convertedStr);
						checkJoinedNssTags.erase(node.key);
					}
					continue;
				}
				if (converted[fieldName].isEmpty() || orig[fieldName].isEmpty()) {
					EXPECT_TRUE(converted[fieldName].isEmpty() && orig[fieldName].isEmpty()) << "fieldName: " << fieldName;
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
		Query q = Query{nsName}.Where("id", CondLt, 5).Sort("2 * "s + f, SortOrder::Asc).Strict(StrictModeNames);
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
		Query q = Query{nsName}.Where("id", CondGt, 5).Sort("2 * "s + f, SortOrder::Asc).Strict(StrictModeNames);
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
		for (auto sortOrder : {SortOrder::Asc, SortOrder::Desc}) {
			for (const char* sortField : {"value", "object.nested_value"}) {
				auto q = qTemplate;
				q.Where("id", cond, values).Sort(sortPrefix + sortField, sortOrder);
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
					int prevId = 10000 * (sortOrder == SortOrder::Desc ? 1 : -1);
					auto prevIt = qr.end();
					for (auto& it : qr) {
						ASSERT_TRUE(it.Status().ok()) << it.Status().what() << print(q, it, prevIt, qr);
						const auto item = it.GetItem();
						ASSERT_TRUE(item.Status().ok()) << item.Status().what() << print(q, it, prevIt, qr);
						const auto currId = item["id"].As<int>();
						if (sortOrder == SortOrder::Desc) {
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

	sortByNsDifferentTypesImpl(nsRight, Query{nsMain}.InnerJoin(Query{nsRight}, "id", CondEq, "id"), nsRight + '.');
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
		Query(default_namespace).Where(kFieldNameUuidArr, CondRange, reindexer_tests_tools::randHeterogeneousUuidArray(2, 2)),
		Query(default_namespace)
			.WhereComposite(kCompositeFieldUuidName, CondRange,
							{VariantArray::Create(reindexer_tests_tools::nilUuid(), RandString()),
							 VariantArray::Create(reindexer_tests_tools::randUuid(), RandString())}),
		Query(default_namespace).Where(Query(default_namespace).Where(kFieldNameId, CondEq, 10), CondAny, VariantArray{}),
		Query(default_namespace).Not().Where(Query(default_namespace), CondEmpty, VariantArray{}),
		Query(default_namespace).Where(kFieldNameId, CondLt, Query(default_namespace).Aggregate(AggAvg, {kFieldNameId})),
		Query(default_namespace)
			.Where(kFieldNameGenre, CondSet, Query(joinNs).Select(kFieldNameGenre).Where(kFieldNameId, CondSet, {10, 20, 30, 40})),

		Query(default_namespace).Where(Query(joinNs).Select(kFieldNameGenre).Where(kFieldNameId, CondGt, 10), CondSet, {10, 20, 30, 40}),
		Query(default_namespace)
			.Where(Query(joinNs).Select(kFieldNameGenre).Where(kFieldNameId, CondGt, 10).Offset(1), CondSet, {10, 20, 30, 40}),
		Query(default_namespace)
			.Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).Aggregate(AggMax, {kFieldNameGenre}), CondRange, {48, 50}),
		Query(default_namespace).Where(Query(joinNs).Where(kFieldNameId, CondGt, 10).ReqTotal(), CondGt, {50}),
		Query(default_namespace)
			.Debug(LogTrace)
			.Where(kFieldNameGenre, CondEq, 5)
			.Not()
			.Where(Query(default_namespace).Where(kFieldNameGenre, CondEq, 5), CondAny, VariantArray{})
			.Or()
			.Where(kFieldNameGenre, CondSet, Query(joinNs).Select(kFieldNameGenre).Where(kFieldNameId, CondSet, {10, 20, 30, 40}))
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
			.Where(kCompositeFieldIdTemp, CondEq, Query(default_namespace).Select(kCompositeFieldIdTemp).Where(kFieldNameId, CondGt, 10)),
		Query(default_namespace)
			.Where(Query(default_namespace).Select(kCompositeFieldUuidName).Where(kFieldNameId, CondGt, 10), CondRange,
				   {VariantArray::Create(reindexer_tests_tools::nilUuid(), RandString()),
					VariantArray::Create(reindexer_tests_tools::randUuid(), RandString())}),
		Query(default_namespace)
			.Where(Query(default_namespace).Select(kCompositeFieldAgeGenre).Where(kFieldNameId, CondGt, 10).Limit(10), CondLe,
				   {Variant(VariantArray::Create(rand() % 50, rand() % 50))}),

		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2+1"), CondEq, VariantArray{Variant{21}}),
		Query(default_namespace)
			.Where(kFieldNameAge, CondLt, reindexer::expressions::ArithmeticExpression(std::string(kFieldNameYear) + "-2000")),
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondGe,
				   reindexer::expressions::ArithmeticExpression("(" + std::string(kFieldNameYear) + "-2000)*2")),
		Query(default_namespace)
			.Where(reindexer::expressions::ArithmeticExpression("flat_array_len(" + std::string(kFieldNamePackages) + ")+1"), CondGt,
				   kFieldNameYear),
		Query(default_namespace)
			.Where(kFieldNameGenre, CondEq, 5)
			.Or()
			.OpenBracket()
			.Not()
			.Where(reindexer::expressions::ArithmeticExpression("now(sec)-" + std::string(kFieldNameAge)), CondLt,
				   VariantArray{Variant{100}})
			.CloseBracket(),
	};
	for (Query& q : queries) {
		reindexer::WrSerializer wser;
		BindingCapabilities caps{kBindingCapabilityQrIdleTimeouts | kBindingCapabilityResultsWithShardIDs |
								 kBindingCapabilityIncarnationTags | kBindingCapabilityComplexRank | kBindingCapabilityQueryFormatV2};
		Impl(q).Serialize(wser, Normal, caps.GetQueryFormat());
		reindexer::Serializer rser(wser.Slice());
		const auto deserializedQuery = QueryImpl::Deserialize(rser, caps.GetQueryFormat());
		EXPECT_EQ(q, deserializedQuery) << "Origin query:\n" << q.GetSQL() << "\nDeserialized query:\n" << deserializedQuery.GetSQL();
	}
}

TEST_F(QueriesApi, DeserializeRejectsBogusValuesCount) {
	auto expectParseBin = [](reindexer::WrSerializer& wser, std::string_view messagePart, int code = errParseBin) {
		reindexer::Serializer rser(wser.Slice());
		try {
			std::ignore = QueryImpl::Deserialize(rser, QueryFormatV2);
			FAIL() << "expected deserialization error";
		} catch (const reindexer::Error& err) {
			EXPECT_EQ(err.code(), code) << err.what();
			EXPECT_THAT(err.what(), ::testing::HasSubstr(messagePart)) << err.what();
		}
	};

	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(ExpressionTypeArithmetic);
		wser.PutVString(std::string(kFieldNameAge) + "*2");
		wser.PutVarUint(OpAnd);
		wser.PutVarUint(CondEq);
		wser.PutVarUint(ExpressionTypeValues);
		wser.PutVarUint(1'000'000'000'000'000ULL);
		wser.PutVarUint(QueryEnd);
		wser.PutVarUint(0);
		wser.PutVarUint(0);
		expectParseBin(wser, "values count");
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryCondition);
		wser.PutVString(kFieldNameAge);
		wser.PutVarUint(OpAnd);
		wser.PutVarUint(CondEq);
		wser.PutVarUint(1'000'000'000'000'000ULL);
		wser.PutVarUint(QueryEnd);
		wser.PutVarUint(0);
		wser.PutVarUint(0);
		expectParseBin(wser, "values count");
	}
	{
		// The buffer ends on the missing second value. A trailing QueryEnd cannot stand in for it:
		// QueryEnd and KeyValueType::Tuple are both 11, so that byte would be parsed as a tuple.
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(ExpressionTypeArithmetic);
		wser.PutVString(std::string(kFieldNameAge) + "*2");
		wser.PutVarUint(OpAnd);
		wser.PutVarUint(CondEq);
		wser.PutVarUint(ExpressionTypeValues);
		wser.PutVarUint(2);
		wser.PutVariant(reindexer::Variant{1});
		const auto missingValueAt = wser.Slice().size();
		expectParseBin(wser, fmt::format("pos={},len={}", missingValueAt, missingValueAt));
	}
	{
		// One value is a tuple. The outer count is 1, so the unread-byte check on the value list does not see the
		// inner count. That count must be rejected before reserve.
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(ExpressionTypeArithmetic);
		wser.PutVString(std::string(kFieldNameAge) + "*2");
		wser.PutVarUint(OpAnd);
		wser.PutVarUint(CondEq);
		wser.PutVarUint(ExpressionTypeValues);
		wser.PutVarUint(1);
		wser.PutVarUint(reindexer::KeyValueType{reindexer::KeyValueType::Tuple{}}.ToNumber());
		wser.PutVarUint(1'000'000'000'000'000ULL);
		expectParseBin(wser, "values count");
	}
	{
		reindexer::VariantArray point;
		point.emplace_back(1);
		point.emplace_back(2);
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(ExpressionTypeArithmetic);
		wser.PutVString(std::string(kFieldNameAge) + "*2");
		wser.PutVarUint(OpAnd);
		wser.PutVarUint(CondEq);
		wser.PutVarUint(ExpressionTypeValues);
		wser.PutVarUint(1);
		wser.PutVariant(reindexer::Variant{point});
		wser.PutVarUint(QueryEnd);
		wser.PutVarUint(0);
		wser.PutVarUint(0);
		reindexer::Serializer rser(wser.Slice());
		const Query expected = Query(default_namespace)
								   .Where(reindexer::expressions::ArithmeticExpression(std::string(kFieldNameAge) + "*2"), CondEq,
										  reindexer::VariantArray{reindexer::Variant{point}});
		EXPECT_EQ(QueryImpl::Deserialize(rser, QueryFormatV2), expected);
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(ExpressionTypeExpression);
		wser.PutVarUint(1'000'000'000'000'000ULL);
		expectParseBin(wser, "values count");
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryAggregation);
		wser.PutVarUint(AggSum);
		wser.PutVarUint(1'000'000'000'000'000ULL);
		expectParseBin(wser, "values count");
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QuerySortIndex);
		wser.PutVString(kFieldNameAge);
		wser.PutVarUint(0);
		wser.PutVarUint(1'000'000'000'000'000ULL);
		expectParseBin(wser, "values count");
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryEnd);
		wser.PutVarUint(1'000'000'000'000'000ULL);
		expectParseBin(wser, "values count");
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryEnd);
		wser.PutVarUint(0);
		wser.PutVarUint(1'000'000'000'000'000ULL);
		expectParseBin(wser, "values count");
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(99);
		expectParseBin(wser, "not supported");
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(ExpressionTypeArithmetic);
		wser.PutVString("");
		expectParseBin(wser, "Empty WHERE arithmetic expression", errParams);
	}
	{
		reindexer::WrSerializer wser;
		wser.PutVarUint(QueryFormatV2);
		wser.PutVString(default_namespace);
		wser.PutVarUint(QueryExpressions);
		wser.PutVarUint(ExpressionTypeValues);
		wser.PutVarUint(0);
		wser.PutVarUint(OpAnd);
		wser.PutVarUint(CondEq);
		wser.PutVarUint(ExpressionTypeValues);
		wser.PutVarUint(0);
		expectParseBin(wser, "Unsupported type of left expression", errLogic);
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

	auto query =
		Query(default_namespace).Where(kFieldNameIsDeleted, CondEq, {true}).Sort(kFieldNameGenre, SortOrder::Asc, {1, 2, 3}).Limit(10);
	check(query, 10);
	query.Where(kFieldNameIsDeleted, CondEq, {false});
	check(query, 0);
}

TEST_F(QueriesApi, ExplainWithCacheTest) {
	for (int i = 0; i < 100; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i * 100;
		item[kFieldNameAge] = 18 + i;
		item[kFieldNameName] = "name";
		Upsert(default_namespace, item);
		saveItem(std::move(item), default_namespace);
	}

	AwaitIndexOptimization(default_namespace);

	const auto q = Query{default_namespace}
					   .Where(kFieldNameAge, CondEq, {18, 19, 20, 21})
					   .Where(kFieldNameName, CondEq, "name")
					   .Sort(kFieldNameName, SortOrder::Asc);
	{
		QueryResults qr;
		ExecuteAndVerifyWithSql(q, qr);
		EXPECT_NE(qr.GetExplainResults().find(",\"method\":\"index\","), std::string::npos);
	}

	{
		QueryResults qr;
		ExecuteAndVerifyWithSql(q, qr);
		EXPECT_NE(qr.GetExplainResults().find(",\"method\":\"index(cached)\","), std::string::npos);
	}
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

	auto query = Query(default_namespace).Distinct(kFieldNameAge).Sort(kFieldNameId, SortOrder::Asc, forceSortOrder).Limit(5);

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

TEST_F(QueriesApi, FlatArrayFunctionArrayTest) {
	unsigned countriesForCondition = 0, countriesActual = 0;
	unsigned pricesForCondition = 0, pricesActual = 0;

	static const std::string nonIndexPrefix = "_non_index";

	for (int i = 0; i < 1000; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;

		const unsigned countriesCount = 1 + rand() % 5;
		item[kFieldNameCountries] = RandStrVector(countriesCount);
		item[kFieldNameCountries + nonIndexPrefix] = RandStrVector(countriesCount);
		if (countriesCount > 2) {
			++countriesForCondition;
		}

		Upsert(default_namespace, item);
	}
	for (int i = 1000; i < 2000; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;

		const unsigned pricesCount = 1 + rand() % 5;
		item[kFieldNamePriceId] = RandIntVector(pricesCount, 0, 100);
		item[kFieldNamePriceId + nonIndexPrefix] = RandIntVector(pricesCount, 0, 100);
		if (pricesCount == 3) {
			++pricesForCondition;
		}

		Upsert(default_namespace, item);
	}

	unsigned sparseArrayForCondition = 0, sparseArrayActual = 0;
	const std::string kFieldNameSparseArray = "sparse_array";
	DefineNamespaceDataset(default_namespace, {IndexDeclaration{kFieldNameSparseArray, "tree", "string", IndexOpts{}.Sparse().Array(), 0}});

	for (int i = 2000; i < 3000; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;

		const unsigned items = 1 + rand() % 10;
		item[kFieldNameSparseArray] = RandStrVector(items);
		if (items == 4) {
			++sparseArrayForCondition;
		}

		Upsert(default_namespace, item);
	}

	QueryResults qrCountries{rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNameCountries), CondGt, 2))};
	for (auto it : qrCountries) {
		auto item = it.GetItem();

		auto countriesField = item[item.GetFieldIndex(kFieldNameCountries)];
		VariantArray countriesValues = countriesField;
		ASSERT_GT(countriesValues.size(), 2);

		unsigned countriesNonIndexedCount = 0;

		gason::JsonParser parser;
		auto root = parser.Parse(item.GetJSON());
		auto countriesNonIndexed = root[kFieldNameCountries + nonIndexPrefix];
		for (const auto& it : countriesNonIndexed) {
			if (!it.isEmpty()) {
				++countriesNonIndexedCount;
			}
		}
		ASSERT_GT(countriesNonIndexedCount, 2);

		++countriesActual;
	}
	ASSERT_EQ(countriesForCondition, countriesActual);

	QueryResults qrPrices{rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNamePriceId), CondEq, 3))};
	for (auto it : qrPrices) {
		auto item = it.GetItem();
		auto pricesField = item[item.GetFieldIndex(kFieldNamePriceId)];
		VariantArray pricesValues = pricesField;
		ASSERT_EQ(pricesValues.size(), 3);
		++pricesActual;
	}
	ASSERT_EQ(pricesForCondition, pricesActual);

	pricesActual = 0;

	QueryResults qrPricesBothConditions{
		rt.Select(Query(default_namespace)
					  .Where(reindexer::functions::FlatArrayLen(kFieldNamePriceId), CondEq, 3)
					  .Where(reindexer::functions::FlatArrayLen(kFieldNamePriceId + nonIndexPrefix), CondEq, 3))};
	for (auto it : qrPricesBothConditions) {
		unsigned count = 0;
		auto item = it.GetItem();
		gason::JsonParser parser;
		auto root = parser.Parse(item.GetJSON());
		auto pricesNonIndexed = root[kFieldNamePriceId + nonIndexPrefix];
		for (const auto& it : pricesNonIndexed) {
			if (!it.isEmpty()) {
				++count;
			}
		}
		ASSERT_EQ(count, 3);
		++pricesActual;
	}
	ASSERT_EQ(pricesForCondition, pricesActual);

	QueryResults qrSparseArray{
		rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNameSparseArray), CondEq, 4))};
	for (auto it : qrSparseArray) {
		auto item = it.GetItem();
		auto field = item[kFieldNameSparseArray];
		VariantArray values = field;
		ASSERT_EQ(values.size(), 4);
		++sparseArrayActual;
	}
	ASSERT_EQ(sparseArrayForCondition, sparseArrayActual);
}

TEST_F(QueriesApi, FlatArrayFunctionSingularFieldTest) {
	static const std::string nonIndexPrefix = "_non_index";

	for (int i = 0; i < 1000; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;

		if (i % 2 == 0) {
			item[kFieldNameYear] = 2010 + rand() % 10;
			item[kFieldNameYear + nonIndexPrefix] = 2015 + rand() % 10;
			item[kFieldNameYearSparse] = RandString();
		}

		Upsert(default_namespace, item);
	}

	QueryResults qrYear{rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNameYear), CondEq, 1))};
	ASSERT_EQ(qrYear.Count(), 1000);

	QueryResults qrYearSparse{
		rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNameYearSparse), CondEq, 1))};
	ASSERT_EQ(qrYearSparse.Count(), 500);

	QueryResults qrYearSparseNull{
		rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNameYearSparse), CondEq, 0))};
	ASSERT_EQ(qrYearSparseNull.Count(), 500);

	QueryResults qrYearNonIndexed{
		rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen(kFieldNameYear + nonIndexPrefix), CondEq, 1))};
	ASSERT_EQ(qrYearNonIndexed.Count(), 500);
}

TEST_F(QueriesApi, FlatArrayFunctionNestedQueriesTest) {
	for (int i = 0; i < 1000; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;
		item[kFieldNameYear] = 2015 + rand() % 10;
		if (i % 2 == 0) {
			item[kFieldNamePriceId] = RandIntVector(2, 0, 100);
		}
		Upsert(default_namespace, item);
	}

	QueryResults qrYear{
		rt.ExecSQL("SELECT * FROM test_namespace WHERE flat_array_len(price_id) = "
				   "(SELECT id FROM test_namespace WHERE id = 2 AND flat_array_len(year) = 1);")};
	ASSERT_EQ(qrYear.Count(), 500);
}

TEST_F(QueriesApi, FlatArrayFunctionObjectFieldTest) {
	auto upsertItem = [this](std::string_view json) {
		Item item = NewItem(default_namespace);
		Error err = item.FromJSON(json);
		ASSERT_TRUE(err.ok()) << err.what();
		Upsert(default_namespace, item);
	};

	for (int i = 0; i < 100; ++i) {
		upsertItem(fmt::format(R"({{"id":{},"obj":{{"name":"{}","price":{},"year":2025}}}})", i, RandString(), rand()));
	}
	QueryResults qr{rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen("obj"), CondEq, 1))};
	ASSERT_EQ(qr.Count(), 100);

	for (int i = 100; i < 200; ++i) {
		upsertItem(fmt::format(R"({{"id":{},"empty_obj":{{}}}})", i));
	}
	qr = rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen("empty_obj"), CondEq, 1));
	ASSERT_EQ(qr.Count(), 100);

	for (int i = 200; i < 300; ++i) {
		upsertItem(fmt::format(R"({{"id":{},"obj":null}})", i));
	}
	qr = rt.Select(Query(default_namespace).Where("id", CondRange, {200, 300}).Where(reindexer::functions::FlatArrayLen("obj"), CondEq, 0));
	ASSERT_EQ(qr.Count(), 100);

	for (int i = 300; i < 400; ++i) {
		upsertItem(fmt::format(R"({{"id":{}}})", i));
	}
	qr = rt.Select(Query(default_namespace).Where("id", CondRange, {300, 400}).Where(reindexer::functions::FlatArrayLen("obj"), CondEq, 0));
	ASSERT_EQ(qr.Count(), 100);
}

TEST_F(QueriesApi, FlatArrayFunctionObjectsArrayTest) {
	for (int i = 0; i < 1000; ++i) {
		Item item = NewItem(default_namespace);
		Error err = item.FromJSON(fmt::format(R"(
		{{
			"id":{},
			"items": [
				{{"obj":{{"name":"test1","year":2022}}}},
				{{"obj":{{"name":"test2","year":2023}}}},
				{{"obj":{{"name":"test3","year":2024}}}},
				{{"obj":{{"name":"test4","year":2025}}}},
				{{"obj":{{"name":"test5","year":2026}}}},
				{{"obj":{{}}}},
				{{"obj":null}}
			],
			"null_items": [
				{{"obj":null}},
				{{"obj":null}}
			]
		}})",
											  i));
		ASSERT_TRUE(err.ok()) << err.what();
		Upsert(default_namespace, item);
	}

	QueryResults qr{rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen("items.obj"), CondEq, 6))};
	ASSERT_EQ(qr.Count(), 1000);

	qr = rt.Select(Query(default_namespace).Where(reindexer::functions::FlatArrayLen("null_items.obj"), CondEq, 0));
	ASSERT_EQ(qr.Count(), 1000);
}

TEST_F(QueriesApi, TestUpdateFieldWithFlatArrayLen) {
	for (int i = 0; i < 100; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;
		item[kFieldNamePackages] = RandIntVector(5, 0, 100);
		Upsert(default_namespace, item);
	}
	auto qr = rt.ExecSQL(
		fmt::format("update {} set {} = flat_array_len({}) where id >= 49;", default_namespace, kFieldNameAge, kFieldNamePackages));
	ASSERT_EQ(qr.Count(), 51);
	for (auto it : qr) {
		auto item = it.GetItem();
		Variant age = item[kFieldNameAge];
		ASSERT_EQ(age.As<int>(), 5);
	}
}

TEST_F(QueriesApi, NowFunctionTest) {
	const int64_t nowSeconds{getTimeNow(reindexer::TimeUnit::sec)};
	for (int i = 0; i < 100; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;
		item[kFieldNameStartTime] = nowSeconds + 60;
		Upsert(default_namespace, item);
	}
	for (int i = 100; i < 200; ++i) {
		Item item = NewItem(default_namespace);
		item[kFieldNameId] = i;
		item[kFieldNameStartTime] = nowSeconds - 60;
		Upsert(default_namespace, item);
	}
	{
		QueryResults qr{rt.Select(Query(default_namespace).Where(kFieldNameStartTime, CondGt, reindexer::functions::Now()))};
		ASSERT_EQ(qr.Count(), 100);
		for (auto it : qr) {
			auto item = it.GetItem();
			Variant id = item[kFieldNameId];
			ASSERT_LE(id.As<int>(), 99);
		}
	}
	{
		QueryResults qr1{rt.Select(Query(default_namespace).Where(kFieldNameStartTime, CondLt, reindexer::functions::Now()))};
		ASSERT_EQ(qr1.Count(), 100);
		for (auto it : qr1) {
			auto item = it.GetItem();
			Variant id = item[kFieldNameId];
			ASSERT_GE(id.As<int>(), 100);
			ASSERT_LE(id.As<int>(), 199);
		}
		QueryResults qr2{rt.ExecSQL(fmt::format("select * from {} where {} < now();", default_namespace, kFieldNameStartTime))};
		ASSERT_EQ(qr1.Count(), qr2.Count());
		auto it1 = qr1.begin();
		auto it2 = qr2.begin();
		while (it1 != qr1.end() && it2 != qr2.end()) {
			Variant t1 = it1.GetItem()[kFieldNameStartTime];
			Variant t2 = it2.GetItem()[kFieldNameStartTime];
			ASSERT_EQ(t1, t2);
			++it1;
			++it2;
		}
		ASSERT_EQ(it1, qr1.end());
		ASSERT_EQ(it2, qr2.end());
	}
}

}  // namespace reindexer_tests
