#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <string_view>
#include "core/query/query_impl.h"
#include "core/query/sql/sql_formatters.h"
#include "tools/scope_guard.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer_tests {

TEST(Sql, PrettyFormat) {
	struct {
		std::string_view oneLineSql;
		std::string_view prettySql;
	} cases[]{
		{"SELECT * FROM ns WHERE id = 2 MERGE (SELECT * FROM ns2 WHERE id = 5)",
		 R"(SELECT
  *
FROM
  ns
WHERE
  id = 2
MERGE (
  SELECT
    *
  FROM
    ns2
  WHERE
    id = 5
))"},
		{"SELECT RANK(), distinct(fld1, fld2), id, field, vectors(), COUNT(*) FROM ns",
		 R"(SELECT
  RANK(),
  distinct(
    fld1,
    fld2
  ),
  id,
  field,
  vectors(),
  COUNT(*)
FROM
  ns)"},

		{"SELECT facet(fld1, fld2 ORDER BY 'fld1' ASC OFFSET 5 LIMIT 9) FROM ns",
		 R"(SELECT
  facet(
    fld1,
    fld2
    ORDER BY 'fld1' ASC
    OFFSET 5
    LIMIT 9
  )
FROM
  ns)"},

		{"SELECT * FROM ns ORDER BY 'fld1', 'fld2' OFFSET 5 LIMIT 9",
		 R"(SELECT
  *
FROM
  ns
ORDER BY
  'fld1',
  'fld2'
OFFSET 5
LIMIT 9)"},
		{"SELECT * FROM ns1 WHERE (SELECT * FROM ns2 WHERE ST_DWithin(rtree, ST_GeomFromText('POINT(0.0 0.0)'), 100.0) AND age = 1 LIMIT "
		 "0) IS NOT NULL AND "
		 "(SELECT time FROM ns3 WHERE id = 45) > now(sec) OR start_time > (SELECT time FROM ns3 WHERE id = 45)",

		 R"(SELECT
  *
FROM
  ns1
WHERE
  (
    SELECT
      *
    FROM
      ns2
    WHERE
      ST_DWithin(rtree, ST_GeomFromText('POINT(0.0 0.0)'), 100.0)
      AND age = 1
    LIMIT 0
  ) IS NOT NULL
  AND (
    (
      SELECT
        time
      FROM
        ns3
      WHERE
        id = 45
    ) > now(sec)
    OR start_time > (
      SELECT
        time
      FROM
        ns3
      WHERE
        id = 45
    )
  ))"},
		{"SELECT * FROM ns ORDER BY FIELD(country, 'Los', 'Fos')", R"(SELECT
  *
FROM
  ns
ORDER BY
  FIELD(
    country,
    'Los',
    'Fos'
  ))"}};

	for (const auto& testCase : cases) {
		auto query = reindexer::Query::FromSQL(testCase.oneLineSql);
		EXPECT_EQ(Impl(query).GetSQL(QueryType::QuerySelect), testCase.oneLineSql);
		EXPECT_EQ(Impl(query).GetSQL(QueryType::QuerySelect, reindexer::Pretty_True), testCase.prettySql);

		query = reindexer::Query::FromSQL(testCase.prettySql);
		EXPECT_EQ(Impl(query).GetSQL(QueryType::QuerySelect, reindexer::Pretty_True), testCase.prettySql);
	}
}

TEST(Sql, FormatDuringStackUnwinding) {
	constexpr std::string_view kQueries[]{
		"SELECT * FROM ns",
		"SELECT max(id) FROM ns WHERE id IN (1, 2)",
		"SELECT * FROM ns WHERE (id IN (1, 2) OR n = 1) AND id = (SELECT id FROM ns2 WHERE n > 1 LIMIT 1)",
		"SELECT * FROM ns WHERE id = 1 OR n = 2 AND n < 5",
		"SELECT * FROM ns ORDER BY FIELD(id, 1, 2)",
		"SELECT * FROM ns INNER JOIN (SELECT * FROM ns2 WHERE n IN (1, 2)) ON ns2.id = ns.id",
		"SELECT * FROM ns MERGE (SELECT * FROM ns2 WHERE id = 5)",
		"SELECT * FROM ns WHERE KNN(vec, [1, 2], k=1)",
		"UPDATE ns SET arr = [1, 2] WHERE id IN (1, 2)",
		"DELETE FROM ns WHERE (id = 1 OR id = 2) AND n = 1",
	};

	for (const auto sql : kQueries) {
		const auto query = reindexer::Query::FromSQL(sql);
		for (const auto pretty : {reindexer::Pretty_False, reindexer::Pretty_True}) {
			const auto getSQL = [&query, pretty] { return Impl(query).GetSQL(Impl(query).Type(), pretty); };
			std::string sqlDuringUnwinding;
			EXPECT_THROW(
				{
					const auto guard = reindexer::MakeScopeGuard([&] { sqlDuringUnwinding = getSQL(); });
					throw std::runtime_error("Unwind the stack");
				},
				std::runtime_error);
			EXPECT_EQ(sqlDuringUnwinding, getSQL()) << sql;
		}
	}
}

TEST(Sql, PrettyFormatSerializerErrorDuringFirstCondition) {
	constexpr size_t kMaxCapacity = 256;
	constexpr bool kEncloseOr = false;
	constexpr bool kNextIsOr = false;
	reindexer::WrSerializer ser{reindexer::WrSerializer::GrowthPolicy{kMaxCapacity}};
	ser.Write(std::string(ser.Cap(), 'x'));

	EXPECT_THROW(
		{
			reindexer::PrettySqlFormatter formatter{ser};
			auto conditions = formatter.StartConditions(kEncloseOr);
			conditions.AddCondition(OpNot, kNextIsOr);
		},
		reindexer::Error);
}

}  // namespace reindexer_tests
