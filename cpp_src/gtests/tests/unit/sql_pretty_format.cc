#include <gtest/gtest.h>
#include <string_view>
#include "core/enums.h"
#include "core/query/impl.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"

namespace reindexer_tests {

using reindexer::impl::Impl;

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
		EXPECT_EQ(Impl{query}->GetSQL(QueryType::QuerySelect), testCase.oneLineSql);
		EXPECT_EQ(Impl{query}->GetSQL(QueryType::QuerySelect, reindexer::Pretty_True), testCase.prettySql);

		query = reindexer::Query::FromSQL(testCase.prettySql);
		EXPECT_EQ(Impl{query}->GetSQL(QueryType::QuerySelect, reindexer::Pretty_True), testCase.prettySql);
	}
}

}  // namespace reindexer_tests
