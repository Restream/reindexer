#include "sqltokenmatching.h"

#include "core/query/knn_search_params.h"

namespace reindexer {

const SqlTokenMatchings& sqlTokenMatchings() {
	static const SqlTokenMatchings matchings = {
		{Start, {"explain", "select", "delete", "update", "truncate", "local"}},
		{StartAfterLocal, {"explain", "select"}},
		{StartAfterExplain, {"select", "delete", "update", "local"}},
		{StartAfterLocalExplain, {"select"}},
		{AggregationSqlToken, {"sum", "avg", "max", "min", "facet", "count", "distinct", "rank()", "count_cached", "vectors()"}},
		{SelectConditionsStart, {"where", "limit", "offset", "order", "join", "left", "inner", "equal_position", "merge", "or", ";"}},
		{NestedSelectConditionsStart, {"where", "limit", "offset", "order", "join", "left", "inner", "equal_position"}},
		{ConditionSqlToken, {">", ">=", "<", "<=", "<>", "in", "allset", "range", "is", "==", "="}},
		{WhereFieldValueSqlToken, {"null", "empty", "not"}},
		{WhereFieldNegateValueSqlToken, {"null", "empty"}},
		{OpSqlToken, {"and", "or"}},
		{WhereOpSqlToken, {"and", "or", "order", "join", "left", "inner", "equal_position"}},
		{SortDirectionSqlToken, {"asc", "desc"}},
		{JoinTypesSqlToken, {"join", "left", "inner"}},
		{LeftSqlToken, {"join"}},
		{InnerSqlToken, {"join"}},
		{SelectSqlToken, {"select"}},
		{OnSqlToken, {"on"}},
		{BySqlToken, {"by"}},
		{NotSqlToken, {"not"}},
		{FieldSqlToken, {"field"}},
		{FromSqlToken, {"from"}},
		{SetSqlToken, {"set"}},
		{WhereSqlToken, {"where"}},
		{AllFieldsToken, {"*"}},
		{ModifyConditionsStart, {"where", "limit", "offset", "order"}},
		{UpdateOptionsSqlToken, {"set", "drop"}},
		{EqualPositionSqlToken, {"equal_position"}},
		{WhereFunction, {"ST_DWithin", "KNN", "flat_array_len"}},
		{ST_GeomFromTextSqlToken, {"ST_GeomFromText"}},
		{KnnParamsToken,
		 {std::string{KnnSearchParams::kKName}, std::string{KnnSearchParams::kEfName}, std::string{KnnSearchParams::kNProbeName}}}};
	return matchings;
}

void getMatchingSqlTokens(SqlTokenType tokenType, const std::string& token, std::unordered_set<std::string>& variants) {
	const auto suggestionsIt = sqlTokenMatchings().find(tokenType);
	if (suggestionsIt == sqlTokenMatchings().end()) {
		return;
	}
	for (const auto& suggestion : suggestionsIt->second) {
		if (isBlank(token) || checkIfStartsWith(token, suggestion)) {
			variants.insert(suggestion);
		}
	}
}

}  // namespace reindexer
