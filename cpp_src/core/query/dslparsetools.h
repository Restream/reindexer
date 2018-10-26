#pragma once

#include "core/query/query.h"
#include "core/type_consts.h"
#include "gason/gason.h"

namespace reindexer {
class Query;
namespace dsl {

enum class Root {
	Namespace,
	Limit,
	Offset,
	Distinct,
	Filters,
	Sort,
	Joined,
	Merged,
	SelectFilter,
	SelectFunctions,
	ReqTotal,
	NextOp,
	Aggregations,
	Explain
};

enum class Sort { Desc, Field, Values };
enum class JoinRoot { Type, On, Op, Namespace, Filters, Sort, Limit, Offset };
enum class JoinEntry { LetfField, RightField, Cond, Op };
enum class Filter { Cond, Op, Field, Value };
enum class Aggregation { Field, Type };

void parse(JsonValue& value, Query& q);
}  // namespace dsl
}  // namespace reindexer
