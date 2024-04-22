#pragma once

#include <atomic>
#include <ostream>
#include "core/type_consts.h"

namespace reindexer {

struct SelectCtx;
class ExplainCalc;
class StringsHolder;
class Query;

class ActiveQueryScope {
public:
	// Core query scope
	ActiveQueryScope(SelectCtx &ctx, const std::atomic<int> &nsOptimizationState, ExplainCalc &explainCalc,
					 const std::atomic<int> &nsLockerState, StringsHolder *strHolder) noexcept;
	ActiveQueryScope(const Query &q, QueryType realQueryType, const std::atomic<int> &nsOptimizationState,
					 StringsHolder *strHolder) noexcept;
	// External query scope
	ActiveQueryScope(const Query &q, QueryType realQueryType) noexcept;
	explicit ActiveQueryScope(std::string_view sql) noexcept;
	~ActiveQueryScope();

public:
	enum class Type { NoTracking, CoreQueryTracker, ExternalQueryTracker, ExternalSQLQueryTracker };

	Type type_ = Type::NoTracking;
};

void PrintCrashedQuery(std::ostream &sout);

}  // namespace reindexer
