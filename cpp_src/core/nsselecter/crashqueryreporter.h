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
	ActiveQueryScope(SelectCtx &ctx, std::atomic<int> &nsOptimizationState, ExplainCalc &explainCalc, std::atomic_bool &nsLockerState,
					 StringsHolder *strHolder) noexcept;
	ActiveQueryScope(const Query &q, QueryType realQueryType, std::atomic<int> &nsOptimizationState, StringsHolder *strHolder) noexcept;
	~ActiveQueryScope();

public:
	bool isTrackedQuery_;
};

void PrintCrashedQuery(std::ostream &sout);

}  // namespace reindexer
