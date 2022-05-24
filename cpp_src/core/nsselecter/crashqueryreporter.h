#pragma once

#include <atomic>
#include <ostream>

namespace reindexer {

struct SelectCtx;
class ExplainCalc;
class StringsHolder;

class ActiveQueryScope {
public:
	ActiveQueryScope(SelectCtx &ctx, std::atomic<int> &nsOptimizationState, ExplainCalc &explainCalc, std::atomic_bool &nsLockerState,
					 StringsHolder *strHolder);
	~ActiveQueryScope();

public:
	bool isTrackedQuery_;
};

void PrintCrashedQuery(std::ostream &sout);

}  // namespace reindexer
