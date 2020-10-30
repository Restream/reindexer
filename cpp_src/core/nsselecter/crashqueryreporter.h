#pragma once

#include <atomic>
#include <ostream>

namespace reindexer {

struct SelectCtx;
class ExplainCalc;

class ActiveQueryScope {
public:
	ActiveQueryScope(SelectCtx &ctx, std::atomic<int> &nsOptimizationState, ExplainCalc &explainCalc);
	~ActiveQueryScope();

public:
	bool mainQuery_;
};

void PrintCrashedQuery(std::ostream &sout);

}  // namespace reindexer
