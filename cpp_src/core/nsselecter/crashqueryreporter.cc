#include "crashqueryreporter.h"
#include <sstream>
#include "core/cjson/jsonbuilder.h"
#include "core/namespace/namespaceimpl.h"
#include "debug/backtrace.h"
#include "nsselecter.h"

namespace reindexer {

struct QueryDebugContext {
	SelectCtx *selectCtx = nullptr;
	std::atomic<int> *nsOptimizationState = nullptr;
	ExplainCalc *explainCalc = nullptr;
};

thread_local QueryDebugContext g_queryDebugCtx;

ActiveQueryScope::ActiveQueryScope(SelectCtx &ctx, std::atomic<int> &nsOptimizationState, ExplainCalc &explainCalc)
	: mainQuery_(ctx.preResult == nullptr) {
	if (mainQuery_) {
		g_queryDebugCtx.selectCtx = &ctx;
		g_queryDebugCtx.nsOptimizationState = &nsOptimizationState;
		g_queryDebugCtx.explainCalc = &explainCalc;
	}
}
ActiveQueryScope::~ActiveQueryScope() {
	if (mainQuery_) {
		g_queryDebugCtx.selectCtx = nullptr;
		g_queryDebugCtx.nsOptimizationState = nullptr;
		g_queryDebugCtx.explainCalc = nullptr;
	}
}

static std::string_view nsOptimizationStateName(int state) {
	using namespace std::string_view_literals;
	switch (state) {
		case NamespaceImpl::NotOptimized:
			return "Not optimized"sv;
		case NamespaceImpl::OptimizingIndexes:
			return "Optimizing indexes"sv;
		case NamespaceImpl::OptimizingSortOrders:
			return "Optimizing sort orders"sv;
		case NamespaceImpl::OptimizationCompleted:
			return "Optimization completed"sv;
		default:
			return "<Unknown>"sv;
	}
}

void PrintCrashedQuery(std::ostream &out) {
	if (!g_queryDebugCtx.selectCtx) return;

	out << "*** Current query dump ***" << std::endl;
	out << " Query:    " << g_queryDebugCtx.selectCtx->query.GetSQL() << std::endl;
	out << " NS state: " << nsOptimizationStateName(g_queryDebugCtx.nsOptimizationState->load()) << std::endl;
	out << " Explain:  " << g_queryDebugCtx.explainCalc->GetJSON() << std::endl;
}

}  // namespace reindexer
