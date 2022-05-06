#include "crashqueryreporter.h"
#include <sstream>
#include "core/cjson/jsonbuilder.h"
#include "core/namespace/namespaceimpl.h"
#include "debug/backtrace.h"
#include "nsselecter.h"
#include "tools/logger.h"

namespace reindexer {

struct QueryDebugContext {
	SelectCtx *selectCtx = nullptr;
	std::atomic<int> *nsOptimizationState = nullptr;
	ExplainCalc *explainCalc = nullptr;
	std::atomic_bool *nsLockerState = nullptr;
	StringsHolder *nsStrHolder = nullptr;
};

thread_local QueryDebugContext g_queryDebugCtx;

ActiveQueryScope::ActiveQueryScope(SelectCtx &ctx, std::atomic<int> &nsOptimizationState, ExplainCalc &explainCalc,
								   std::atomic_bool &nsLockerState, StringsHolder *strHolder)
	: isTrackedQuery_(ctx.requiresCrashTracking) {
	if (isTrackedQuery_) {
		g_queryDebugCtx.selectCtx = &ctx;
		g_queryDebugCtx.nsOptimizationState = &nsOptimizationState;
		g_queryDebugCtx.explainCalc = &explainCalc;
		g_queryDebugCtx.nsLockerState = &nsLockerState;
		g_queryDebugCtx.nsStrHolder = strHolder;
	}
}
ActiveQueryScope::~ActiveQueryScope() {
	if (isTrackedQuery_) {
		if (!g_queryDebugCtx.selectCtx) {
			logPrintf(LogWarning, "~ActiveQueryScope: Empty context for tracked query");
		}
		g_queryDebugCtx.selectCtx = nullptr;
		g_queryDebugCtx.nsOptimizationState = nullptr;
		g_queryDebugCtx.explainCalc = nullptr;
		g_queryDebugCtx.nsLockerState = nullptr;
		g_queryDebugCtx.nsStrHolder = nullptr;
	}
}

static std::string_view nsOptimizationStateName(int state) {
	using namespace std::string_view_literals;
	switch (state) {
		case NamespaceImpl::NotOptimized:
			return "Not optimized"sv;
		case NamespaceImpl::OptimizedPartially:
			return "Optimized Partially"sv;
		case NamespaceImpl::OptimizationCompleted:
			return "Optimization completed"sv;
		default:
			return "<Unknown>"sv;
	}
}

void PrintCrashedQuery(std::ostream &out) {
	if (!g_queryDebugCtx.selectCtx) {
		out << "*** No additional info from crash query tracker ***" << std::endl;
		return;
	}

	out << "*** Current query dump ***" << std::endl;
	out << " Query:    " << g_queryDebugCtx.selectCtx->query.GetSQL() << std::endl;
	if (g_queryDebugCtx.selectCtx->parentQuery) {
		out << " Parent Query:    " << g_queryDebugCtx.selectCtx->parentQuery->GetSQL() << std::endl;
	}
	out << " NS state: " << nsOptimizationStateName(g_queryDebugCtx.nsOptimizationState->load()) << std::endl;
	out << " NS.locker state: ";
	if (g_queryDebugCtx.nsLockerState->load()) {
		out << " readonly";
	} else {
		out << " regular";
	}
	out << std::endl;
	out << " NS.strHolder state: [" << std::endl;
	out << " memstat = " << g_queryDebugCtx.nsStrHolder->MemStat() << std::endl;
	out << " holds indexes = " << std::boolalpha << g_queryDebugCtx.nsStrHolder->HoldsIndexes() << std::endl;
	if (g_queryDebugCtx.nsStrHolder->HoldsIndexes()) {
		const auto &indexes = g_queryDebugCtx.nsStrHolder->Indexes();
		out << " indexes.size = " << indexes.size() << std::endl;
		out << " indexes = [";
		for (size_t i = 0; i < indexes.size(); ++i) {
			if (i) out << " ";
			out << indexes[i]->Name();
		}
		out << "]" << std::endl;
	}
	out << "]" << std::endl;
	out << " Explain:  " << g_queryDebugCtx.explainCalc->GetJSON() << std::endl;

	g_queryDebugCtx.selectCtx = nullptr;
}

}  // namespace reindexer
