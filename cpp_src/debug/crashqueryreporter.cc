#include "crashqueryreporter.h"
#include <sstream>
#include "core/nsselecter/nsselecter.h"
#include "debug/backtrace.h"
#include "tools/logger.h"

namespace reindexer {

struct [[nodiscard]] QueryDebugContext {
	bool HasTrackedQuery() const noexcept { return mainQuery || externQuery || parentQuery || !externSql.empty(); }
	std::string_view GetMainSQL(std::string& storage) const noexcept {
		try {
			storage.clear();
			if (mainQuery) {
				storage = mainQuery->GetSQL(realQueryType);
			} else if (externQuery) {
				storage = externQuery->GetSQL(externRealQueryType);
			} else if (!externSql.empty()) {
				return externSql;
			}
			return storage;
		} catch (...) {
			return "<unable to get tracked main query SQL (exception)>";
		}
	}
	std::string_view GetParentSQL(std::string& storage) const noexcept {
		try {
			storage.clear();
			if (parentQuery) {
				storage = parentQuery->GetSQL(realQueryType);
			}
			return storage;
		} catch (...) {
			return "<unable to get tracked parent query SQL (exception)>";
		}
	}
	void ResetQueries() noexcept {
		mainQuery = externQuery = parentQuery = nullptr;
		externSql = std::string_view();
	}

	const Query* mainQuery = nullptr;
	const Query* externQuery = nullptr;
	std::string_view externSql;
	const Query* parentQuery = nullptr;
	const std::atomic<OptimizationState>* nsOptimizationState = nullptr;
	ExplainCalc* explainCalc = nullptr;
	const std::atomic<int>* nsLockerState = nullptr;
	StringsHolder* nsStrHolder = nullptr;
	QueryType realQueryType = QuerySelect;
	QueryType externRealQueryType = QuerySelect;
};

thread_local QueryDebugContext g_queryDebugCtx;

ActiveQueryScope::ActiveQueryScope(SelectCtx& ctx, const std::atomic<OptimizationState>& nsOptimizationState, ExplainCalc& explainCalc,
								   const std::atomic<int>& nsLockerState, StringsHolder* strHolder) noexcept
	: type_(ctx.requiresCrashTracking ? Type::CoreQueryTracker : Type::NoTracking) {
	if (ctx.requiresCrashTracking) {
		g_queryDebugCtx.mainQuery = &ctx.query;
		g_queryDebugCtx.parentQuery = ctx.parentQuery;
		g_queryDebugCtx.nsOptimizationState = &nsOptimizationState;
		g_queryDebugCtx.explainCalc = &explainCalc;
		g_queryDebugCtx.nsLockerState = &nsLockerState;
		g_queryDebugCtx.nsStrHolder = strHolder;
		g_queryDebugCtx.realQueryType = ctx.crashReporterQueryType;
	}
}

ActiveQueryScope::ActiveQueryScope(const Query& q, QueryType realQueryType, const std::atomic<OptimizationState>& nsOptimizationState,
								   StringsHolder* strHolder) noexcept
	: type_(Type::CoreQueryTracker) {
	g_queryDebugCtx.mainQuery = &q;
	g_queryDebugCtx.parentQuery = nullptr;
	g_queryDebugCtx.nsOptimizationState = &nsOptimizationState;
	g_queryDebugCtx.explainCalc = nullptr;
	g_queryDebugCtx.nsLockerState = nullptr;
	g_queryDebugCtx.nsStrHolder = strHolder;
	g_queryDebugCtx.realQueryType = realQueryType;
}

ActiveQueryScope::ActiveQueryScope(const Query& q, QueryType realQueryType) noexcept : type_(Type::ExternalQueryTracker) {
	g_queryDebugCtx.externQuery = &q;
	g_queryDebugCtx.externRealQueryType = realQueryType;
}

ActiveQueryScope::ActiveQueryScope(std::string_view sql) noexcept : type_(Type::ExternalSQLQueryTracker) {
	g_queryDebugCtx.externSql = sql;
}

ActiveQueryScope::~ActiveQueryScope() {
	switch (type_) {
		case Type::NoTracking:
			break;
		case Type::CoreQueryTracker:
			if (!g_queryDebugCtx.mainQuery) {
				logFmt(LogWarning, "~ActiveQueryScope: Empty query pointer in the ActiveQueryScope");
			}
			g_queryDebugCtx.mainQuery = nullptr;
			g_queryDebugCtx.parentQuery = nullptr;
			g_queryDebugCtx.nsOptimizationState = nullptr;
			g_queryDebugCtx.explainCalc = nullptr;
			g_queryDebugCtx.nsLockerState = nullptr;
			g_queryDebugCtx.nsStrHolder = nullptr;
			g_queryDebugCtx.realQueryType = QuerySelect;
			break;
		case Type::ExternalQueryTracker:
			if (!g_queryDebugCtx.externQuery) {
				logFmt(LogWarning, "~ActiveQueryScope: Empty external query pointer in the ActiveQueryScope");
			}
			g_queryDebugCtx.externQuery = nullptr;
			g_queryDebugCtx.externRealQueryType = QuerySelect;
			break;
		case Type::ExternalSQLQueryTracker:
			if (g_queryDebugCtx.externSql.empty()) {
				logFmt(LogWarning, "~ActiveQueryScope: Empty external query SQL in the ActiveQueryScope");
			}
			g_queryDebugCtx.externSql = std::string_view();
			break;
	}
}

static std::string_view nsOptimizationStateName(OptimizationState state) {
	using namespace std::string_view_literals;
	switch (state) {
		case OptimizationState::None:
			return "Not optimized"sv;
		case OptimizationState::Partial:
			return "Optimized Partially"sv;
		case OptimizationState::Completed:
			return "Optimization completed"sv;
		case OptimizationState::Error:
			return "Unexpected optimization error"sv;
		default:
			return "<Unknown>"sv;
	}
}

static std::string_view nsInvalidationStateName(int state) {
	using namespace std::string_view_literals;
	switch (NamespaceImpl::InvalidationType(state)) {
		case NamespaceImpl::InvalidationType::Valid:
			return "Valid"sv;
		case NamespaceImpl::InvalidationType::Readonly:
			return "Readonly"sv;
		case NamespaceImpl::InvalidationType::OverwrittenByUser:
			return "Overwritten by user"sv;
		case NamespaceImpl::InvalidationType::OverwrittenByReplicator:
			return "Overwritten by replicator (force sync)"sv;
		default:
			return "<Unknown>"sv;
	}
}

void PrintCrashedQuery(std::ostream& out) {
	if (!g_queryDebugCtx.HasTrackedQuery()) {
		out << "*** No additional info from crash query tracker ***" << std::endl;
		return;
	}

	out << "*** Current query dump ***" << std::endl;
	std::string storage;
	out << " Query:    " << g_queryDebugCtx.GetMainSQL(storage) << std::endl;
	if (g_queryDebugCtx.parentQuery) {
		out << " Parent Query:    " << g_queryDebugCtx.GetParentSQL(storage) << std::endl;
	}
	if (g_queryDebugCtx.nsOptimizationState) {
		out << " NS state: " << nsOptimizationStateName(g_queryDebugCtx.nsOptimizationState->load()) << std::endl;
	}
	if (g_queryDebugCtx.nsLockerState) {
		out << " NS.locker state: ";
		out << nsInvalidationStateName(g_queryDebugCtx.nsLockerState->load());
		out << std::endl;
	}
	if (g_queryDebugCtx.nsStrHolder) {
		out << " NS.strHolder state: [" << std::endl;
		out << " memstat = " << g_queryDebugCtx.nsStrHolder->MemStat() << std::endl;
		out << " holds indexes = " << std::boolalpha << g_queryDebugCtx.nsStrHolder->HoldsIndexes() << std::endl;
		if (g_queryDebugCtx.nsStrHolder->HoldsIndexes()) {
			const auto& indexes = g_queryDebugCtx.nsStrHolder->Indexes();
			out << " indexes.size = " << indexes.size() << std::endl;
			out << " indexes = [";
			for (size_t i = 0; i < indexes.size(); ++i) {
				if (i) {
					out << " ";
				}
				out << indexes[i]->Name();
			}
			out << "]" << std::endl;
		}
		out << "]" << std::endl;
	}
	if (g_queryDebugCtx.explainCalc) {
		out << " Explain:  " << g_queryDebugCtx.explainCalc->GetJSON() << std::endl;
	}

	g_queryDebugCtx.ResetQueries();
}

}  // namespace reindexer
