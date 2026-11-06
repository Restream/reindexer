#pragma once

#include "core/namespace/namespaceimpl.h"
#include "core/nsselecter/explaincalc.h"
#include "core/nsselecter/selectctx_traits.h"
#include "core/queryresults/localqueryresults.h"
#include "tools/logger.h"

namespace reindexer {

template <typename SelectCtx>
RX_ALWAYS_INLINE void logPreSelectState(SelectCtx& ctx, SingleQueryExplainCalc& explain) {
	if constexpr (IsJoinPreSelectCtx<SelectCtx>) {
		std::visit(overloaded{[&](const IdSetPlain& ids) {
								  logFmt(LogInfo, "Built idset preResult (expected {} iterations) with {} ids, q = '{}'",
										 explain.Iterations(), ids.Size(), ctx.query.GetSQL());
							  },
							  [&](const joins::PreSelect::Values& values) {
								  logFmt(LogInfo,
										 "Built values preInitializeSortOrdersResult (expected {} iterations) with {} values, q = '{}'",
										 explain.Iterations(), values.Size(), ctx.query.GetSQL());
							  },
							  [](const SelectIteratorContainer&) { throw_as_assert; }},
				   ctx.preSelect.Result().payload);
	}
}

template <typename SelectCtx>
	requires IsJoinPreSelectCtx<SelectCtx>
RX_ALWAYS_INLINE void explainPutCount(SelectCtx& ctx, SingleQueryExplainCalc& explain, LocalQueryResults&) {
	explain.PutCount(std::visit(overloaded{[](const IdSetPlain& ids) -> size_t { return ids.Size(); },
										   [](const joins::PreSelect::Values& values) { return values.Size(); },
										   [](const SelectIteratorContainer&) -> size_t { throw_as_assert; }},
								ctx.preSelect.Result().payload));
}

template <typename SelectCtx>
	requires(IsJoinSelectCtx<SelectCtx> || IsMainSelectCtx<SelectCtx>)
RX_ALWAYS_INLINE void explainPutCount(SelectCtx&, SingleQueryExplainCalc& explain, LocalQueryResults& results) {
	explain.PutCount(results.Count());
}

template <typename SelectCtx>
	requires IsJoinPreSelectCtx<SelectCtx>
RX_ALWAYS_INLINE void updateExplain(SelectCtx& ctx, SingleQueryExplainCalc& explain) {
	ctx.preSelect.Result().explainPreSelect = explain.GetJSON();
}

template <typename SelectCtx>
	requires(IsJoinSelectCtx<SelectCtx> || IsMainSelectCtx<SelectCtx>)
RX_ALWAYS_INLINE void updateExplain(SelectCtx& ctx, SingleQueryExplainCalc& explain) {
	assertrx_dbg(ctx.explain);
	if (ctx.explain) {
		ctx.explain->Append(explain);
	}
}

}  // namespace reindexer
