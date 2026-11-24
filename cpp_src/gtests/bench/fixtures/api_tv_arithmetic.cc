#include "api_tv_arithmetic.h"

#include <thread>
#include "allocs_tracker.h"
#include "core/query/expression/arithmetic_expression.h"
#include "core/query/query.h"
#include "core/system_ns_names.h"
#include "helpers.h"

using reindexer::IndexOpts;
using reindexer::Query;
using reindexer::expressions::ArithmeticExpression;

namespace reindexer_benchmarks {

ApiTvArithmetic::ApiTvArithmetic(Reindexer* db, std::string_view name, size_t maxItems)
	: BaseFixture(db, name, maxItems), sparseNs_(std::string(name) + "Sparse"), sparseNsDef_(sparseNs_) {
	nsdef_.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("year", "tree", "int", IndexOpts())
		.AddIndex("age", "hash", "int", IndexOpts())
		.AddIndex("start_time", "tree", "int", IndexOpts());

	sparseNsDef_.AddIndex("id", "hash", "int", IndexOpts().PK())
		.AddIndex("year", "tree", "int", IndexOpts().Sparse())
		.AddIndex("age", "hash", "int", IndexOpts().Sparse())
		.AddIndex("start_time", "tree", "int", IndexOpts().Sparse());
}

reindexer::Error ApiTvArithmetic::Initialize() {
	assertrx(db_);
	auto err = db_->AddNamespace(nsdef_);
	if (!err.ok()) {
		return err;
	}
	return db_->AddNamespace(sparseNsDef_);
}

void ApiTvArithmetic::RegisterAllCases() {
	// NOLINTBEGIN(*cplusplus.NewDeleteLeaks)
	Register("Insert" + std::to_string(id_seq_->Count()), &ApiTvArithmetic::Insert, this)->Iterations(1);
	Register("WarmUpIndexes", &ApiTvArithmetic::WarmUpIndexes, this)->Iterations(1);

	const auto exprVsLiteral = [](std::string_view ns) { return Query(ns).Where(ArithmeticExpression("year*2"), CondGe, 4040).Limit(20); };
	const auto singleFieldExprVsLiteral = [](std::string_view ns) {
		return Query(ns).Where(ArithmeticExpression("year"), CondGe, 2020).Limit(20);
	};
	const auto literalVsExpr = [](std::string_view ns) { return Query(ns).Where("year", CondGe, ArithmeticExpression("2020")).Limit(20); };
	const auto exprLiteralVsField = [](std::string_view ns) {
		return Query(ns).Where(ArithmeticExpression("2020"), CondGe, std::string{"year"}).Limit(20);
	};
	const auto literalVsSingleFieldExpr = [](std::string_view ns) {
		return Query(ns).Where(ArithmeticExpression("2020"), CondGe, ArithmeticExpression("year")).Limit(20);
	};
	const auto fieldVsExpr = [](std::string_view ns) {
		return Query(ns).Where("year", CondGe, ArithmeticExpression("start_time")).Limit(20);
	};
	const auto exprVsExpr = [](std::string_view ns) {
		return Query(ns).Where(ArithmeticExpression("year*2"), CondGe, ArithmeticExpression("start_time")).Limit(20);
	};

	registerWithTotals("ExprVsLiteral", exprVsLiteral(nsdef_.name));
	registerWithTotals("ExprVsLiteralSparse", exprVsLiteral(sparseNs_));
	registerWithTotals("SingleFieldExprVsLiteral", singleFieldExprVsLiteral(nsdef_.name));
	registerWithTotals("SingleFieldExprVsLiteralSparse", singleFieldExprVsLiteral(sparseNs_));
	registerWithTotals("LiteralVsExpr", literalVsExpr(nsdef_.name));
	registerWithTotals("LiteralVsExprSparse", literalVsExpr(sparseNs_));
	registerWithTotals("ExprLiteralVsField", exprLiteralVsField(nsdef_.name));
	registerWithTotals("ExprLiteralVsFieldSparse", exprLiteralVsField(sparseNs_));
	registerWithTotals("LiteralVsSingleFieldExpr", literalVsSingleFieldExpr(nsdef_.name));
	registerWithTotals("LiteralVsSingleFieldExprSparse", literalVsSingleFieldExpr(sparseNs_));
	registerWithTotals("FieldVsExpr", fieldVsExpr(nsdef_.name));
	registerWithTotals("FieldVsExprSparse", fieldVsExpr(sparseNs_));
	registerWithTotals("ExprVsExpr", exprVsExpr(nsdef_.name));
	registerWithTotals("ExprVsExprSparse", exprVsExpr(sparseNs_));

	// SortExpression on the same fields. Limit(20) still evaluates the expression
	// for every item (no index); ReqTotal does the same plus a full count.
	registerWithTotals("SortYear", Query(nsdef_.name).Sort("year", SortOrder::Asc).Limit(20));
	registerWithTotals("SortYearMul2", Query(nsdef_.name).Sort("year*2", SortOrder::Asc).Limit(20));
	registerWithTotals("SortYearMul2MinusStart", Query(nsdef_.name).Sort("year*2-start_time", SortOrder::Asc).Limit(20));
	registerWithTotals("SortYearMul2Sparse", Query(sparseNs_).Sort("year*2", SortOrder::Asc).Limit(20));
	// NOLINTEND(*cplusplus.NewDeleteLeaks)
}

void ApiTvArithmetic::registerWithTotals(const std::string& name, const Query& q) {
	RegisterF(name, [this, q](State& state) { benchQuery(q, state); });
	{
		Query total = q;
		ReqTotal::Apply(total);
		RegisterF(name + "Total", [this, total](State& state) { benchQuery(total, state); });
	}
	{
		Query cached = q;
		CachedTotal::Apply(cached);
		RegisterF(name + "CachedTotal", [this, cached](State& state) { benchQuery(cached, state); });
	}
}

reindexer::Item ApiTvArithmetic::MakeItem(benchmark::State&) { return makeItem(nsdef_.name); }

reindexer::Item ApiTvArithmetic::makeItem(std::string_view ns) {
	reindexer::Item item = db_->NewItem(ns);
	std::ignore = item.Unsafe();
	item["id"] = id_seq_->Next();
	item["year"] = random<int>(2000, 2049);
	item["age"] = random<int>(0, 4);
	item["start_time"] = random<int>(0, 50'000);
	return item;
}

void ApiTvArithmetic::Insert(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		id_seq_->Reset();
		for (int i = 0; i < id_seq_->Count(); ++i) {
			auto item = makeItem(nsdef_.name);
			if (!item.Status().ok()) {
				state.SkipWithError(item.Status().what());
			}
			const auto id = item["id"].As<int>();
			const auto year = item["year"].As<int>();
			const auto age = item["age"].As<int>();
			const auto startTime = item["start_time"].As<int>();
			auto err = db_->Insert(nsdef_.name, item);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}

			reindexer::Item sparseItem = db_->NewItem(sparseNs_);
			std::ignore = sparseItem.Unsafe();
			sparseItem["id"] = id;
			sparseItem["year"] = year;
			sparseItem["age"] = age;
			sparseItem["start_time"] = startTime;
			err = db_->Insert(sparseNs_, sparseItem);
			if (!err.ok()) {
				state.SkipWithError(err.what());
			}
			state.SetItemsProcessed(state.items_processed() + 1);
		}
	}
}

void ApiTvArithmetic::waitForOptimization(std::string_view ns) const {
	Query q(reindexer::kMemStatsNamespace);
	q.Where("name", CondEq, ns);
	for (;;) {
		reindexer::QueryResults res;
		auto e = db_->Select(q, res);
		assertrx(e.ok());
		assertrx(res.Count() == 1);
		assertrx(res.IsLocal());
		auto item = res.ToLocalQr().begin().GetItem(false);
		if (item["optimization_completed"].As<bool>() == true) {
			break;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(20));
	}
}

void ApiTvArithmetic::WarmUpIndexes(State& state) {
	AllocsTracker allocsTracker(state);
	for (auto _ : state) {	// NOLINT(*deadcode.DeadStores)
		waitForOptimization(nsdef_.name);
		waitForOptimization(sparseNs_);
	}
}

}  // namespace reindexer_benchmarks
