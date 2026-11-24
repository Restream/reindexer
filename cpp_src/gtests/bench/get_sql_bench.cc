#include <benchmark/benchmark.h>

#include <array>
#include <cstddef>
#include <string>
#include "core/query/expression/arithmetic_expression.h"
#include "core/query/query.h"
#include "core/query/query_impl.h"
#include "tools/serilize/wrserializer.h"

namespace reindexer_benchmarks {
namespace {

using reindexer::Query;
using reindexer::expressions::ArithmeticExpression;

constexpr std::array kCases{"PlainFields", "SingleFieldExpression", "ShortExpression",	"LongExpression",	  "ManyExpressions",
							"Functions",   "QuotedKeywords",		"UnquotedKeywords", "SubqueryExpression", "LargeQuery"};

Query makeQuery(size_t index) {
	using namespace reindexer;
	Query query("items");
	query.Limit(20);
	switch (index) {
		case 0:
			return query.Where("id", CondEq, 42).Where("year", CondGe, 2020);
		case 1:
			return query.Where(ArithmeticExpression("year"), CondGe, 2020);
		case 2:
			return query.Where(ArithmeticExpression("year * 2"), CondGe, 4040);
		case 3:
			return query.Where(ArithmeticExpression("(price + tax - discount) * quantity / (rate + 1)"), CondGt, 100);
		case 4:
			return query.Where(ArithmeticExpression("year * 2"), CondGe, ArithmeticExpression("start_time + 1"))
				.Where(ArithmeticExpression("price * quantity - discount"), CondLt, ArithmeticExpression("budget + tax"))
				.Where(ArithmeticExpression("age + 10"), CondGt, ArithmeticExpression("rating / 2"));
		case 5:
			return query.Where(ArithmeticExpression("now() - updated_at"), CondGe, 60)
				.Where(ArithmeticExpression("flat_array_len(tags) * 2"), CondGt, 4);
		case 6:
			return query.Where(ArithmeticExpression(R"(("select" + "order") * 2)"), CondGt, 100);
		case 7:
			return query.Where(ArithmeticExpression("(select + order) * 2"), CondGt, 100);
		case 8:
			return query.Where(ArithmeticExpression("year * 2"), CondGt, Query("archive").Select("max(year)"));
		case 9:
			for (int i = 0; i < 12; ++i) {
				query.Where(ArithmeticExpression("price * quantity - discount"), CondLt, ArithmeticExpression("budget + tax"));
			}
			return query;
		default:
			throw Error(errParams, "Unknown GetSQL benchmark case: {}", index);
	}
}

void queriesPerfStats(benchmark::State& state, size_t index) {
	const Query query = makeQuery(index);
	const auto impl = reindexer::Impl(query);

	// Match ReindexerImpl::Select: fresh serializers and capacity reserved from the normalized SQL.
	for (auto _ : state) {
		reindexer::WrSerializer normalized, nonNormalized;
		impl.GetSQL(normalized, true);
		nonNormalized.Reserve(normalized.Cap());
		impl.GetSQL(nonNormalized, false);
		benchmark::DoNotOptimize(normalized.Slice());
		benchmark::DoNotOptimize(nonNormalized.Slice());
		benchmark::ClobberMemory();
	}
	state.SetItemsProcessed(state.iterations());
}

}  // namespace
}  // namespace reindexer_benchmarks

// NOLINTNEXTLINE(bugprone-exception-escape)
int main(int argc, char** argv) {
	benchmark::Initialize(&argc, argv);
	if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
		return 1;
	}
	for (size_t i = 0; i < reindexer_benchmarks::kCases.size(); ++i) {
		benchmark::RegisterBenchmark((std::string("GetSQL/QueriesPerfStats/") + reindexer_benchmarks::kCases[i]).c_str(),
									 reindexer_benchmarks::queriesPerfStats, i);
	}
	benchmark::RunSpecifiedBenchmarks();
	benchmark::Shutdown();
	return 0;
}
