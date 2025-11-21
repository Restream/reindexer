#include <benchmark/benchmark.h>

#include "aggregation.h"
#include "api_encdec.h"
#include "api_tv_composite.h"
#include "api_tv_simple.h"
#include "api_tv_simple_comparators.h"
#include "api_tv_simple_sparse.h"
#include "equalpositions.h"
#include "geometry.h"
#include "join_items.h"

#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN)
const int kItemsInBenchDataset = 5'000;
const int kItemsInComparatorsBenchDataset = 5'000;
#elif defined(RX_WITH_STDLIB_DEBUG)
const int kItemsInBenchDataset = 50'000;
const int kItemsInComparatorsBenchDataset = 30'000;
#else
const int kItemsInBenchDataset = 500'000;
const int kItemsInComparatorsBenchDataset = 100'000;
#endif

// NOLINTNEXTLINE (bugprone-exception-escape) Get stacktrace is probably better, than generic error-message
int main(int argc, char** argv) {
	using namespace std::string_view_literals;

	auto DB = InitBenchDB("bench_test"sv);

	JoinItems joinItems(DB.get(), 50'000);
	ApiTvSimple apiTvSimple(DB.get(), "ApiTvSimple"sv, kItemsInBenchDataset);
	ApiTvSimpleComparators apiTvSimpleComparators(DB.get(), "ApiTvSimpleComparators"sv, kItemsInComparatorsBenchDataset);
	ApiTvSimpleSparse apiTvSimpleSparse(DB.get(), "ApiTvSimpleSparse"sv, kItemsInBenchDataset);
	ApiTvComposite apiTvComposite(DB.get(), "ApiTvComposite"sv, kItemsInBenchDataset);
	Geometry geometry(DB.get(), "Geometry"sv, kItemsInBenchDataset);
	Aggregation aggregation(DB.get(), "Aggregation"sv, kItemsInBenchDataset);
	ApiEncDec decoding(DB.get(), "EncDec");
	EqualPositions equalPosition(DB.get(), "EqualPositions", kItemsInBenchDataset);

	auto err = apiTvSimple.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = apiTvSimpleComparators.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = apiTvSimpleSparse.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = joinItems.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = apiTvComposite.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = geometry.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = aggregation.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = decoding.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	err = equalPosition.Initialize();
	if (!err.ok()) {
		return err.code();
	}

	::benchmark::Initialize(&argc, argv);
	if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
		return 1;
	}

	joinItems.RegisterAllCases();
	apiTvSimple.RegisterAllCases();
	apiTvSimpleComparators.RegisterAllCases();
	apiTvSimpleSparse.RegisterAllCases();
	apiTvComposite.RegisterAllCases();
	geometry.RegisterAllCases();
	aggregation.RegisterAllCases();
	decoding.RegisterAllCases();
	equalPosition.RegisterAllCases();

	::benchmark::RunSpecifiedBenchmarks();
}
