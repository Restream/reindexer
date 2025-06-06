#include <iostream>

#include <benchmark/benchmark.h>

#include "aggregation.h"
#include "api_encdec.h"
#include "api_tv_composite.h"
#include "api_tv_simple.h"
#include "api_tv_simple_comparators.h"
#include "api_tv_simple_sparse.h"
#include "geometry.h"
#include "join_items.h"
#include "tools/reporter.h"

#include "tools/fsops.h"

#include "core/reindexer.h"

using std::shared_ptr;
using reindexer::Reindexer;

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
	const auto storagePath = reindexer::fs::JoinPath(reindexer::fs::GetTempDir(), "reindex/bench_test");
	if (reindexer::fs::RmDirAll(storagePath) < 0 && errno != ENOENT) {
		std::cerr << "Could not clean working dir '" << storagePath << "'.";
		std::cerr << "Reason: " << strerror(errno) << std::endl;

		return 1;
	}

	auto DB = std::make_shared<Reindexer>();
	auto err = DB->Connect("builtin://" + storagePath);
	if (!err.ok()) {
		return err.code();
	}

	JoinItems joinItems(DB.get(), 50'000);
	ApiTvSimple apiTvSimple(DB.get(), "ApiTvSimple", kItemsInBenchDataset);
	ApiTvSimpleComparators apiTvSimpleComparators(DB.get(), "ApiTvSimpleComparators", kItemsInComparatorsBenchDataset);
	ApiTvSimpleSparse apiTvSimpleSparse(DB.get(), "ApiTvSimpleSparse", kItemsInBenchDataset);
	ApiTvComposite apiTvComposite(DB.get(), "ApiTvComposite", kItemsInBenchDataset);
	Geometry geometry(DB.get(), "Geometry", kItemsInBenchDataset);
	Aggregation aggregation(DB.get(), "Aggregation", kItemsInBenchDataset);
	ApiEncDec decoding(DB.get(), "EncDec");

	err = apiTvSimple.Initialize();
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

	::benchmark::RunSpecifiedBenchmarks();
}
