#include <iostream>

#include "benchmark/benchmark.h"

#include "api_tv_composite.h"
#include "api_tv_simple.h"
#include "join_items.h"

#include "tools/fsops.h"

#include "core/reindexer.h"

#define kStoragePath "/tmp/reindex/test"
#define kConectStr "builtin:///tmp/reindex/test"

using std::shared_ptr;
using reindexer::Reindexer;

#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN)
const int kItemsInBenchDataset = 5000;
#else
const int kItemsInBenchDataset = 500000;
#endif

int main(int argc, char** argv) {
	if (reindexer::fs::RmDirAll(kStoragePath) < 0 && errno != ENOENT) {
		std::cerr << "Could not clean working dir '" << kStoragePath << "'.";
		std::cerr << "Reason: " << strerror(errno) << std::endl;

		return 1;
	}

	shared_ptr<Reindexer> DB = std::make_shared<Reindexer>();
	DB->Connect(kConectStr);

	JoinItems joinItems(DB.get(), 500);
	ApiTvSimple apiTvSimple(DB.get(), "ApiTvSimple", kItemsInBenchDataset);
	ApiTvComposite apiTvComposite(DB.get(), "ApiTvComposite", kItemsInBenchDataset);

	auto err = apiTvSimple.Initialize();
	if (!err.ok()) return err.code();

	err = joinItems.Initialize();
	if (!err.ok()) return err.code();

	err = apiTvComposite.Initialize();
	if (!err.ok()) return err.code();

	::benchmark::Initialize(&argc, argv);
	if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

	joinItems.RegisterAllCases();
	apiTvSimple.RegisterAllCases();
	apiTvComposite.RegisterAllCases();

	::benchmark::RunSpecifiedBenchmarks();
}
