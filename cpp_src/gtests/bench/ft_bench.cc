#include <iostream>

#include "benchmark/benchmark.h"
#include "core/reindexer.h"
#include "tools/fsops.h"
#include "tools/reporter.h"

#include "ft_fixture.h"

#define STORAGE_PATH "/tmp/reindex/test"

using std::shared_ptr;
using reindexer::Reindexer;

shared_ptr<Reindexer> DB = std::make_shared<Reindexer>();

#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN)
const int kItemsInBenchDataset = 1000;
#else
const int kItemsInBenchDataset = 100000;
#endif

int main(int argc, char** argv) {
	if (reindexer::fs::RmDirAll(STORAGE_PATH) < 0 && errno != ENOENT) {
		std::cerr << "Could not clean working dir '" << STORAGE_PATH << "'.";
		std::cerr << "Reason: " << strerror(errno) << std::endl;

		return 1;
	}
	DB->EnableStorage(STORAGE_PATH);

	FullText ft(DB.get(), "fulltext", kItemsInBenchDataset);

	auto err = ft.Initialize();
	if (!err.ok()) return err.code();

	::benchmark::Initialize(&argc, argv);
	if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;

	ft.RegisterAllCases();

	benchmark::Reporter reporter;
	::benchmark::RunSpecifiedBenchmarks(&reporter);

	return 0;
}
