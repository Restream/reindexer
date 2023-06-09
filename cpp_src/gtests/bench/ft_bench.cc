#include <iostream>

#include <benchmark/benchmark.h>
#include "core/reindexer.h"
#include "tools/fsops.h"
#include "tools/reporter.h"

#include "args/args.hpp"
#include "ft_fixture.h"

const std::string kStoragePath = "/tmp/reindex/ft_bench_test";

using std::shared_ptr;
using reindexer::Reindexer;

#if defined(REINDEX_WITH_ASAN) || defined(REINDEX_WITH_TSAN)
const int kItemsInBenchDataset = 1'000;
#elif defined(RX_WITH_STDLIB_DEBUG)
const int kItemsInBenchDataset = 10'000;
#else
const int kItemsInBenchDataset = 100'000;
#endif

int main(int argc, char** argv) {
	if (reindexer::fs::RmDirAll(kStoragePath) < 0 && errno != ENOENT) {
		std::cerr << "Could not clean working dir '" << kStoragePath << "'.";
		std::cerr << "Reason: " << strerror(errno) << std::endl;

		return 1;
	}

	shared_ptr<Reindexer> DB = std::make_shared<Reindexer>();
	DB->Connect("builtin://" + kStoragePath);

	FullText ft(DB.get(), "fulltext", kItemsInBenchDataset);

	auto err = ft.Initialize();
	if (!err.ok()) return err.code();

	::benchmark::Initialize(&argc, argv);
	size_t iterationCount = -1;	 // count is determined by the execution time
	if (argc > 1) {
		try {
			args::ArgumentParser parser("ft_bench additional args");
			args::ValueFlag<size_t> iterCountF(parser, "ITERCOUNT", "iteration count", {"iteration_count"}, args::Options::Single);
			parser.ParseCLI(argc, argv);
			if (iterCountF) {
				iterationCount = args::get(iterCountF);
				argc--;	 // only one additional argument, otherwise need to rearrange the argv rows
			}
		} catch (const args::ParseError& e) {
			std::cout << "argument parse error '" << e.what() << "'" << std::endl;
			return 1;
		}
	}
	ft.RegisterAllCases(iterationCount);

#ifdef _GLIBCXX_DEBUG
	::benchmark::RunSpecifiedBenchmarks();
#else	// #ifdef _GLIBCXX_DEBUG
	benchmark::Reporter reporter;
	::benchmark::RunSpecifiedBenchmarks(&reporter);
#endif	// #ifdef _GLIBCXX_DEBUG

	return 0;
}
