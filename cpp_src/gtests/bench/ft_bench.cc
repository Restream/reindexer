#include <iostream>

#include <benchmark/benchmark.h>
#include "core/reindexer.h"
#include "tools/fsops.h"
#include "tools/reporter.h"

#include "args/args.hpp"
#include "ft_fixture.h"
#include "ft_merge_limit.h"

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
	auto err = DB->Connect("builtin://" + kStoragePath);
	if (!err.ok()) return err.code();

	FullText ft(DB.get(), "fulltext", kItemsInBenchDataset);

	err = ft.Initialize();
	if (!err.ok()) return err.code();

	::benchmark::Initialize(&argc, argv);
	std::optional<size_t> slowIterationCount;
	std::optional<size_t> fastIterationCount;
	if (argc > 1) {
		try {
			args::ArgumentParser parser("ft_bench additional args");
			args::ValueFlag<size_t> siterCountF(parser, "SITERCOUNT", "iteration count for the slow cases", {"slow_iteration_count"},
												args::Options::Single);
			args::ValueFlag<size_t> fiterCountF(parser, "FITERCOUNT", "iteration count for the fast cases", {"fast_iteration_count"},
												args::Options::Single);
			parser.ParseCLI(argc, argv);
			if (siterCountF) {
				slowIterationCount = args::get(siterCountF);
				argc--;	 // sub argument, otherwise need to rearrange the argv rows
			}
			if (fiterCountF) {
				fastIterationCount = args::get(fiterCountF);
				argc--;	 // sub additional argument, otherwise need to rearrange the argv rows
			}
		} catch (const args::ParseError& e) {
			std::cout << "argument parse error '" << e.what() << "'" << std::endl;
			return 1;
		}
	}
	ft.RegisterAllCases(fastIterationCount, slowIterationCount);

	// Disabled bench for large merge limits
	// FullTextMergeLimit ftMergeLimit(DB.get(), "merge_limit", 100000);
	// err = ftMergeLimit.Initialize();
	// if (!err.ok()) return err.code();
	// ftMergeLimit.RegisterAllCases();

#ifdef _GLIBCXX_DEBUG
	::benchmark::RunSpecifiedBenchmarks();
#else	// #ifdef _GLIBCXX_DEBUG
	benchmark::Reporter reporter;
	::benchmark::RunSpecifiedBenchmarks(&reporter);
#endif	// #ifdef _GLIBCXX_DEBUG

	return 0;
}
