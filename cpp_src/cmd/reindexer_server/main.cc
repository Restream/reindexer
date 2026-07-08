#include <iostream>
#include "debug/backtrace.h"
#include "server/server.h"
#include "spdlog/spdlog.h"
#include "tools/cpucheck.h"

// NOLINTNEXTLINE (bugprone-exception-escape) Get stacktrace is probably better, than generic error-message
int main(int argc, char* argv[]) {
	reindexer::debug::backtrace_init();

	try {
		reindexer::CheckRequiredSSESupport();
	} catch (std::exception& err) {
		std::cerr << err.what();
		return EXIT_FAILURE;
	}

	reindexer_server::Server svc(reindexer_server::ServerMode::Standalone);
	auto err = svc.InitFromCLI(argc, argv);
	if (!err.ok()) {
		std::cerr << err.what() << std::endl;
		return EXIT_FAILURE;
	}
#if defined(_WIN32) && defined(REINDEX_WITH_CPPTRACE)
	reindexer::debug::set_minidump_path(svc.GetCoreLogPath());
#endif

	svc.EnableHandleSignals();
	int ret = svc.Start();
	spdlog::shutdown();
	return ret;
}
