#include <iostream>
#include "debug/backtrace.h"
#include "server/server.h"
#include "spdlog/spdlog.h"
#include "tools/cpucheck.h"

int main(int argc, char* argv[]) {
	reindexer::debug::backtrace_init();

	reindexer::CheckRequiredSSESupport();

	reindexer_server::Server svc(reindexer_server::ServerMode::Standalone);
	auto err = svc.InitFromCLI(argc, argv);
	if (!err.ok()) {
		std::cerr << err.what() << std::endl;
		return EXIT_FAILURE;
	}
	svc.EnableHandleSignals();
	int ret = svc.Start();
	spdlog::drop_all();
	return ret;
}
