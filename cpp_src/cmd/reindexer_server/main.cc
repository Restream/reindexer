#include <iostream>
#include "debug/backtrace.h"
#include "server/server.h"

int main(int argc, char* argv[]) {
	backtrace_init();
	reindexer_server::Server svc;
	auto err = svc.InitFromCLI(argc, argv);
	if (!err.ok()) {
		std::cerr << err.what() << std::endl;
		return EXIT_FAILURE;
	}
	svc.EnableHandleSignals();
	return svc.Start();
}
