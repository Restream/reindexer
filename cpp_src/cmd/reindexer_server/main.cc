#include "server/server.h"
#include "debug/backtrace.h"

int main(int argc, char* argv[]) {
	backtrace_init();
	return reindexer_server::Server::main(argc, argv);
}
