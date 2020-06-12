#include "terminate_handler.h"
#include <sstream>

#include "debug/backtrace.h"
#include "debug/resolver.h"
#include "tools/fsops.h"

#if REINDEX_WITH_LIBUNWIND
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#endif

namespace reindexer {

void terminate_handler() {
	std::ostringstream sout;
	debug::getBackTraceString(sout, nullptr, -1);
	std::string traceString(sout.str());
	FILE* fp = fopen(fs::JoinPath(fs::GetTempDir(), "crash_reindexer.log").c_str(), "wb");
	if (fp) {
		fwrite(traceString.c_str(), 1, traceString.size(), fp);
		fclose(fp);
	}
	std::cerr << traceString << std::endl;
	std::abort();
}

SetTerminateHandler::SetTerminateHandler() {
	//
	std::set_terminate(&terminate_handler);
}

}  // namespace reindexer
