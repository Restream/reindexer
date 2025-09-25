
#include <time.h>
#include "debug/backtrace.h"
#include "gtest/gtest.h"
#include "tools/fsops.h"

// NOLINTNEXTLINE (bugprone-exception-escape) Get stacktrace is probably better, than generic error-message
int main(int argc, char* argv[]) {
	srand(time(NULL));
	::testing::InitGoogleTest(&argc, argv);

	reindexer::debug::backtrace_init();

#ifndef _WIN32
	const char* tmpDir = getenv("REINDEXER_TEST_DB_ROOT");
	if (tmpDir && *tmpDir) {
		reindexer::fs::SetTempDir(std::string(tmpDir));
	}
#endif	// _WIN32

	return RUN_ALL_TESTS();
}
