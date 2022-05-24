#include "assertrx.h"
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include "debug/backtrace.h"
#include "spdlog/fmt/bundled/printf.h"
#include "spdlog/fmt/fmt.h"

namespace reindexer {

void fail_assertrx(const char *assertion, const char *file, unsigned line, const char *function) {
	std::cerr << fmt::sprintf("Assertion failed: %s (%s:%u: %s)\n", assertion, file, line, function);
	debug::print_crash_query(std::cerr);
	std::abort();
}

}  // namespace reindexer
