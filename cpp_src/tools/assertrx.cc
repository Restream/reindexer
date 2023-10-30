#include "assertrx.h"
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include "debug/backtrace.h"
#include "spdlog/fmt/bundled/printf.h"
#include "spdlog/fmt/fmt.h"
#include "tools/errors.h"

namespace reindexer {

void fail_assertrx(const char *assertion, const char *file, unsigned line, const char *function) noexcept {
	std::cerr << fmt::sprintf("Assertion failed: %s (%s:%u: %s)\n", assertion, file, line, function);
	debug::print_crash_query(std::cerr);
	std::abort();
}

void fail_throwrx(const char *assertion, const char *file, unsigned line, const char *function) {
	std::string errText{fmt::sprintf("Assertion failed (handled via exception): %s (%s:%u: %s)\n", assertion, file, line, function)};
	std::cerr << errText;
	debug::print_crash_query(std::cerr);
	throw Error{errAssert, std::move(errText)};
}

}  // namespace reindexer
