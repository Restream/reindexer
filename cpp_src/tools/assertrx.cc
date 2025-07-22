#include "assertrx.h"
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include "debug/backtrace.h"
#include "fmt/format.h"
#include "tools/errors.h"

namespace reindexer {

// NOLINTNEXTLINE(bugprone-exception-escape) Exception does not matter here - we are going to crash anyway
void fail_assertrx(const char* assertion, const char* file, unsigned line, const char* function) noexcept {
	auto msg = fmt::format("Assertion failed: {} ({}:{}: {})", assertion, file, line, function);
	std::cerr << msg << "\n";
	debug::backtrace_set_assertion_message(std::move(msg));
	debug::print_crash_query(std::cerr);
	std::abort();
}

void fail_throwrx(const char* assertion, const char* file, unsigned line, const char* function) {
	std::string errText{fmt::format("Assertion failed (handled via exception): {} ({}:{}: {})\n", assertion, file, line, function)};
	std::cerr << errText;
	debug::print_crash_query(std::cerr);
	throw Error{errAssert, std::move(errText)};
}

}  // namespace reindexer
