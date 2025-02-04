#include "errors.h"
#include "debug/backtrace.h"

namespace reindexer {

void print_backtrace_and_abort(std::string&& msg) {
	std::cerr << msg;
	reindexer::debug::print_backtrace(std::cerr, nullptr, -1);
	reindexer::debug::print_crash_query(std::cerr);
	reindexer::debug::backtrace_set_assertion_message(std::move(msg));
	abort();
}

const Error::WhatPtr Error::defaultErrorText_{make_intrusive<Error::WhatT>("Error text generation failed")};

std::ostream& operator<<(std::ostream& os, const Error& error) {
	return os << "{ code: " << error.code() << "; what: \"" << error.what() << "\"}";
}

}  // namespace reindexer
