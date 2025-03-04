#include "terminate_handler.h"
#include <iostream>
#include <sstream>

#ifndef _WIN32
#include <cxxabi.h>
#endif
#include "debug/backtrace.h"

namespace reindexer {
namespace debug {

static void terminate_handler() {
	std::ostringstream sout;
	std::exception_ptr exptr = std::current_exception();
	if (exptr) {
#ifndef _WIN32
		const char* type = abi::__cxa_current_exception_type()->name();
		int status;
		const char* demangled = abi::__cxa_demangle(type, NULL, NULL, &status);
		sout << "*** Terminating with uncaught exception of type " << (demangled ? demangled : type);
#else
		sout << "*** Terminating with uncaught exception ";
#endif
		try {
			std::rethrow_exception(exptr);
		} catch (std::exception& ex) {
			sout << ": " << ex.what();
		} catch (...) {
			sout << ": <unknown exception>";
		}
		sout << " ***" << std::endl;
	} else {
		sout << "*** Backtrace on terminate call ***" << std::endl;
	}
	const auto writer = backtrace_get_writer();
	writer(sout.str());
	std::cerr << "Reindexer was terminated by terminate()-call. STDERR info:\n";
	std::cerr << sout.str() << std::endl;
	sout.str(std::string());
	sout.clear();
	print_crash_query(sout);
	writer(sout.str());
	sout.str(std::string());
	sout.clear();
	print_backtrace(sout, nullptr, -1);
	writer(sout.str());
	std::cerr << sout.str() << std::endl;
	exit(-1);
}

void terminate_handler_init() { std::set_terminate(&terminate_handler); }

}  // namespace debug

}  // namespace reindexer
