#include "terminate_handler.h"
#include <sstream>

#ifndef _WIN32
#include <cxxabi.h>
#endif
#include "debug/backtrace.h"
#include "debug/resolver.h"
#include "tools/errors.h"

namespace reindexer {
namespace debug {

extern std::function<void(std::string_view out)> g_writer;

void terminate_handler() {
	std::ostringstream sout;
	std::exception_ptr exptr = std::current_exception();
	if (exptr) {
#ifndef _WIN32
		const char *type = abi::__cxa_current_exception_type()->name();
		int status;
		const char *demangled = abi::__cxa_demangle(type, NULL, NULL, &status);
		sout << "Terminating with uncaught exception of type " << (demangled ? demangled : type);
#else
		sout << "Terminating with uncaught exception ";
#endif
		try {
			std::rethrow_exception(exptr);
		} catch (std::exception &ex) {
			sout << ": " << ex.what() << std::endl;
		} catch (Error &err) {
			sout << ": " << err.what() << std::endl;
		} catch (...) {
			sout << std::endl;
		}
	}

	print_backtrace(sout, nullptr, -1);
	print_crash_query(sout);
	g_writer(sout.str());
	exit(-1);
}

void terminate_handler_init() { std::set_terminate(&terminate_handler); }

}  // namespace debug

}  // namespace reindexer
