#include <iostream>
#ifndef _WIN32
#include <signal.h>
#endif // _WIN32

#include <sstream>
#include "debug/backtrace.h"
#include "debug/resolver.h"
#include "estl/string_view.h"

static std::unique_ptr<reindexer::debug::TraceResolver> resolver{nullptr};

struct cgoTracebackArg {
	uintptr_t context;
	uintptr_t sigContext;
	uintptr_t* buf;
	uintptr_t max;
};

struct cgoSymbolizerArg {
	uintptr_t pc;
	const char* file;
	uintptr_t lineno;
	const char* func;
	uintptr_t entry;
	uintptr_t more;
	uintptr_t data;
};

extern "C" void cgoSymbolizer(cgoSymbolizerArg* arg) {
	if (!resolver) resolver = reindexer::debug::TraceResolver::New();
	// Leak it!
	auto* te = new reindexer::debug::TraceEntry(arg->pc);
	if (resolver->Resolve(*te)) {
		arg->file = te->srcFile_.data();
		arg->func = te->funcName_.data();
		arg->lineno = te->srcLine_;
	}
}

#ifdef _WIN32
extern "C" void cgoSignalsInit() {}
#else // !_WIN32
static struct sigaction oldsa[32];

static void cgoSighandler(int sig, siginfo_t* info, void* ucontext) {
	reindexer::debug::print_crash_query(std::cout);
	if (sig < 32) {
        struct sigaction &old = oldsa[sig];
        if (old.sa_flags & SA_SIGINFO) {
            (old.sa_sigaction)(sig, info, ucontext);
        } else {
            (old.sa_handler)(sig);
        }
	} else {
		std::exit(-1);
	}
}

extern "C" void cgoSignalsInit() {
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = cgoSighandler;
	sa.sa_flags = SA_ONSTACK | SA_RESTART | SA_SIGINFO;
	sigaction(SIGSEGV, &sa, &oldsa[SIGSEGV]);
	sigaction(SIGABRT, &sa, &oldsa[SIGABRT]);
	sigaction(SIGBUS, &sa, &oldsa[SIGBUS]);
}
#endif // _WIN32

extern "C" void cgoTraceback(cgoTracebackArg* arg) {
	reindexer::string_view method;
	void* addrlist[64] = {};

	if (arg->context != 0) {
		arg->buf[0] = 0;
		return;
	}
	uintptr_t addrlen = reindexer::debug::backtrace_internal(addrlist, sizeof(addrlist) / sizeof(addrlist[0]),
															 reinterpret_cast<void*>(arg->context), method);
	if (addrlen > 3) memcpy(arg->buf, addrlist + 3, std::min(addrlen - 3, arg->max) * sizeof(void*));
}
