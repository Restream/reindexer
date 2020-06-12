#include "backtrace.h"
#ifndef WIN32
#include <signal.h>
#include <unistd.h>
#include <sstream>
#include "estl/span.h"
#include "resolver.h"
// There are 3 backtrace methods are available:
// 1. stangalone libunwind ( https://github.com/libunwind/libunwind )
// 2. libgcc's/llvm built in unwind
// 3. GNU's execinfo backtrace() call

#if REINDEX_WITH_LIBUNWIND
#define UNW_LOCAL_ONLY
#include <libunwind.h>
#endif

#if REINDEX_WITH_UNWIND
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <unwind.h>
#endif

#if REINDEX_WITH_EXECINFO
#include <execinfo.h>
#endif

#if REINDEX_OVERRIDE_ABORT
#include <syscall.h>
// Override abort/__assert_fail for musl build for correct backtrace
extern "C" void abort() {
	pid_t tid = syscall(SYS_gettid);
	syscall(SYS_tkill, tid, SIGABRT);
	for (;;) {
	}
}

extern "C" void __assert_fail(const char *expr, const char *file, int line, const char *func) {
	fprintf(stderr, "Assertion failed: %s (%s: %s: %d)\n", expr, file, func, line);
	fflush(NULL);
	abort();
}
#endif

namespace reindexer {
namespace debug {

std::function<void(string_view out)> g_writer = [](string_view sv) { std::cerr << sv; };

#if REINDEX_WITH_UNWIND
class Unwinder {
public:
	size_t operator()(span<void *> trace) {
		trace_ = trace;
		index_ = -1;
		_Unwind_Backtrace(&this->backtrace_trampoline, this);
		return static_cast<size_t>(index_);
	}

private:
	static _Unwind_Reason_Code backtrace_trampoline(_Unwind_Context *ctx, void *self) {
		return (static_cast<Unwinder *>(self))->backtrace(ctx);
	}

	_Unwind_Reason_Code backtrace(_Unwind_Context *ctx) {
		if (index_ >= 0 && size_t(index_) >= trace_.size()) return _URC_END_OF_STACK;

		int ip_before_instruction = 0;
		uintptr_t ip = _Unwind_GetIPInfo(ctx, &ip_before_instruction);

		if (!ip_before_instruction) {
			if (ip == 0) {
				ip = std::numeric_limits<uintptr_t>::max();
			} else {
				ip -= 1;
			}
		}

		if (index_ >= 0) {
			trace_[index_] = reinterpret_cast<void *>(ip);
		}
		index_++;
		return _URC_NO_REASON;
	}
	ssize_t index_;
	span<void *> trace_;
};
#endif

int backtrace_internal(void **addrlist, size_t size, void *ctx, string_view &method) {
	(void)ctx;
	size_t addrlen = 0;
	(void)size;
	(void)method;
	(void)addrlist;

#if REINDEX_WITH_LIBUNWIND
	method = "libunwind"_sv;
	unw_cursor_t cursor;
	unw_context_t uc;

	if (!ctx) {
		unw_getcontext(&uc);
		ctx = &uc;
	}

	unw_init_local(&cursor, reinterpret_cast<unw_context_t *>(ctx));

	addrlen = 1;
	do {
		unw_word_t ip;
		unw_get_reg(&cursor, UNW_REG_IP, &ip);
		addrlist[addrlen++] = reinterpret_cast<void *>(ip);
	} while (unw_step(&cursor) && addrlen < size);
#endif
#if REINDEX_WITH_UNWIND
	Unwinder unw;
	if (addrlen < 3) {
		method = "unwind"_sv;
		addrlen = unw(span<void *>(addrlist, size));
	}
#endif
#if REINDEX_WITH_EXECINFO
	if (addrlen < 3) {
		method = "execinfo"_sv;
		addrlen = ::backtrace(addrlist, size);
	}
#endif
	return addrlen;
}

void inline getBackTraceString(std::ostringstream &sout, void *ctx, int sig) {
#if !REINDEX_WITH_EXECINFO && !REINDEX_WITH_UNWIND && !REINDEX_WITH_LIBUNWIND
	sout << "Sorry, reindexer has been compiled without any backtrace methods." << std::endl;
#else
	void *addrlist[64] = {};
	auto resolver = TraceResolver::New();
	string_view method;
	int addrlen = backtrace_internal(addrlist, sizeof(addrlist) / sizeof(addrlist[0]), ctx, method);

	sout << "Signal " << sig << " backtrace (" << method << "):" << std::endl;
	for (int i = 1; i < addrlen; i++) {
		auto te = TraceEntry(uintptr_t(addrlist[i]));
		resolver->Resolve(te);
		sout << " #" << i << " " << te << std::endl;
	}
#endif
}

static void sighandler(int sig, siginfo_t *, void *ctx) {
	std::ostringstream sout;
	getBackTraceString(sout, ctx, sig);
	g_writer(sout.str());

	raise(sig);
	exit(-1);
}

void backtrace_init() {
	struct sigaction sa;
	memset(&sa, 0, sizeof(sa));
	sa.sa_sigaction = sighandler;
	sa.sa_flags = SA_SIGINFO | SA_NODEFER | SA_RESETHAND;
	sigaction(SIGSEGV, &sa, nullptr);
	sigaction(SIGABRT, &sa, nullptr);
	sigaction(SIGBUS, &sa, nullptr);
}
void backtrace_set_writer(std::function<void(string_view out)> writer) { g_writer = writer; }

}  // namespace debug
}  // namespace reindexer

#else
namespace reindexer {
namespace debug {
void backtrace_init() {}
void backtrace_set_writer(std::function<void(string_view out)>) {}
int backtrace_internal(void **, size_t, void *, string_view &) { return 0; }
void getBackTraceString(std::ostringstream &, void *, int) {}

}  // namespace debug
}  // namespace reindexer

#endif
