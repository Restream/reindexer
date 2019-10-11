

#include "debug/backtrace.h"
#include "debug/resolver.h"

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

extern "C" void cgoTraceback(cgoTracebackArg* arg) {
	reindexer::string_view method;
	void* addrlist[64] = {};
	uintptr_t addrlen = reindexer::debug::backtrace_internal(addrlist, sizeof(addrlist) / sizeof(addrlist[0]),
															 reinterpret_cast<void*>(arg->context), method);
	if (addrlen > 3) memcpy(arg->buf, addrlist + 3, std::min(addrlen - 3, arg->max) * sizeof(void*));
}
