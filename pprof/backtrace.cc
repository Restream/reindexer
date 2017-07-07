#define __STDC_FORMAT_MACROS 1
#include <cxxabi.h>
#include <execinfo.h>
#include <inttypes.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>

char* resolve_symbol(void* addr, bool onlyName) {
	char filename[512], symbol[512], out[512];
	uintptr_t address = (uintptr_t)addr, offset = 0;
	char** symbols = backtrace_symbols(&addr, 1);
	*filename = *symbol = 0;

#if __APPLE__
	int ret =
		sscanf(symbols[0], "%*d%*[ \t]%s%*[ \t]%" SCNxPTR "%*[ \t]%s%*[ \t]+%*[ \t]%" SCNuPTR, filename, &address, symbol, &offset) + 1;
#else
	int ret = sscanf(symbols[0], "%[^(](%[A-Za-z0-9_]+%" SCNxPTR ")%*[ \t][%" SCNxPTR "]", filename, symbol, &offset, &address);
#endif

	free(symbols);
	if (ret < 4 && onlyName) {
		sprintf(out, "%s@%" PRIxPTR, filename, address);
		return strdup(out);
	}

	int status;
	char* demangled = abi::__cxa_demangle(symbol, NULL, NULL, &status);
	if (onlyName) return demangled ? demangled : strdup(symbol);

	snprintf(out, sizeof(out), " %-30s 0x%016" PRIxPTR " %s + %" PRIuPTR, filename, address, demangled ? demangled : symbol, offset);
	free(demangled);
	return strdup(out);
}

static void sighandler(int sig) {
	fprintf(stderr, "\nSignal %d, backtrace:\n", sig);
	void* addrlist[64];
	int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

	for (int i = 0; i < addrlen; i++) {
		char* p = resolve_symbol(addrlist[i], false);
		fprintf(stderr, "%d %s\n", i, p);
		free(p);
	}
	exit(-1);
}

void backtrace_init() {
	signal(SIGSEGV, sighandler);
	signal(SIGABRT, sighandler);
	signal(SIGBUS, sighandler);
}
