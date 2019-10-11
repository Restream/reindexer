#define __STDC_FORMAT_MACROS 1
#include <cxxabi.h>
#include <execinfo.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>

char* resolve_symbol(void* addr) {
	char filename[512], symbol[512], out[1024];
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
	if (ret < 4) {
		snprintf(out, sizeof(out), "%s@%" PRIxPTR, filename, address);
		return strdup(out);
	}

	int status;
	char* demangled = abi::__cxa_demangle(symbol, NULL, NULL, &status);
	return demangled ? demangled : strdup(symbol);
}
