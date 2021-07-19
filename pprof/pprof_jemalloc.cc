// +build pprof_jemalloc

#include <jemalloc/jemalloc.h>
#include <memory.h>
#include <stdio.h>
#include <unistd.h>
#include <cstring>
#include "backtrace.h"
#include "pprof.h"

char* cgo_pprof_get_heapprofile() {
	size_t val = 0, sz = sizeof(size_t);
	mallctl("config.prof", &val, &sz, NULL, 0);
	if (!val) {
		return strdup(
			"**** Error:\nJemalloc compiled without heap profiler.\nTo enable profiling you should reconfigure jemalloc with "
			"'--enable-prof'\n");
	}

	mallctl("opt.prof", &val, &sz, NULL, 0);
	if (!val) {
		return strdup("**** Error:\nJemalloc heap profiler is turned off.\nExport MALLOC_CONF=\"prof:true\" to enable it\n");
	}

	const char* fileName = "/tmp/samples.heap";
	unlink(fileName);
	mallctl("prof.dump", NULL, NULL, &fileName, sizeof(fileName));

	FILE* f = fopen(fileName, "r");
	if (!f) {
		return 0;
	}

	fseek(f, 0, SEEK_END);
	sz = ftell(f);
	char* profile = reinterpret_cast<char*>(malloc(sz + 1));
	fseek(f, 0, SEEK_SET);
	size_t nread = fread(profile, 1, sz, f);
	fclose(f);
	profile[sz] = 0;
	return profile;
}

char* cgo_pprof_lookup_symbol(void* ptr) { return resolve_symbol(ptr); }
