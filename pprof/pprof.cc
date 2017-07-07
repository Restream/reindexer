#include "pprof.h"
#include <gperftools/heap-profiler.h>
#include <gperftools/profiler.h>
#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include "backtrace.h"

void cgo_pprof_init() {
	char* flag = getenv("CGOBACKTRACE");
	if (flag && strlen(flag) > 0) {
		backtrace_init();
	}
}

char* cgo_pprof_get_heapprofile() { return GetHeapProfile(); }
char* cgo_pprof_lookup_symbol(void* ptr) { return resolve_symbol(ptr, true); }

int cgo_pprof_start_cpu_profile(char* fname) { return ProfilerStart(fname); }
void cgo_pprof_stop_cpu_profile() { ProfilerStop(); }
