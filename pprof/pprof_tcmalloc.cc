// +build !pprof_jemalloc

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include "backtrace.h"
#include "pprof.h"

void cgo_pprof_init() {
	char* flag = getenv("CGOBACKTRACE");
	if (flag && strlen(flag) > 0) {
		backtrace_init();
	}
}

char* cgo_pprof_get_heapprofile() {
	if (std::getenv("HEAPPROFILE")) {
		return GetHeapProfile();
	}
	std::string profile;
	MallocExtension::instance()->GetHeapSample(&profile);
	return strdup(profile.c_str());
}
char* cgo_pprof_lookup_symbol(void* ptr) { return resolve_symbol(ptr, true); }

int cgo_pprof_start_cpu_profile(char* fname) { return ProfilerStart(fname); }
void cgo_pprof_stop_cpu_profile() { ProfilerStop(); }
