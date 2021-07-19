// +build !pprof_jemalloc

#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#include <cstdlib>
#include <cstring>
#include <string>
#include "backtrace.h"
#include "pprof.h"

char* cgo_pprof_get_heapprofile() {
	if (std::getenv("HEAPPROFILE")) {
		return GetHeapProfile();
	}
	std::string profile;
	MallocExtension::instance()->GetHeapSample(&profile);
	return strdup(profile.c_str());
}
char* cgo_pprof_lookup_symbol(void* ptr) { return resolve_symbol(ptr); }
