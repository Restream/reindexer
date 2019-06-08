#include "gperf_profiler.h"

#if REINDEX_WITH_GPERFTOOLS && defined(_WIN32)
bool gperf_profiler_is_available() { return true; }
#elif REINDEX_WITH_GPERFTOOLS
#include <dlfcn.h>
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#include <stdlib.h>
#include <mutex>
#include "tools/alloc_ext/tc_malloc_extension.h"

namespace {

using ProfilerRegisterThreadFn = void (*)();
using ProfilerStartFn = int (*)(const char *);
using ProfilerStopFn = void (*)();
using GetHeapProfileFn = char *(*)();

static ProfilerRegisterThreadFn getProfilerRegisterThreadFn() {
	static auto profiler_register_thread_fn = reinterpret_cast<ProfilerRegisterThreadFn>(dlsym(RTLD_NEXT, "ProfilerRegisterThread"));
	return profiler_register_thread_fn;
}

static ProfilerStartFn getProfilerStartFn() {
	static auto profiler_start_fn = reinterpret_cast<ProfilerStartFn>(dlsym(RTLD_NEXT, "ProfilerStart"));
	return profiler_start_fn;
}

static ProfilerStopFn getProfilerStopFn() {
	static auto profiler_stop_fn = reinterpret_cast<ProfilerStopFn>(dlsym(RTLD_NEXT, "ProfilerStop"));
	return profiler_stop_fn;
}

static GetHeapProfileFn getGetHeapProfileFn() {
	static auto get_heap_profile_fn = reinterpret_cast<GetHeapProfileFn>(dlsym(RTLD_NEXT, "GetHeapProfile"));
	return get_heap_profile_fn;
}
}  // namespace

void WEAK_ATTR ProfilerRegisterThread() {
	auto profiler_register_thread_fn = getProfilerRegisterThreadFn();
	if (profiler_register_thread_fn) {
		profiler_register_thread_fn();
	}
	return;
}

int WEAK_ATTR ProfilerStart(const char *fname) {
	auto profiler_start_fn = getProfilerStartFn();
	if (profiler_start_fn) {
		return profiler_start_fn(fname);
	}
	return 1;
}

void WEAK_ATTR ProfilerStop() {
	auto profiler_stop_fn = getProfilerStopFn();
	if (profiler_stop_fn) {
		profiler_stop_fn();
	}
	return;
}

char *WEAK_ATTR GetHeapProfile() {
	auto get_heap_profile_fn = getGetHeapProfileFn();
	if (get_heap_profile_fn) {
		return get_heap_profile_fn();
	}
	return nullptr;
}

bool gperf_profiler_is_available() {
	return tc_malloc_available() && (getProfilerRegisterThreadFn() != nullptr) && (getProfilerStartFn() != nullptr) &&
		   (getProfilerStopFn() != nullptr) && (getGetHeapProfileFn() != nullptr);
}
#else
bool gperf_profiler_is_available() { return false; }
#endif
