#include "gperf_profiler.h"

#if REINDEX_WITH_GPERFTOOLS
#include <gperftools/heap-profiler.h>
#include <gperftools/malloc_extension.h>
#include <gperftools/profiler.h>
#ifndef _WIN32
#include <dlfcn.h>
#include <stdlib.h>
#include "tools/alloc_ext/tc_malloc_extension.h"
#endif	// _WIN32

#ifdef _WIN32

void ProfilerRegisterThread() { ::ProfilerRegisterThread(); }

int ProfilerStart(const char* fname) { return ::ProfilerStart(); }

void ProfilerStop() { ::ProfilerStop(); }

char* GetHeapProfile() { return ::GetHeapProfile(); }

bool GperfProfilerIsAvailable() { return true; }

#else  // _WIN32

namespace reindexer_server {
namespace pprof {

using ProfilerRegisterThreadFn = void (*)();
using ProfilerStartFn = int (*)(const char*);
using ProfilerStopFn = void (*)();
using GetHeapProfileFn = char* (*)();

static ProfilerRegisterThreadFn getProfilerRegisterThreadFn() {
	static auto profiler_register_thread_fn = reinterpret_cast<ProfilerRegisterThreadFn>(dlsym(RTLD_DEFAULT, "ProfilerRegisterThread"));
	return profiler_register_thread_fn;
}

static ProfilerStartFn getProfilerStartFn() {
	static auto profiler_start_fn = reinterpret_cast<ProfilerStartFn>(dlsym(RTLD_DEFAULT, "ProfilerStart"));
	return profiler_start_fn;
}

static ProfilerStopFn getProfilerStopFn() {
	static auto profiler_stop_fn = reinterpret_cast<ProfilerStopFn>(dlsym(RTLD_DEFAULT, "ProfilerStop"));
	return profiler_stop_fn;
}

static GetHeapProfileFn getGetHeapProfileFn() {
	static auto get_heap_profile_fn = reinterpret_cast<GetHeapProfileFn>(dlsym(RTLD_DEFAULT, "GetHeapProfile"));
	return get_heap_profile_fn;
}

void ProfilerRegisterThread() {
	auto profiler_register_thread_fn = getProfilerRegisterThreadFn();
	if (profiler_register_thread_fn) {
		profiler_register_thread_fn();
	}
	return;
}

int ProfilerStart(const char* fname) {
	auto profiler_start_fn = getProfilerStartFn();
	if (profiler_start_fn) {
		return profiler_start_fn(fname);
	}
	return 1;
}

void ProfilerStop() {
	auto profiler_stop_fn = getProfilerStopFn();
	if (profiler_stop_fn) {
		profiler_stop_fn();
	}
	return;
}

char* GetHeapProfile() {
	auto get_heap_profile_fn = getGetHeapProfileFn();
	if (get_heap_profile_fn) {
		return get_heap_profile_fn();
	}
	return nullptr;
}

bool GperfProfilerIsAvailable() {
	return reindexer::alloc_ext::TCMallocIsAvailable() && (getProfilerRegisterThreadFn() != nullptr) && (getProfilerStartFn() != nullptr) &&
		   (getProfilerStopFn() != nullptr) && (getGetHeapProfileFn() != nullptr);
}

#endif	//_WIN32

}  // namespace pprof
}  // namespace reindexer_server

#endif	// REINDEX_WITH_GPERFTOOLS
