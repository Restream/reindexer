#include "je_malloc_extension.h"

#if REINDEX_WITH_JEMALLOC
#include <jemalloc/jemalloc.h>
#ifndef _WIN32
#include <dlfcn.h>
#include <stdlib.h>
#endif	// _WIN32
#endif	// REINDEX_WITH_JEMALLOC && !defined(_WIN32)

namespace reindexer {
namespace alloc_ext {

#if REINDEX_WITH_JEMALLOC && defined(_WIN32)

int mallctl(const char* /*name*/, void* /*oldp*/, size_t* /*oldlenp*/, void* /*newp*/, size_t /*newlen*/) { return -1; }

bool JEMallocIsAvailable() { return true; }

#elif REINDEX_WITH_JEMALLOC

using MallctlFn = int (*)(const char* name, void* oldp, size_t* oldlenp, void* newp, size_t newlen);

static MallctlFn getMallctlFn() {
	static auto getInstanceFn = reinterpret_cast<MallctlFn>(dlsym(RTLD_DEFAULT, "mallctl"));
	return getInstanceFn;
}

int mallctl(const char* name, void* oldp, size_t* oldlenp, void* newp, size_t newlen) {
	auto getInstanceFn = getMallctlFn();
	if (getInstanceFn) {
		return getInstanceFn(name, oldp, oldlenp, newp, newlen);
	}
	return -1;
}

bool JEMallocIsAvailable() { return (getMallctlFn() != nullptr); }

#else
// suppress clang warning
int ___je_malloc_extension_dummy_suppress_warning;
#endif	// REINDEX_WITH_JEMALLOC

}  // namespace alloc_ext
}  // namespace reindexer
