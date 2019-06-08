#include "je_malloc_extension.h"

#if REINDEX_WITH_JEMALLOC && defined(_WIN32)
bool je_malloc_available() { return true; }
#elif REINDEX_WITH_JEMALLOC
#include <dlfcn.h>
#include <jemalloc/jemalloc.h>
#include <stdlib.h>
#include <mutex>

namespace {

using mallctl_fn = int (*)(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen);

static mallctl_fn get_mallctl_fn() {
	static auto get_instance_fn = reinterpret_cast<mallctl_fn>(dlsym(RTLD_NEXT, "mallctl"));
	return get_instance_fn;
}
}  // namespace

int WEAK_ATTR mallctl(const char *name, void *oldp, size_t *oldlenp, void *newp, size_t newlen) {
	auto get_instance_fn = get_mallctl_fn();
	if (get_instance_fn) {
		return get_instance_fn(name, oldp, oldlenp, newp, newlen);
	}
	return -1;
}

bool je_malloc_available() { return (get_mallctl_fn() != nullptr); }
#else
bool je_malloc_available() { return false; }
#endif
