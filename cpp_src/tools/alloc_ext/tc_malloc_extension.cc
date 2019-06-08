#include "tc_malloc_extension.h"

#if REINDEX_WITH_GPERFTOOLS && defined(_WIN32)
bool tc_malloc_available() { return true; }
#elif REINDEX_WITH_GPERFTOOLS
#include <dlfcn.h>
#include <gperftools/malloc_extension.h>
#include <stdlib.h>
#include <mutex>

namespace {

using GetInstanceFn = MallocExtension* (*)();

static GetInstanceFn getGetInstanceFn() {
	static auto get_instance_fn = reinterpret_cast<GetInstanceFn>(dlsym(RTLD_NEXT, "_ZN15MallocExtension8instanceEv"));
	return get_instance_fn;
}
}  // namespace

MallocExtension* WEAK_ATTR MallocExtension::instance() {
	auto get_instance_fn = getGetInstanceFn();
	if (get_instance_fn) {
		return get_instance_fn();
	}
	return nullptr;
}

bool tc_malloc_available() { return (getGetInstanceFn() != nullptr); }
#else
bool tc_malloc_available() { return false; }
#endif
