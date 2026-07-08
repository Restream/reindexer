#include "tc_malloc_extension.h"

#if REINDEX_WITH_GPERFTOOLS && !defined(_WIN32)
#include <dlfcn.h>
#include <stdlib.h>
#endif	// REINDEX_WITH_GPERFTOOLS && !defined(_WIN32)

namespace reindexer {
namespace alloc_ext {

#if REINDEX_WITH_GPERFTOOLS && defined(_WIN32)

MallocExtension* instance() { return MallocExtension::instance(); }
bool TCMallocIsAvailable() { return true; }

#elif REINDEX_WITH_GPERFTOOLS

using GetInstanceFn = MallocExtension* (*)();

static GetInstanceFn getGetInstanceFn() {
	static auto get_instance_fn = reinterpret_cast<GetInstanceFn>(dlsym(RTLD_DEFAULT, "_ZN15MallocExtension8instanceEv"));
	return get_instance_fn;
}

MallocExtension* instance() {
	auto get_instance_fn = getGetInstanceFn();
	if (get_instance_fn) {
		return get_instance_fn();
	}
	return nullptr;
}

bool TCMallocIsAvailable() { return (getGetInstanceFn() != nullptr); }

#endif	// REINDEX_WITH_GPERFTOOLS

}  // namespace alloc_ext
}  // namespace reindexer
