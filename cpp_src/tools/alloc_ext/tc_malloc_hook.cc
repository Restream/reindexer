#include "tc_malloc_extension.h"

#if REINDEX_WITH_GPERFTOOLS && !defined(_WIN32)
#include <dlfcn.h>
#include <stdlib.h>
#endif	// REINDEX_WITH_GPERFTOOLS

namespace reindexer {
namespace alloc_ext {

#if REINDEX_WITH_GPERFTOOLS && defined(_WIN32)
bool TCMallocHooksAreAvailable() { return true; }
#elif REINDEX_WITH_GPERFTOOLS

using AddNewHookFn = int (*)(MallocHook_NewHook);
using AddDeleteHookFn = int (*)(MallocHook_DeleteHook);

static AddNewHookFn getAddNewHookFn() {
	static auto add_new_hook_fn = reinterpret_cast<AddNewHookFn>(dlsym(RTLD_DEFAULT, "MallocHook_AddNewHook"));
	return add_new_hook_fn;
}

static AddDeleteHookFn getAddDeleteHookFn() {
	static auto add_delete_hook_fn = reinterpret_cast<AddDeleteHookFn>(dlsym(RTLD_DEFAULT, "MallocHook_AddDeleteHook"));
	return add_delete_hook_fn;
}

int MallocHook_AddNewHook(MallocHook_NewHook hook) {
	auto add_new_hook_fn = getAddNewHookFn();
	if (add_new_hook_fn && TCMallocIsAvailable()) {
		return add_new_hook_fn(hook);
	}
	return 1;
}

int MallocHook_AddDeleteHook(MallocHook_DeleteHook hook) {
	auto add_delete_hook_fn = getAddDeleteHookFn();
	if (add_delete_hook_fn && TCMallocIsAvailable()) {
		return add_delete_hook_fn(hook);
	}
	return 1;
}

bool TCMallocHooksAreAvailable() { return (getAddNewHookFn() != nullptr) && (getAddDeleteHookFn() != nullptr); }

#endif	// REINDEX_WITH_GPERFTOOLS

}  // namespace alloc_ext
}  // namespace reindexer
