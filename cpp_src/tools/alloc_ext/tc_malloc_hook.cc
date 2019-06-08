#if REINDEX_WITH_GPERFTOOLS && defined(_WIN32)
bool tc_malloc_hooks_available() { return true; }
#elif REINDEX_WITH_GPERFTOOLS
#include <dlfcn.h>
#include <gperftools/malloc_hook_c.h>
#include <stdlib.h>
#include <mutex>
#include "tc_malloc_extension.h"

namespace {

using AddNewHookFn = int (*)(MallocHook_NewHook);
using AddDeleteHookFn = int (*)(MallocHook_DeleteHook);

static AddNewHookFn getAddNewHookFn() {
	static auto add_new_hook_fn = reinterpret_cast<AddNewHookFn>(dlsym(RTLD_NEXT, "MallocHook_AddNewHook"));
	return add_new_hook_fn;
}

static AddDeleteHookFn getAddDeleteHookFn() {
	static auto add_delete_hook_fn = reinterpret_cast<AddDeleteHookFn>(dlsym(RTLD_NEXT, "MallocHook_AddDeleteHook"));
	return add_delete_hook_fn;
}
}  // namespace

int WEAK_ATTR MallocHook_AddNewHook(MallocHook_NewHook hook) {
	auto add_new_hook_fn = getAddNewHookFn();
	if (add_new_hook_fn && tc_malloc_available()) {
		return add_new_hook_fn(hook);
	}
	return 1;
}

int WEAK_ATTR MallocHook_AddDeleteHook(MallocHook_DeleteHook hook) {
	auto add_delete_hook_fn = getAddDeleteHookFn();
	if (add_delete_hook_fn && tc_malloc_available()) {
		return add_delete_hook_fn(hook);
	}
	return 1;
}

bool tc_malloc_hooks_available() { return (getAddNewHookFn() != nullptr) && (getAddDeleteHookFn() != nullptr); }
#else
bool tc_malloc_hooks_available() { return false; }
#endif
