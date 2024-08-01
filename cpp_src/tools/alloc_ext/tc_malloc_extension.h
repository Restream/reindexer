#pragma once

#if REINDEX_WITH_GPERFTOOLS
#include <gperftools/malloc_extension.h>
#include <gperftools/malloc_hook_c.h>

namespace reindexer {
namespace alloc_ext {

bool TCMallocIsAvailable();
bool TCMallocHooksAreAvailable();
MallocExtension* instance();

int MallocHook_AddNewHook(MallocHook_NewHook hook);
int MallocHook_AddDeleteHook(MallocHook_DeleteHook hook);

}  // namespace alloc_ext
}  // namespace reindexer

#endif	// REINDEX_WITH_GPERFTOOLS
