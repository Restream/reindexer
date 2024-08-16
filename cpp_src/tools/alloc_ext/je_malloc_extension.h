#pragma once

#include <stddef.h>

#if REINDEX_WITH_JEMALLOC

namespace reindexer {
namespace alloc_ext {

int mallctl(const char* name, void* oldp, size_t* oldlenp, void* newp, size_t newlen);

bool JEMallocIsAvailable();

}  // namespace alloc_ext
}  // namespace reindexer

#endif	// REINDEX_WITH_JEMALLOC
