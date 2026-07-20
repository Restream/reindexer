
#pragma once

#define USE_PMR

#if defined(__clang_major__) && __clang_major__ <= 15
#pragma message("Disabling PMR for clang 15 and lower")
#undef USE_PMR
#endif	// __clang_major__ <= 15

#if REINDEX_WITH_GPERFTOOLS
#include "gperftools/tcmalloc.h"
#if TC_VERSION_MAJOR < 2 || (TC_VERSION_MAJOR == 2 && TC_VERSION_MINOR < 7)
#undef USE_PMR
#endif	// TC_VERSION_MAJOR < 2 || (TC_VERSION_MAJOR == 2 && TC_VERSION_MINOR < 7)
#endif	// REINDEX_WITH_GPERFTOOLS
