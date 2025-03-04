#pragma once

#if RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES

#if REINDEXER_WITH_SSE
#include <immintrin.h>

#if defined(__GNUC__)
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))
#define PORTABLE_ALIGN64 __attribute__((aligned(64)))
#else
#define PORTABLE_ALIGN32 __declspec(align(32))
#define PORTABLE_ALIGN64 __declspec(align(64))
#endif
#endif	// REINDEXER_WITH_SSE

#endif	// RX_WITH_BUILTIN_ANN_INDEXES || RX_WITH_FAISS_ANN_INDEXES
