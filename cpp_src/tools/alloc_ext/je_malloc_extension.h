#pragma once

bool je_malloc_available();
bool je_malloc_hooks_available();

#if defined(__GNUC__) && !defined(WEAK_ATTR)
#define WEAK_ATTR __attribute__((weak))
#else
#define WEAK_ATTR
#endif
