#pragma once

bool tc_malloc_available();
bool tc_malloc_hooks_available();

#if defined(__GNUC__) && !defined(WEAK_ATTR)
#define WEAK_ATTR __attribute__((weak))
#else
#define WEAK_ATTR
#endif
