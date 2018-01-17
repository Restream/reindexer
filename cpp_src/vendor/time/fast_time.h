#pragma once

#include <time.h>
#ifdef __cplusplus
extern "C" {
#endif

int fast_gmtime_r(const time_t *timer, tm *ptm);
#ifdef __cplusplus
}
#endif
