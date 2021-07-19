#pragma once

#ifdef __cplusplus
extern "C" {
#endif

char *cgo_pprof_get_heapprofile();
char *cgo_pprof_lookup_symbol(void *ptr);

#ifdef __cplusplus
}
#endif
