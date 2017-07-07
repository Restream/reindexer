#pragma once

#ifdef __cplusplus
extern "C" {
#endif

char *cgo_pprof_get_heapprofile();
char *cgo_pprof_lookup_symbol(void *ptr);
void cgo_pprof_init();
int cgo_pprof_start_cpu_profile(char *);
void cgo_pprof_stop_cpu_profile();

#ifdef __cplusplus
}
#endif
