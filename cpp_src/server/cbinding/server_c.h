
#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "core/cbinding/reindexer_ctypes.h"

uintptr_t init_reindexer_server(void);
void destroy_reindexer_server(uintptr_t psvc);
reindexer_error start_reindexer_server(uintptr_t psvc, reindexer_string config);
reindexer_error stop_reindexer_server(uintptr_t psvc);
reindexer_error get_reindexer_instance(uintptr_t psvc, reindexer_string dbname, reindexer_string user, reindexer_string pass,
									   uintptr_t* rx);
int check_server_ready(uintptr_t psvc);
reindexer_error reopen_log_files(uintptr_t psvc);

#ifdef __cplusplus
}
#endif
