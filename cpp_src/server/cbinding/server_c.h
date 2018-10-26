#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "core/cbinding/reindexer_ctypes.h"

void init_reindexer_server();
void destroy_reindexer_server();
reindexer_error start_reindexer_server(reindexer_string config);
reindexer_error stop_reindexer_server();
reindexer_error get_reindexer_instance(reindexer_string dbname, reindexer_string user, reindexer_string pass, uintptr_t* rx);
int check_server_ready();

#ifdef __cplusplus
}
#endif
