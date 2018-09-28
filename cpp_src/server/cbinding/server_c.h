#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "core/cbinding/reindexer_ctypes.h"

struct server_access;

uintptr_t init();

void init_reindexer_server();
void destroy_reindexer_server();

reindexer_error start_reindexer_server(reindexer_string _config);
reindexer_error stop_reindexer_server();

reindexer_error get_reindexer_instance(reindexer_string _dbname, reindexer_string _user, reindexer_string _pass, uintptr_t* rx);
int check_server_ready();

#ifdef __cplusplus
}
#endif
