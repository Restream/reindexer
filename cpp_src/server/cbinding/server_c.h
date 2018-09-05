#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "core/cbinding/reindexer_ctypes.h"

struct server_access;

void init_reindexer_server();
void destroy_reindexer_server();

reindexer_error start_reindexer_server(reindexer_string _config);
reindexer_error stop_reindexer_server();

void* get_reindexer_instance(reindexer_string _dbname, reindexer_string _user, reindexer_string _pass);
int check_server_ready();

#ifdef __cplusplus
}
#endif
