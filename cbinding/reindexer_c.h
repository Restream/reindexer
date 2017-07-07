#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "cbinding/reindexer_ctypes.h"

void init_reindexer();
void destroy_reindexer();

typedef struct reindexer_ret {
	reindexer_error err;
	reindexer_buffer out;
} reindexer_ret;

reindexer_error reindexer_addnamespace(reindexer_string _namespace);
reindexer_error reindexer_delete_namespace(reindexer_string _namespace);
reindexer_error reindexer_clone_namespace(reindexer_string src, reindexer_string dst);
reindexer_error reindexer_rename_namespace(reindexer_string src, reindexer_string dst);
reindexer_error reindexer_enable_storage(reindexer_string _namespace, reindexer_string path);

reindexer_error reindexer_addindex(reindexer_string _namespace, reindexer_string index, reindexer_string json_path, IndexType type,
								   IndexOpts opts);

reindexer_ret reindexer_upsert(reindexer_buffer in);
reindexer_ret reindexer_delete(reindexer_buffer in);
reindexer_ret reindexer_get_items(reindexer_buffer in);
reindexer_ret reindexer_select(reindexer_string query, int with_items);

reindexer_ret reindexer_select_query(reindexer_query query, int with_items, reindexer_buffer in);
reindexer_ret reindexer_delete_query(reindexer_query query, reindexer_buffer in);
reindexer_error reindexer_commit(reindexer_string _namespace);

reindexer_error reindexer_put_meta(reindexer_buffer in);
reindexer_ret reindexer_get_meta(reindexer_buffer in);

reindexer_error reindexer_reset_stats();
reindexer_stat reindexer_get_stats();

void reindexer_enable_logger(void (*logWriter)(int level, char *msg));
void reindexer_disable_logger();

#ifdef __cplusplus
}
#endif
