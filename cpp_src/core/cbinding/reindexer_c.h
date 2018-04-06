#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include "reindexer_ctypes.h"

void init_reindexer();
void destroy_reindexer();
reindexer_error reindexer_enable_storage(reindexer_string path);

typedef struct reindexer_ret {
	reindexer_error err;
	reindexer_buffer out;
} reindexer_ret;

reindexer_error reindexer_open_namespace(reindexer_string _namespace, StorageOpts opts);
reindexer_error reindexer_drop_namespace(reindexer_string _namespace);
reindexer_error reindexer_close_namespace(reindexer_string _namespace);

reindexer_error reindexer_add_index(reindexer_string _namespace, reindexer_string index, reindexer_string json_path,
									reindexer_string index_type, reindexer_string field_type, IndexOpts opts);

reindexer_error reindexer_configure_index(reindexer_string _namespace, reindexer_string index, reindexer_string config);

reindexer_ret reindexer_modify_item(reindexer_buffer in, int mode);
reindexer_ret reindexer_select(reindexer_string query, int with_items, int32_t *pt_versions);

reindexer_ret reindexer_select_query(reindexer_buffer in, int with_items, int32_t *pt_versions);
reindexer_ret reindexer_delete_query(reindexer_buffer in);

reindexer_error reindexer_free_buffer(reindexer_buffer in);
reindexer_error reindexer_free_buffers(reindexer_buffer *in, int count);

reindexer_error reindexer_commit(reindexer_string _namespace);

reindexer_error reindexer_put_meta(reindexer_string ns, reindexer_string key, reindexer_string data);
reindexer_ret reindexer_get_meta(reindexer_string ns, reindexer_string key);

reindexer_error reindexer_reset_stats();
reindexer_stat reindexer_get_stats();

void reindexer_enable_logger(void (*logWriter)(int level, char *msg));
void reindexer_disable_logger();

#ifdef __cplusplus
}
#endif
