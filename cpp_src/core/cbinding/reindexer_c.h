#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "reindexer_ctypes.h"

uintptr_t init_reindexer();
void destroy_reindexer(uintptr_t rx);

reindexer_error reindexer_ping(uintptr_t rx);

reindexer_error reindexer_enable_storage(uintptr_t rx, reindexer_string path);
reindexer_error reindexer_init_system_namespaces(uintptr_t rx);

reindexer_error reindexer_open_namespace(uintptr_t rx, reindexer_string _namespace, StorageOpts opts, uint8_t cacheMode);
reindexer_error reindexer_drop_namespace(uintptr_t rx, reindexer_string _namespace);
reindexer_error reindexer_close_namespace(uintptr_t rx, reindexer_string _namespace);

reindexer_error reindexer_add_index(uintptr_t rx, reindexer_string _namespace, reindexer_string indexDefJson);
reindexer_error reindexer_update_index(uintptr_t rx, reindexer_string _namespace, reindexer_string indexDefJson);
reindexer_error reindexer_drop_index(uintptr_t rx, reindexer_string _namespace, reindexer_string index);

reindexer_ret reindexer_modify_item_packed(uintptr_t rx, reindexer_buffer args, reindexer_buffer data);
reindexer_ret reindexer_select(uintptr_t rx, reindexer_string query, int with_items, int32_t *pt_versions, int pt_versions_count);

reindexer_ret reindexer_select_query(uintptr_t rx, reindexer_buffer in, int with_items, int32_t *pt_versions, int pt_versions_count);
reindexer_ret reindexer_delete_query(uintptr_t rx, reindexer_buffer in);

reindexer_error reindexer_free_buffer(reindexer_resbuffer in);
reindexer_error reindexer_free_buffers(reindexer_resbuffer *in, int count);

reindexer_error reindexer_commit(uintptr_t rx, reindexer_string _namespace);

reindexer_error reindexer_put_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_string data);
reindexer_ret reindexer_get_meta(uintptr_t rx, reindexer_string ns, reindexer_string key);

void reindexer_enable_logger(void (*logWriter)(int level, char *msg));
void reindexer_disable_logger();

#ifdef __cplusplus
}
#endif
