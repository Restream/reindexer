#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "core/type_consts.h"
#include "reindexer_ctypes.h"

uintptr_t init_reindexer(void);
uintptr_t init_reindexer_with_config(reindexer_config config);

void destroy_reindexer(uintptr_t rx);

reindexer_error reindexer_connect(uintptr_t rx, reindexer_string dsn, ConnectOpts opts, reindexer_string client_vers,
								  BindingCapabilities caps);
reindexer_error reindexer_ping(uintptr_t rx);

reindexer_error reindexer_open_namespace(uintptr_t rx, reindexer_string nsName, StorageOpts opts, reindexer_ctx_info ctx_info);
reindexer_error reindexer_drop_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info);
reindexer_error reindexer_truncate_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info);
reindexer_error reindexer_rename_namespace(uintptr_t rx, reindexer_string srcNsName, reindexer_string dstNsName,
										   reindexer_ctx_info ctx_info);
reindexer_error reindexer_close_namespace(uintptr_t rx, reindexer_string nsName, reindexer_ctx_info ctx_info);

reindexer_error reindexer_add_index(uintptr_t rx, reindexer_string nsName, reindexer_string indexDefJson, reindexer_ctx_info ctx_info);
reindexer_error reindexer_update_index(uintptr_t rx, reindexer_string nsName, reindexer_string indexDefJson, reindexer_ctx_info ctx_info);
reindexer_error reindexer_drop_index(uintptr_t rx, reindexer_string nsName, reindexer_string index, reindexer_ctx_info ctx_info);
reindexer_error reindexer_set_schema(uintptr_t rx, reindexer_string nsName, reindexer_string schemaJson, reindexer_ctx_info ctx_info);

reindexer_tx_ret reindexer_start_transaction(uintptr_t rx, reindexer_string nsName);

reindexer_error reindexer_modify_item_packed_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer args, reindexer_buffer data);
reindexer_error reindexer_update_query_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer in);
reindexer_error reindexer_delete_query_tx(uintptr_t rx, uintptr_t tr, reindexer_buffer in);
reindexer_ret reindexer_commit_transaction(uintptr_t rx, uintptr_t tr, reindexer_ctx_info ctx_info);

reindexer_error reindexer_rollback_transaction(uintptr_t rx, uintptr_t tr);

reindexer_ret reindexer_modify_item_packed(uintptr_t rx, reindexer_buffer args, reindexer_buffer data, reindexer_ctx_info ctx_info);
reindexer_ret reindexer_select(uintptr_t rx, reindexer_string query, int as_json, int32_t* pt_versions, int pt_versions_count,
							   reindexer_ctx_info ctx_info);

reindexer_ret reindexer_select_query(uintptr_t rx, reindexer_buffer in, int as_json, int32_t* pt_versions, int pt_versions_count,
									 reindexer_ctx_info ctx_info);
reindexer_ret reindexer_delete_query(uintptr_t rx, reindexer_buffer in, reindexer_ctx_info ctx_info);
reindexer_ret reindexer_update_query(uintptr_t rx, reindexer_buffer in, reindexer_ctx_info ctx_info);

reindexer_buffer reindexer_cptr2cjson(uintptr_t results_ptr, uintptr_t cptr, int ns_id);
void reindexer_free_cjson(reindexer_buffer b);

reindexer_error reindexer_free_buffer(reindexer_resbuffer in);
reindexer_error reindexer_free_buffers(reindexer_resbuffer* in, int count);

reindexer_ret reindexer_enum_meta(uintptr_t rx, reindexer_string ns, reindexer_ctx_info ctx_info);
reindexer_error reindexer_put_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_string data,
								   reindexer_ctx_info ctx_info);
reindexer_ret reindexer_get_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_ctx_info ctx_info);
reindexer_error reindexer_delete_meta(uintptr_t rx, reindexer_string ns, reindexer_string key, reindexer_ctx_info ctx_info);

reindexer_error reindexer_subscribe(uintptr_t rx, reindexer_string optsJSON);
reindexer_error reindexer_unsubscribe(uintptr_t rx);
reindexer_array_ret reindexer_read_events(uintptr_t rx, reindexer_buffer* out_buffers, uint32_t buffers_count);
reindexer_error reindexer_erase_events(uintptr_t rx, uint32_t events_count);

reindexer_error reindexer_cancel_context(reindexer_ctx_info ctx_info, ctx_cancel_type how);

void reindexer_enable_logger(void (*logWriter)(int level, char* msg));
void reindexer_disable_logger(void);

void reindexer_init_locale(void);

const char* reindexer_version(void);

#ifdef __cplusplus
}
#endif
