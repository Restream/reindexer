#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>

typedef struct reindexer_buffer {
	uint8_t* data;
	int len;
} reindexer_buffer;

typedef struct reindexer_resbuffer {
	uintptr_t results_ptr;
	uintptr_t data;
	int len;
} reindexer_resbuffer;

typedef struct reindexer_error {
	const char* what;
	int code;
} reindexer_error;

typedef struct reindexer_string {
	void* p;
	int32_t n;
	int8_t reserved[4];
} reindexer_string;

typedef struct reindexer_config {
	int64_t allocator_cache_limit;
	float allocator_max_cache_part;
	uint64_t max_updates_size;
	reindexer_string sub_db_name;
} reindexer_config;

typedef struct reindexer_ret {
	reindexer_resbuffer out;
	int err_code;
} reindexer_ret;

typedef struct reindexer_array_ret {
	reindexer_buffer* out_buffers;
	uint32_t out_size;
	uintptr_t data;
	int err_code;
} reindexer_array_ret;

typedef struct reindexer_tx_ret {
	uintptr_t tx_id;
	reindexer_error err;
} reindexer_tx_ret;

typedef struct reindexer_ctx_info {
	uint64_t ctx_id;  // 3 most significant bits will be used as flags and discarded
	int64_t exec_timeout;
} reindexer_ctx_info;

typedef enum { cancel_expilicitly, cancel_on_timeout } ctx_cancel_type;

typedef struct reindexer_subscription_opts {
	int stream_id;
	// TODO: More opts
} reindexer_subscription_opts;

#ifdef __cplusplus
}
#endif
