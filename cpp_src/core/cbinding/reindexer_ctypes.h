#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "core/type_consts.h"

typedef struct reindexer_buffer {
	uint8_t *data;
	int len;
} reindexer_buffer;

typedef struct reindexer_resbuffer {
	uintptr_t results_ptr;
	uintptr_t data;
	int len;

} reindexer_resbuffer;

typedef struct reindexer_error {
	const char *what;
	int code;
} reindexer_error;

typedef struct reindexer_string {
	void *p;
	int n;
} reindexer_string;

typedef struct reindexer_ret {
	reindexer_resbuffer out;
	int err_code;
} reindexer_ret;

#ifdef __cplusplus
}
#endif
