#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "core/type_consts.h"

typedef struct reindexer_buffer {
	int len;
	int results_flag;
	uint8_t *data;
} reindexer_buffer;

typedef struct reindexer_error {
	int code;
	const char *what;
} reindexer_error;

typedef struct reindexer_string {
	void *p;
	int n;
} reindexer_string;

// enum DataType { NoItemsData, JsonItemsData, PtrItemsData, PlainItemsData };

enum DataFormat { FormatJson, FormatCJson };

enum { ModeUpdate, ModeInsert, ModeUpsert, ModeDelete };

#ifdef __cplusplus
}
#endif
