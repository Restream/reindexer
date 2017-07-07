#pragma once

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "core/type_consts.h"

typedef struct reindexer_buffer {
	int len;
	uint8_t *data;
} reindexer_buffer;

typedef struct reindexer_query {
	int sort_dir_desc;
	int offset;
	int limit;
	int calc_total;
	int debug_level;
} reindexer_query;

typedef enum QueryItemType { QueryCondition, QueryDistinct, QuerySortIndex, QueryJoinOn, QueryEnd } QueryItemType;

typedef enum JoinType { LeftJoin, InnerJoin, OrInnerJoin } JoinType;

typedef struct reindexer_error {
	int code;
	const char *what;
} reindexer_error;

typedef struct reindexer_string {
	void *p;
	int n;
} reindexer_string;

typedef struct reindexer_stat {
	int count_get_item, time_get_item;
	int count_select, time_select;
	int count_upsert, time_upsert;
	int count_delete, time_delete;
	int count_join, time_join;
} reindexer_stat;

#ifdef __cplusplus
}
#endif
