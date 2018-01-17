#include <stdlib.h>

#ifndef REINDEX_WITH_GPERFTOOLS

void allocdebug_init() {}
void allocdebug_show() {}
size_t get_alloc_size() { return -1; }
size_t get_alloc_size_total() { return -1; }
size_t get_alloc_cnt() { return -1; }
size_t get_alloc_cnt_total() { return -1; }

#endif
