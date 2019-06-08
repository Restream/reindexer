#pragma once

#include <stdint.h>

char* u32toa(uint32_t value, char* buffer);
char* i32toa(int32_t value, char* buffer);
char* u64toa(uint64_t value, char* buffer);
char* i64toa(int64_t value, char* buffer);

char* u32toax(uint32_t value, char* buffer, int n = 0);
