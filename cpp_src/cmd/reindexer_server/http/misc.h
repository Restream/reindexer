#pragma once

#include <time.h>

namespace reindexer_server {
namespace http {

int fast_strftime(char *buf, const tm *tm);
void urldecode2(char *dst, const char *src);

inline static char *strappend(char *dst, const char *src) {
	while (*src) *dst++ = *src++;
	return dst;
}

}  // namespace http
}  // namespace reindexer_server
