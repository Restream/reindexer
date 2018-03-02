#pragma once

#ifdef __cplusplus
extern "C" {
#endif

int Base64decode(char *bufplain, const char *bufcoded);
int Base64decode_len(const char *bufcoded);
int Base64decode(char *bufplain, const char *bufcoded);

#ifdef __cplusplus
}
#endif
