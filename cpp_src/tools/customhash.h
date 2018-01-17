#pragma once
#include <stdint.h>
#include <string>

namespace reindexer {
using std::string;
using std::wstring;

uint32_t Hash(const wstring &s) noexcept;
uint32_t Hash(const string &s, bool caseInsensitive = false) noexcept;
uint32_t HashTreGram(const wchar_t *ptr) noexcept;
uint32_t _Hash_bytes(const void *ptr, uint32_t len);
uint32_t _Hash_bytes_ascii_collate(const void *ptr, uint32_t len);

}  // namespace reindexer
