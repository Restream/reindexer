#pragma once
#include <stdint.h>
#include <string>
#include "core/type_consts.h"
#include "estl/string_view.h"

namespace reindexer {
using std::string;
using std::wstring;

uint32_t Hash(const wstring &s) noexcept;
uint32_t collateHash(const string_view &s, CollateMode collateMode) noexcept;
uint32_t HashTreGram(const wchar_t *ptr) noexcept;
uint32_t _Hash_bytes(const void *ptr, uint32_t len);

}  // namespace reindexer
